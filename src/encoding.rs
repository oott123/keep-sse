//! Content-Encoding 解析、Accept-Encoding 协商、流式编解码器包装。

use std::collections::HashMap;
use std::io;
use std::pin::Pin;

use async_compression::tokio::bufread::{
    BrotliDecoder as BrBuf, GzipDecoder as GzBuf, ZlibDecoder as DeflBuf, ZstdDecoder as ZstBuf,
};
use async_compression::tokio::write::{BrotliEncoder, GzipEncoder, ZlibEncoder, ZstdEncoder};
use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt};
use hyper::body::{Body, Frame};
use hyper::HeaderMap;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::io::{ReaderStream, StreamReader};

/// 识别的内容编码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coding {
    Identity,
    Gzip,
    Deflate,
    Br,
    Zstd,
}

impl Coding {
    pub fn as_str(self) -> &'static str {
        match self {
            Coding::Identity => "identity",
            Coding::Gzip => "gzip",
            Coding::Deflate => "deflate",
            Coding::Br => "br",
            Coding::Zstd => "zstd",
        }
    }

    /// 按 IANA token（不区分大小写）解析。未知返回 `None`。
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("identity") {
            return Some(Coding::Identity);
        }
        match s.to_ascii_lowercase().as_str() {
            "gzip" | "x-gzip" => Some(Coding::Gzip),
            "deflate" => Some(Coding::Deflate),
            "br" => Some(Coding::Br),
            "zstd" => Some(Coding::Zstd),
            _ => None,
        }
    }

    /// `Content-Encoding` 头值。`identity` 不写头（按 design 省略）。
    pub fn header_value(self) -> Option<&'static str> {
        match self {
            Coding::Identity => None,
            other => Some(other.as_str()),
        }
    }
}

/// 解析请求或响应的 `Content-Encoding`。无头视为 `Identity`；多重编码或未知编码返回 `None`。
pub fn parse_content_encoding(headers: &HeaderMap) -> Option<Coding> {
    let mut iter = headers.get_all(http::header::CONTENT_ENCODING).iter();
    let first = match iter.next() {
        None => return Some(Coding::Identity),
        Some(v) => v,
    };
    if iter.next().is_some() {
        return None;
    }
    let s = first.to_str().ok()?;
    let parts: Vec<&str> = s
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() != 1 {
        return None;
    }
    Coding::parse(parts[0])
}

/// 按客户端 `Accept-Encoding` 协商下行编码。
///
/// 优先级 `zstd > br > gzip > deflate > identity`；只识别 `;q=0` 为排除项，
/// 不做完整 q 值排序。`identity` 默认始终可接受，除非显式 `identity;q=0`。
pub fn negotiate(accept_encoding: Option<&str>) -> Coding {
    let Some(val) = accept_encoding else {
        return Coding::Identity;
    };
    if val.trim().is_empty() {
        return Coding::Identity;
    }

    let mut explicit: HashMap<String, bool> = HashMap::new();
    let mut wildcard: Option<bool> = None;
    for raw in val.split(',') {
        let tok = raw.trim();
        if tok.is_empty() {
            continue;
        }
        let (name, q) = match tok.split_once(';') {
            Some((n, rest)) => {
                let qv: f32 = rest
                    .trim()
                    .strip_prefix("q=")
                    .and_then(|q| q.trim().parse().ok())
                    .unwrap_or(1.0);
                (n.trim(), qv)
            }
            None => (tok, 1.0),
        };
        let allowed = q > 0.0;
        if name == "*" {
            wildcard = Some(allowed);
        } else {
            explicit.insert(name.to_ascii_lowercase(), allowed);
        }
    }

    let accepted = |coding: &str| -> bool {
        match explicit.get(coding) {
            Some(&allowed) => allowed,
            None => wildcard.unwrap_or_default(),
        }
    };

    let identity_ok = *explicit.get("identity").unwrap_or(&true);

    if accepted("zstd") {
        return Coding::Zstd;
    }
    if accepted("br") {
        return Coding::Br;
    }
    if accepted("gzip") {
        return Coding::Gzip;
    }
    if accepted("deflate") {
        return Coding::Deflate;
    }
    if identity_ok {
        return Coding::Identity;
    }
    // 客户端拒绝 identity 且不接受任何已知编码：退化为 identity（degraded）。
    Coding::Identity
}

/// 一次性解压（探测与错误体读取用）。解压输出上限 `max_out` 字节，超出返回 `InvalidData`。
pub async fn decode_bytes(coding: Coding, input: &[u8], max_out: usize) -> io::Result<Vec<u8>> {
    match coding {
        Coding::Identity => {
            if input.len() > max_out {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("decoded size {} exceeds limit {}", input.len(), max_out),
                ));
            }
            Ok(input.to_vec())
        }
        Coding::Gzip => decode_async(GzBuf::new(input), max_out).await,
        Coding::Deflate => decode_async(DeflBuf::new(input), max_out).await,
        Coding::Br => decode_async(BrBuf::new(input), max_out).await,
        Coding::Zstd => decode_async(ZstBuf::new(input), max_out).await,
    }
}

async fn decode_async<R: AsyncRead + Unpin>(decoder: R, max_out: usize) -> io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    // 读 max_out + 1 字节：读满则超限。
    let mut limited = decoder.take(max_out as u64 + 1);
    let mut out = Vec::new();
    limited.read_to_end(&mut out).await?;
    if out.len() > max_out {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("decoded size exceeds limit {}", max_out),
        ));
    }
    Ok(out)
}

/// 把上游响应 body 转换为 `Stream<Item = io::Result<Bytes>>`。
fn body_to_io_stream<B>(body: B) -> impl Stream<Item = io::Result<Bytes>>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    use http_body_util::BodyDataStream;
    BodyDataStream::new(body).map(|res| res.map_err(io::Error::other))
}

/// 上游响应流式解压。输入为 `Body<Data = Bytes>`，输出 `Stream<Item = io::Result<Bytes>>`。
pub fn decoder_stream<B>(
    coding: Coding,
    body: B,
) -> Box<dyn Stream<Item = io::Result<Bytes>> + Send + Unpin>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    let stream = body_to_io_stream(body);
    let reader = StreamReader::new(stream);
    let bufreader = BufReader::new(reader);
    match coding {
        Coding::Identity => Box::new(ReaderStream::new(bufreader)),
        Coding::Gzip => Box::new(ReaderStream::new(GzBuf::new(bufreader))),
        Coding::Deflate => Box::new(ReaderStream::new(DeflBuf::new(bufreader))),
        Coding::Br => Box::new(ReaderStream::new(BrBuf::new(bufreader))),
        Coding::Zstd => Box::new(ReaderStream::new(ZstBuf::new(bufreader))),
    }
}

/// 内部写入缓冲：`AsyncWrite` 实现，把字节追加到 `Vec<u8>`。
pub struct VecSink(pub Vec<u8>);

impl AsyncWrite for VecSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.0.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

type FrameTx = mpsc::Sender<Result<Frame<Bytes>, io::Error>>;

/// 下行 SSE 写入端：按协商编码产出事件字节，每个事件后 flush。
pub enum SseWriter {
    Identity { tx: FrameTx },
    Gzip(Box<GzipEncoder<VecSink>>, FrameTx),
    Deflate(Box<ZlibEncoder<VecSink>>, FrameTx),
    Br(Box<BrotliEncoder<VecSink>>, FrameTx),
    Zstd(Box<ZstdEncoder<VecSink>>, FrameTx),
}

impl SseWriter {
    pub fn new(coding: Coding, tx: FrameTx) -> Self {
        match coding {
            Coding::Identity => SseWriter::Identity { tx },
            Coding::Gzip => SseWriter::Gzip(Box::new(GzipEncoder::new(VecSink(Vec::new()))), tx),
            Coding::Deflate => {
                SseWriter::Deflate(Box::new(ZlibEncoder::new(VecSink(Vec::new()))), tx)
            }
            Coding::Br => SseWriter::Br(Box::new(BrotliEncoder::new(VecSink(Vec::new()))), tx),
            Coding::Zstd => SseWriter::Zstd(Box::new(ZstdEncoder::new(VecSink(Vec::new()))), tx),
        }
    }

    /// 写出一个事件并 flush；客户端断开时返回错误。
    pub async fn write_event(&mut self, bytes: Bytes) -> io::Result<()> {
        match self {
            SseWriter::Identity { tx } => tx
                .send(Ok(Frame::data(bytes)))
                .await
                .map_err(|_| io::Error::other("client gone")),
            SseWriter::Gzip(enc, tx) => {
                write_coded(enc, tx, bytes, |e| GzipEncoder::get_mut(e)).await
            }
            SseWriter::Deflate(enc, tx) => {
                write_coded(enc, tx, bytes, |e| ZlibEncoder::get_mut(e)).await
            }
            SseWriter::Br(enc, tx) => {
                write_coded(enc, tx, bytes, |e| BrotliEncoder::get_mut(e)).await
            }
            SseWriter::Zstd(enc, tx) => {
                write_coded(enc, tx, bytes, |e| ZstdEncoder::get_mut(e)).await
            }
        }
    }

    /// 结束流：编码器收尾并 flush 残留字节，关闭下游通道。
    pub async fn end(self) {
        match self {
            SseWriter::Identity { tx } => drop(tx),
            SseWriter::Gzip(mut enc, tx) => {
                finish_coded(&mut enc, tx, |e| GzipEncoder::get_mut(e)).await
            }
            SseWriter::Deflate(mut enc, tx) => {
                finish_coded(&mut enc, tx, |e| ZlibEncoder::get_mut(e)).await
            }
            SseWriter::Br(mut enc, tx) => {
                finish_coded(&mut enc, tx, |e| BrotliEncoder::get_mut(e)).await
            }
            SseWriter::Zstd(mut enc, tx) => {
                finish_coded(&mut enc, tx, |e| ZstdEncoder::get_mut(e)).await
            }
        }
    }
}

async fn write_coded<E, F>(enc: &mut E, tx: &FrameTx, bytes: Bytes, get_sink: F) -> io::Result<()>
where
    E: AsyncWrite + Unpin,
    F: Fn(&mut E) -> &mut VecSink,
{
    enc.write_all(&bytes).await?;
    enc.flush().await?;
    let out = std::mem::take(&mut get_sink(enc).0);
    if !out.is_empty() {
        tx.send(Ok(Frame::data(Bytes::from(out))))
            .await
            .map_err(|_| io::Error::other("client gone"))?;
    }
    Ok(())
}

/// 关闭编码器并刷出残留字节（gzip trailer 等）。
async fn finish_coded<E, F>(enc: &mut E, tx: FrameTx, get_sink: F)
where
    E: AsyncWrite + Unpin,
    F: Fn(&mut E) -> &mut VecSink,
{
    let _ = enc.shutdown().await;
    let out = std::mem::take(&mut get_sink(enc).0);
    if !out.is_empty() {
        let _ = tx.send(Ok(Frame::data(Bytes::from(out)))).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::CONTENT_ENCODING;
    use http::HeaderMap;

    #[test]
    fn negotiate_priority() {
        assert_eq!(negotiate(Some("zstd, br, gzip")), Coding::Zstd);
        assert_eq!(negotiate(Some("br, gzip")), Coding::Br);
        assert_eq!(negotiate(Some("gzip")), Coding::Gzip);
        assert_eq!(negotiate(Some("deflate")), Coding::Deflate);
        assert_eq!(negotiate(None), Coding::Identity);
        assert_eq!(negotiate(Some("")), Coding::Identity);
        // 通配符
        assert_eq!(negotiate(Some("*")), Coding::Zstd);
    }

    #[test]
    fn negotiate_q0_exclusion() {
        // zstd 被排除，选 br
        assert_eq!(negotiate(Some("zstd;q=0, br")), Coding::Br);
        // 全排除通配
        assert_eq!(negotiate(Some("*;q=0, identity")), Coding::Identity);
        // identity 被排除且无其它可用 → 退化 identity
        assert_eq!(negotiate(Some("identity;q=0")), Coding::Identity);
    }

    #[test]
    fn parse_single_and_chained() {
        let mut h = HeaderMap::new();
        h.insert(CONTENT_ENCODING, http::HeaderValue::from_static("gzip"));
        assert_eq!(parse_content_encoding(&h), Some(Coding::Gzip));

        let mut h = HeaderMap::new();
        h.insert(CONTENT_ENCODING, http::HeaderValue::from_static("br"));
        assert_eq!(parse_content_encoding(&h), Some(Coding::Br));

        // 链式多重编码 → None
        let mut h = HeaderMap::new();
        h.insert(
            CONTENT_ENCODING,
            http::HeaderValue::from_static("gzip, deflate"),
        );
        assert_eq!(parse_content_encoding(&h), None);

        // 未知编码 → None
        let mut h = HeaderMap::new();
        h.insert(CONTENT_ENCODING, http::HeaderValue::from_static("snappy"));
        assert_eq!(parse_content_encoding(&h), None);

        // 缺省 → Identity
        let h = HeaderMap::new();
        assert_eq!(parse_content_encoding(&h), Some(Coding::Identity));

        // 多个 Content-Encoding 头 → None
        let mut h = HeaderMap::new();
        h.append(CONTENT_ENCODING, http::HeaderValue::from_static("gzip"));
        h.append(CONTENT_ENCODING, http::HeaderValue::from_static("br"));
        assert_eq!(parse_content_encoding(&h), None);
    }

    #[tokio::test]
    async fn roundtrip_all_codings() {
        let payload = b"the quick brown fox jumps over the lazy dog ".repeat(20);
        for coding in [Coding::Gzip, Coding::Deflate, Coding::Br, Coding::Zstd] {
            let compressed = encode_once(coding, &payload).await;
            let decoded = decode_bytes(coding, &compressed, payload.len())
                .await
                .unwrap();
            assert_eq!(decoded, payload, "roundtrip failed for {:?}", coding);
        }
    }

    #[tokio::test]
    async fn decode_bytes_exactly_at_limit_passes() {
        let payload = b"x".repeat(256);
        let compressed = encode_once(Coding::Gzip, &payload).await;
        let decoded = decode_bytes(Coding::Gzip, &compressed, 256).await.unwrap();
        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn decode_bytes_one_byte_over_errors() {
        let payload = b"x".repeat(256);
        let compressed = encode_once(Coding::Gzip, &payload).await;
        let err = decode_bytes(Coding::Gzip, &compressed, 255).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn decode_bytes_identity_limit() {
        let payload = b"x".repeat(10);
        assert_eq!(decode_bytes(Coding::Identity, &payload, 10).await.unwrap(), payload);
        assert!(decode_bytes(Coding::Identity, &payload, 9).await.is_err());
    }

    async fn encode_once(coding: Coding, input: &[u8]) -> Vec<u8> {
        use async_compression::tokio::bufread::{
            BrotliEncoder as BrBuf, GzipEncoder as GzBuf, ZlibEncoder as DeflBuf,
            ZstdEncoder as ZstBuf,
        };
        use tokio::io::AsyncReadExt;
        let mut out = Vec::new();
        match coding {
            Coding::Gzip => {
                let mut e = GzBuf::new(input);
                e.read_to_end(&mut out).await.unwrap();
            }
            Coding::Deflate => {
                let mut e = DeflBuf::new(input);
                e.read_to_end(&mut out).await.unwrap();
            }
            Coding::Br => {
                let mut e = BrBuf::new(input);
                e.read_to_end(&mut out).await.unwrap();
            }
            Coding::Zstd => {
                let mut e = ZstBuf::new(input);
                e.read_to_end(&mut out).await.unwrap();
            }
            Coding::Identity => out.extend_from_slice(input),
        }
        out
    }
}
