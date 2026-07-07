//! Content-Encoding 解析、上游流式解压、下行 SSE 写入端。

use std::io;

use async_compression::tokio::bufread::{
    BrotliDecoder as BrBuf, GzipDecoder as GzBuf, ZlibDecoder as DeflBuf, ZstdDecoder as ZstBuf,
};
use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt};
use hyper::body::{Body, Frame};
use hyper::HeaderMap;
use tokio::io::{AsyncRead, BufReader};
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

type FrameTx = mpsc::Sender<Result<Frame<Bytes>, io::Error>>;

/// 下行 SSE 写入端：以 identity 产出事件字节。
pub struct SseWriter {
    tx: FrameTx,
}

impl SseWriter {
    pub fn new(tx: FrameTx) -> Self {
        SseWriter { tx }
    }

    /// 写出一个事件；客户端断开时返回错误。
    pub async fn write_event(&mut self, bytes: Bytes) -> io::Result<()> {
        self.tx
            .send(Ok(Frame::data(bytes)))
            .await
            .map_err(|_| io::Error::other("client gone"))
    }

    /// 关闭下游通道。
    pub async fn end(self) {
        drop(self.tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::CONTENT_ENCODING;
    use http::HeaderMap;

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
        let err = decode_bytes(Coding::Gzip, &compressed, 255)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn decode_bytes_identity_limit() {
        let payload = b"x".repeat(10);
        assert_eq!(
            decode_bytes(Coding::Identity, &payload, 10).await.unwrap(),
            payload
        );
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
