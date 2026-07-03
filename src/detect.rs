//! 端点识别与 `stream` 字段探测。

use hyper::Method;

/// 识别出的 LLM 流式接口类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointKind {
    /// OpenAI Chat Completions：路径后缀 `/chat/completions`。
    ChatCompletions,
    /// OpenAI Responses：路径后缀 `/responses`。
    Responses,
    /// Anthropic Messages：路径后缀 `/messages`。
    AnthropicMessages,
    /// Gemini `:streamGenerateContent` + `alt=sse`。
    GeminiStream,
}

/// 按路径后缀匹配 LLM 接口。返回 `Some(kind)` 表示命中（仍需后续判定是否流式）。
///
/// 后缀匹配为完整路径段匹配：
/// - `/api/v1/chat/completions`、`/openai/api/v1/chat/completions` 命中 ChatCompletions；
/// - `/v1/messages/batches`、`/v1/responses/{id}` 不命中；
/// - Gemini 的最后一段以 `:streamGenerateContent` 结尾，且查询串含 `alt=sse`。
pub fn match_endpoint(method: &Method, path: &str, query: Option<&str>) -> Option<EndpointKind> {
    if method != Method::POST {
        return None;
    }
    if path.ends_with("/chat/completions") {
        return Some(EndpointKind::ChatCompletions);
    }
    if path.ends_with("/responses") {
        return Some(EndpointKind::Responses);
    }
    if path.ends_with("/messages") {
        return Some(EndpointKind::AnthropicMessages);
    }
    // Gemini：最后一段以 `:streamGenerateContent` 结尾，且 alt=sse。
    let last_seg = path.rsplit('/').next()?;
    if last_seg.ends_with(":streamGenerateContent") && has_alt_sse(query.unwrap_or("")) {
        return Some(EndpointKind::GeminiStream);
    }
    None
}

/// 判定查询串中是否存在 `alt=sse`。容忍前导 `?`、`&` 分隔、值尾的 `&`。
fn has_alt_sse(query: &str) -> bool {
    for pair in query.split('&') {
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        if k == "alt" && v == "sse" {
            return true;
        }
    }
    false
}

#[derive(serde::Deserialize)]
struct Probe {
    #[serde(default)]
    stream: bool,
}

/// 解析 JSON 请求体中的 `stream` 字段；解析失败或字段缺省返回 `false`。
pub fn probe_stream_flag(json_body: &[u8]) -> bool {
    serde_json::from_slice::<Probe>(json_body)
        .map(|p| p.stream)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m() -> Method {
        Method::POST
    }

    #[test]
    fn chat_completions_prefix_invariant() {
        assert_eq!(
            match_endpoint(&m(), "/chat/completions", None),
            Some(EndpointKind::ChatCompletions)
        );
        assert_eq!(
            match_endpoint(&m(), "/api/v1/chat/completions", None),
            Some(EndpointKind::ChatCompletions)
        );
        assert_eq!(
            match_endpoint(&m(), "/openai/api/v1/chat/completions", None),
            Some(EndpointKind::ChatCompletions)
        );
    }

    #[test]
    fn messages_suffix_segment_only() {
        assert_eq!(
            match_endpoint(&m(), "/v1/messages", None),
            Some(EndpointKind::AnthropicMessages)
        );
        // 下一层路径段不算命中
        assert_eq!(match_endpoint(&m(), "/v1/messages/batches", None), None);
        // 不是 messages 后缀
        assert_eq!(match_endpoint(&m(), "/v1/notmessages", None), None);
    }

    #[test]
    fn responses_suffix_segment_only() {
        assert_eq!(
            match_endpoint(&m(), "/v1/responses", None),
            Some(EndpointKind::Responses)
        );
        // /responses/{id} 不命中
        assert_eq!(match_endpoint(&m(), "/v1/responses/abc", None), None);
    }

    #[test]
    fn non_post_not_matched() {
        assert_eq!(
            match_endpoint(&Method::GET, "/chat/completions", None),
            None
        );
    }

    #[test]
    fn gemini_alt_sse_required() {
        assert_eq!(
            match_endpoint(
                &m(),
                "/v1beta/models/gemini-pro:streamGenerateContent",
                Some("alt=sse")
            ),
            Some(EndpointKind::GeminiStream)
        );
        // 带其它参数
        assert_eq!(
            match_endpoint(
                &m(),
                "/v1beta/models/gemini-pro:streamGenerateContent",
                Some("foo=bar&alt=sse")
            ),
            Some(EndpointKind::GeminiStream)
        );
        // 缺 alt=sse
        assert_eq!(
            match_endpoint(
                &m(),
                "/v1beta/models/gemini-pro:streamGenerateContent",
                Some("alt=json")
            ),
            None
        );
        assert_eq!(
            match_endpoint(
                &m(),
                "/v1beta/models/gemini-pro:streamGenerateContent",
                None
            ),
            None
        );
    }

    #[test]
    fn stream_flag_probing() {
        assert!(probe_stream_flag(br#"{"stream":true}"#));
        assert!(probe_stream_flag(br#"{"stream":true,"model":"x"}"#));
        assert!(probe_stream_flag(br#"{"model":"x","stream":true}"#));
        assert!(!probe_stream_flag(br#"{"stream":false}"#));
        assert!(!probe_stream_flag(br#"{"model":"x"}"#));
        assert!(!probe_stream_flag(br#"not json"#));
        // stream 为非布尔值也应视为非流式
        assert!(!probe_stream_flag(br#"{"stream":"true"}"#));
    }
}
