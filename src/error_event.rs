//! 四种接口的 SSE 错误事件构造。
//!
//! 输入：接口类型、HTTP 状态（连接失败为 `None`/502）、上游错误体。
//! 输出：完整的 SSE 事件字节序列（含结尾空行、`[DONE]` 等按 api.md 规定）。
//!
//! 关键规则：上游体若能解析出该 API 的标准错误 JSON，则整体原样嵌入（不改写字段）；
//! 否则按下述模板合成，`message` 取错误体原文（截断至 4 KiB，无法解码时用状态行描述）。

use bytes::Bytes;
use hyper::StatusCode;

use crate::detect::EndpointKind;

const MAX_MSG: usize = 4 * 1024;

pub fn build(
    kind: EndpointKind,
    status: Option<StatusCode>,
    body: Option<&[u8]>,
    reason: Option<&str>,
) -> Bytes {
    let status = status.unwrap_or(StatusCode::BAD_GATEWAY);
    match kind {
        EndpointKind::ChatCompletions => build_chat(status, body, reason),
        EndpointKind::Responses => build_responses(status, body, reason),
        EndpointKind::AnthropicMessages => build_anthropic(status, body, reason),
        EndpointKind::GeminiStream => build_gemini(status, body, reason),
    }
}

fn message(body: Option<&[u8]>, reason: Option<&str>, status: StatusCode) -> String {
    if let Some(r) = reason {
        return format!("upstream request failed: {r}");
    }
    match body {
        Some(b) if !b.is_empty() => {
            let Ok(s) = std::str::from_utf8(b) else {
                return status_fallback(status);
            };
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return status_fallback(status);
            }
            if trimmed.len() <= MAX_MSG {
                return trimmed.to_string();
            }
            // 截断 trimmed 至 MAX_MSG，回退到 UTF-8 字符边界。
            let mut end = MAX_MSG;
            while end > 0 && !trimmed.is_char_boundary(end) {
                end -= 1;
            }
            trimmed[..end].to_string()
        }
        _ => status_fallback(status),
    }
}

fn status_fallback(status: StatusCode) -> String {
    format!(
        "{} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("Bad Gateway")
    )
}

fn parse_top_level_error(body: &[u8]) -> Option<serde_json::Value> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    if v.get("error").is_some() {
        Some(v)
    } else {
        None
    }
}

fn parse_responses_error(body: &[u8]) -> Option<(String, String, serde_json::Value)> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    let e = v.get("error")?;
    let code = e.get("code").and_then(|x| x.as_str())?.to_string();
    let msg = e.get("message").and_then(|x| x.as_str())?.to_string();
    let param = e.get("param").cloned().unwrap_or(serde_json::Value::Null);
    Some((code, msg, param))
}

fn parse_anthropic_error(body: &[u8]) -> Option<serde_json::Value> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    if v.get("type").and_then(|t| t.as_str()) == Some("error") && v.get("error").is_some() {
        Some(v)
    } else {
        None
    }
}

// ===== OpenAI Chat Completions =====

fn build_chat(status: StatusCode, body: Option<&[u8]>, reason: Option<&str>) -> Bytes {
    let mut s = String::from("data: ");
    if let Some(b) = body.filter(|b| !b.is_empty()) {
        if let Some(value) = parse_top_level_error(b) {
            s.push_str(&serde_json::to_string(&value).expect("json serializable"));
            s.push_str("\n\ndata: [DONE]\n\n");
            return Bytes::from(s);
        }
    }
    let etype = chat_error_type(status);
    let msg = message(body, reason, status);
    let payload = serde_json::json!({
        "error": {
            "message": msg,
            "type": etype,
            "param": null,
            "code": null,
        }
    });
    s.push_str(&serde_json::to_string(&payload).expect("json serializable"));
    s.push_str("\n\ndata: [DONE]\n\n");
    Bytes::from(s)
}

fn chat_error_type(status: StatusCode) -> &'static str {
    if status == StatusCode::TOO_MANY_REQUESTS {
        "rate_limit_error"
    } else if status.is_client_error() {
        "invalid_request_error"
    } else {
        "server_error"
    }
}

// ===== OpenAI Responses =====

fn build_responses(status: StatusCode, body: Option<&[u8]>, reason: Option<&str>) -> Bytes {
    let (code, msg, param) = if let Some(b) = body.filter(|b| !b.is_empty()) {
        if let Some((c, m, p)) = parse_responses_error(b) {
            (c, m, p)
        } else {
            (
                responses_code(status).to_string(),
                message(body, reason, status),
                serde_json::Value::Null,
            )
        }
    } else {
        (
            responses_code(status).to_string(),
            message(body, reason, status),
            serde_json::Value::Null,
        )
    };
    let payload = serde_json::json!({
        "type": "error",
        "code": code,
        "message": msg,
        "param": param,
        "sequence_number": 0,
    });
    let mut s = String::from("event: error\ndata: ");
    s.push_str(&serde_json::to_string(&payload).expect("json serializable"));
    s.push_str("\n\n");
    Bytes::from(s)
}

fn responses_code(status: StatusCode) -> &'static str {
    if status == StatusCode::TOO_MANY_REQUESTS {
        "rate_limit_exceeded"
    } else if status.is_server_error() || status == StatusCode::BAD_GATEWAY {
        "server_error"
    } else {
        "invalid_request_error"
    }
}

// ===== Anthropic Messages =====

fn build_anthropic(status: StatusCode, body: Option<&[u8]>, reason: Option<&str>) -> Bytes {
    let mut s = String::from("event: error\ndata: ");
    if let Some(b) = body.filter(|b| !b.is_empty()) {
        if let Some(value) = parse_anthropic_error(b) {
            s.push_str(&serde_json::to_string(&value).expect("json serializable"));
            s.push_str("\n\n");
            return Bytes::from(s);
        }
    }
    let etype = anthropic_error_type(status);
    let msg = message(body, reason, status);
    let payload = serde_json::json!({
        "type": "error",
        "error": {
            "type": etype,
            "message": msg,
        }
    });
    s.push_str(&serde_json::to_string(&payload).expect("json serializable"));
    s.push_str("\n\n");
    Bytes::from(s)
}

fn anthropic_error_type(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        529 => "overloaded_error",
        _ => "api_error",
    }
}

// ===== Gemini (alt=sse) =====

fn build_gemini(status: StatusCode, body: Option<&[u8]>, reason: Option<&str>) -> Bytes {
    let mut s = String::from("data: ");
    if let Some(b) = body.filter(|b| !b.is_empty()) {
        if let Some(value) = parse_top_level_error(b) {
            s.push_str(&serde_json::to_string(&value).expect("json serializable"));
            s.push_str("\n\n");
            return Bytes::from(s);
        }
    }
    let gstatus = gemini_status(status);
    let code = status.as_u16();
    let msg = message(body, reason, status);
    let payload = serde_json::json!({
        "error": {
            "code": code,
            "message": msg,
            "status": gstatus,
        }
    });
    s.push_str(&serde_json::to_string(&payload).expect("json serializable"));
    s.push_str("\n\n");
    Bytes::from(s)
}

fn gemini_status(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 => "INVALID_ARGUMENT",
        401 => "UNAUTHENTICATED",
        403 => "PERMISSION_DENIED",
        404 => "NOT_FOUND",
        429 => "RESOURCE_EXHAUSTED",
        500 => "INTERNAL",
        503 => "UNAVAILABLE",
        504 => "DEADLINE_EXCEEDED",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(code: u16) -> Option<StatusCode> {
        StatusCode::from_u16(code).ok()
    }

    #[test]
    fn chat_standard_error_body_passes_through() {
        let body = br#"{"error":{"message":"rate limited","type":"rate_limit_error","param":null,"code":null}}"#;
        let out = build(EndpointKind::ChatCompletions, s(429), Some(body), None);
        let expected = "data: {\"error\":{\"code\":null,\"message\":\"rate limited\",\"param\":null,\"type\":\"rate_limit_error\"}}\n\ndata: [DONE]\n\n";
        assert_eq!(out.as_ref(), expected.as_bytes());
    }

    #[test]
    fn chat_synthesized_from_non_json_body() {
        let out = build(EndpointKind::ChatCompletions, s(500), Some(b"oops"), None);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.starts_with("data: {\"error\":"));
        assert!(s.contains("\"type\":\"server_error\""));
        assert!(s.contains("\"message\":\"oops\""));
        assert!(s.ends_with("\n\ndata: [DONE]\n\n"));
    }

    #[test]
    fn chat_connection_failure() {
        let out = build(
            EndpointKind::ChatCompletions,
            None,
            None,
            Some("connection refused"),
        );
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"type\":\"server_error\""));
        assert!(s.contains("upstream request failed: connection refused"));
        assert!(s.ends_with("\n\ndata: [DONE]\n\n"));
    }

    #[test]
    fn responses_standard_error_body_extracts_fields() {
        let body =
            br#"{"error":{"code":"rate_limit_exceeded","message":"slow down","param":null}}"#;
        let out = build(EndpointKind::Responses, s(429), Some(body), None);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.starts_with("event: error\n"));
        assert!(s.contains("\"code\":\"rate_limit_exceeded\""));
        assert!(s.contains("\"message\":\"slow down\""));
        assert!(s.contains("\"sequence_number\":0"));
        assert!(s.ends_with("\n\n"));
    }

    #[test]
    fn responses_synthesized_from_non_json_body() {
        let out = build(EndpointKind::Responses, s(500), Some(b"fail"), None);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"code\":\"server_error\""));
        assert!(s.contains("\"message\":\"fail\""));
    }

    #[test]
    fn responses_connection_failure() {
        let out = build(EndpointKind::Responses, None, None, Some("timeout"));
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"code\":\"server_error\""));
        assert!(s.contains("upstream request failed: timeout"));
    }

    #[test]
    fn anthropic_standard_error_body_passes_through() {
        let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
        let out = build(EndpointKind::AnthropicMessages, s(429), Some(body), None);
        let expected = "event: error\ndata: {\"error\":{\"message\":\"slow down\",\"type\":\"rate_limit_error\"},\"type\":\"error\"}\n\n";
        assert_eq!(out.as_ref(), expected.as_bytes());
    }

    #[test]
    fn anthropic_synthesized_429() {
        let out = build(EndpointKind::AnthropicMessages, s(429), Some(b"nope"), None);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"type\":\"rate_limit_error\""));
        assert!(s.contains("\"message\":\"nope\""));
    }

    #[test]
    fn anthropic_synthesized_401() {
        let out = build(
            EndpointKind::AnthropicMessages,
            s(401),
            Some(b"bad key"),
            None,
        );
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"type\":\"authentication_error\""));
    }

    #[test]
    fn anthropic_connection_failure() {
        let out = build(
            EndpointKind::AnthropicMessages,
            None,
            None,
            Some("dns error"),
        );
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"type\":\"api_error\""));
        assert!(s.contains("upstream request failed: dns error"));
    }

    #[test]
    fn gemini_standard_error_body_passes_through() {
        let body = br#"{"error":{"code":429,"message":"slow down","status":"RESOURCE_EXHAUSTED"}}"#;
        let out = build(EndpointKind::GeminiStream, s(429), Some(body), None);
        let expected = "data: {\"error\":{\"code\":429,\"message\":\"slow down\",\"status\":\"RESOURCE_EXHAUSTED\"}}\n\n";
        assert_eq!(out.as_ref(), expected.as_bytes());
    }

    #[test]
    fn gemini_synthesized_500() {
        let out = build(EndpointKind::GeminiStream, s(500), Some(b"err"), None);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"status\":\"INTERNAL\""));
        assert!(s.contains("\"code\":500"));
        assert!(s.contains("\"message\":\"err\""));
    }

    #[test]
    fn gemini_connection_failure() {
        let out = build(EndpointKind::GeminiStream, None, None, Some("refused"));
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"status\":\"UNKNOWN\""));
        assert!(s.contains("\"code\":502"));
        assert!(s.contains("upstream request failed: refused"));
    }

    #[test]
    fn chat_pretty_printed_reserialized_single_line() {
        let body = br#"{
  "error": { "message": "rate limited", "type": "rate_limit_error" }
}"#;
        let out = build(EndpointKind::ChatCompletions, s(429), Some(body), None);
        let s = std::str::from_utf8(&out).unwrap();
        // data: 行内无裸换行
        let data_line = s.lines().next().unwrap();
        assert!(data_line.starts_with("data: {"));
        assert!(!data_line.contains('\n'));
        assert!(s.contains("\"message\":\"rate limited\""));
        assert!(s.contains("\"type\":\"rate_limit_error\""));
        assert!(s.ends_with("\n\ndata: [DONE]\n\n"));
    }

    #[test]
    fn message_truncation_lands_on_char_boundary() {
        // "é" is 2 bytes in UTF-8; MAX_MSG=4096 would split a multibyte char.
        // Build a body long enough to trigger truncation with a multibyte char near the cut.
        let prefix = "a".repeat(MAX_MSG - 1);
        let body = format!("{prefix}éé");
        let out = message(Some(body.as_bytes()), None, StatusCode::BAD_GATEWAY);
        // Must be valid UTF-8 and not exceed MAX_MSG
        assert!(out.len() <= MAX_MSG);
        // The é (2 bytes) at position MAX_MSG-1 would overflow; cut backs off to MAX_MSG-1 ("a")
        assert_eq!(out.len(), MAX_MSG - 1);
        assert!(out.is_char_boundary(out.len()));
    }
}
