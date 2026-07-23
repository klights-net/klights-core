//! Framework-neutral Kubernetes JSON/protobuf response negotiation.

pub const JSON_MEDIA_TYPE: &str = "application/json";
pub const PROTOBUF_MEDIA_TYPE: &str = "application/vnd.kubernetes.protobuf";
pub const PROTOBUF_WATCH_MEDIA_TYPE: &str = "application/vnd.kubernetes.protobuf;stream=watch";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseFormat {
    Json,
    Protobuf,
}

impl ResponseFormat {
    pub const fn unary_content_type(self) -> &'static str {
        match self {
            Self::Json => JSON_MEDIA_TYPE,
            Self::Protobuf => PROTOBUF_MEDIA_TYPE,
        }
    }

    pub const fn watch_content_type(self) -> &'static str {
        match self {
            Self::Json => JSON_MEDIA_TYPE,
            Self::Protobuf => PROTOBUF_WATCH_MEDIA_TYPE,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptValue<'a> {
    Text(&'a str),
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiationError {
    message: &'static str,
}

impl NegotiationError {
    pub const fn message(self) -> &'static str {
        self.message
    }
}

impl std::fmt::Display for NegotiationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for NegotiationError {}

#[derive(Clone, Copy, Debug)]
struct AcceptRange {
    media: AcceptMedia,
    quality_millis: u16,
    order: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptMedia {
    Json,
    Protobuf,
    ApplicationWildcard,
    Any,
}

impl AcceptMedia {
    fn specificity_for(self, format: ResponseFormat) -> Option<u8> {
        match (self, format) {
            (Self::Json, ResponseFormat::Json) | (Self::Protobuf, ResponseFormat::Protobuf) => {
                Some(2)
            }
            (Self::ApplicationWildcard, _) => Some(1),
            (Self::Any, _) => Some(0),
            _ => None,
        }
    }
}

pub fn negotiate_unary_response<'a>(
    accepts: impl IntoIterator<Item = AcceptValue<'a>>,
    protobuf_supported: bool,
) -> Result<ResponseFormat, NegotiationError> {
    negotiate(accepts, protobuf_supported, NegotiationMode::Unary)
}

pub fn negotiate_watch_response<'a>(
    accepts: impl IntoIterator<Item = AcceptValue<'a>>,
    protobuf_supported: bool,
) -> Result<ResponseFormat, NegotiationError> {
    negotiate(accepts, protobuf_supported, NegotiationMode::Watch)
}

#[derive(Clone, Copy)]
enum NegotiationMode {
    Unary,
    Watch,
}

fn negotiate<'a>(
    accepts: impl IntoIterator<Item = AcceptValue<'a>>,
    protobuf_supported: bool,
    mode: NegotiationMode,
) -> Result<ResponseFormat, NegotiationError> {
    let mut ranges = Vec::new();
    let mut order = 0usize;
    let mut header_present = false;
    let mut valid_part_present = false;

    for accept in accepts {
        header_present = true;
        let AcceptValue::Text(accept) = accept else {
            continue;
        };
        for raw_part in accept.split(',') {
            let part = raw_part.trim();
            if part.is_empty() {
                continue;
            }
            valid_part_present = true;
            if let Some(range) = parse_accept_part(part, order, mode) {
                ranges.push(range);
            }
            order += 1;
        }
    }

    let accept_required = match mode {
        NegotiationMode::Unary => valid_part_present,
        NegotiationMode::Watch => header_present,
    };
    if !accept_required {
        return Ok(ResponseFormat::Json);
    }

    let formats = [ResponseFormat::Json]
        .into_iter()
        .chain(protobuf_supported.then_some(ResponseFormat::Protobuf));
    let mut candidates = Vec::new();
    for format in formats {
        let Some((quality_millis, controlling_order)) = effective_quality(format, &ranges) else {
            continue;
        };
        if quality_millis == 0 {
            continue;
        }
        let server_preference = match format {
            ResponseFormat::Json => 0,
            ResponseFormat::Protobuf => 1,
        };
        candidates.push((quality_millis, controlling_order, server_preference, format));
    }
    candidates.sort_by_key(|(quality, order, preference, _)| {
        (std::cmp::Reverse(*quality), *order, *preference)
    });
    candidates
        .first()
        .map(|candidate| candidate.3)
        .ok_or(NegotiationError {
            message: match mode {
                NegotiationMode::Unary => "no acceptable response media type is supported",
                NegotiationMode::Watch => "no supported watch stream media type requested",
            },
        })
}

fn effective_quality(format: ResponseFormat, ranges: &[AcceptRange]) -> Option<(u16, usize)> {
    ranges
        .iter()
        .filter_map(|range| {
            range.media.specificity_for(format).map(|specificity| {
                (
                    specificity,
                    range.quality_millis,
                    std::cmp::Reverse(range.order),
                )
            })
        })
        .max()
        .map(|(_, quality_millis, std::cmp::Reverse(order))| (quality_millis, order))
}

fn parse_accept_part(part: &str, order: usize, mode: NegotiationMode) -> Option<AcceptRange> {
    let mut segments = part.split(';');
    let media_type = segments.next()?.trim().to_ascii_lowercase();
    let mut quality_millis = 1000;
    let mut well_formed = true;

    for segment in segments {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let Some((name, value)) = segment.split_once('=') else {
            well_formed = false;
            continue;
        };
        let name = name.trim();
        if name.eq_ignore_ascii_case("q") {
            quality_millis = parse_quality_millis(value.trim(), mode);
        } else if matches!(mode, NegotiationMode::Watch)
            && name.eq_ignore_ascii_case("stream")
            && !value.trim().eq_ignore_ascii_case("watch")
        {
            well_formed = false;
        }
    }
    if !well_formed {
        return None;
    }

    let media = match media_type.as_str() {
        JSON_MEDIA_TYPE => AcceptMedia::Json,
        PROTOBUF_MEDIA_TYPE => AcceptMedia::Protobuf,
        "application/*" => AcceptMedia::ApplicationWildcard,
        "*/*" => AcceptMedia::Any,
        _ => return None,
    };
    Some(AcceptRange {
        media,
        quality_millis,
        order,
    })
}

fn parse_quality_millis(value: &str, mode: NegotiationMode) -> u16 {
    let value = match mode {
        NegotiationMode::Unary => value.trim_matches('"').trim(),
        NegotiationMode::Watch => value.trim(),
    };
    if value == "1" {
        return 1000;
    }
    if let Some(fraction) = value.strip_prefix("1.") {
        return if fraction.len() <= 3 && fraction.bytes().all(|byte| byte == b'0') {
            1000
        } else {
            0
        };
    }
    if value == "0" {
        return 0;
    }
    let Some(fraction) = value.strip_prefix("0.") else {
        return 0;
    };
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return 0;
    }
    fraction
        .bytes()
        .enumerate()
        .fold(0, |quality, (idx, byte)| {
            quality
                + u16::from(byte - b'0')
                    * match idx {
                        0 => 100,
                        1 => 10,
                        _ => 1,
                    }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Expected {
        Json,
        Protobuf,
        NotAcceptable,
    }

    #[test]
    fn unary_negotiation_preserves_quality_specificity_and_order() {
        let cases = [
            (None, true, Expected::Json),
            (Some("application/json"), true, Expected::Json),
            (Some(PROTOBUF_MEDIA_TYPE), true, Expected::Protobuf),
            (Some(PROTOBUF_MEDIA_TYPE), false, Expected::NotAcceptable),
            (
                Some("application/json;q=0, */*;q=1"),
                true,
                Expected::Protobuf,
            ),
            (
                Some("application/vnd.kubernetes.protobuf;q=0, */*;q=1"),
                true,
                Expected::Json,
            ),
            (
                Some("application/vnd.kubernetes.protobuf, application/json"),
                true,
                Expected::Protobuf,
            ),
            (Some("*/*"), true, Expected::Json),
            (Some("application/json;q"), true, Expected::NotAcceptable),
        ];

        for (accept, supported, expected) in cases {
            let values = accept.into_iter().map(AcceptValue::Text);
            let actual = match negotiate_unary_response(values, supported) {
                Ok(ResponseFormat::Json) => Expected::Json,
                Ok(ResponseFormat::Protobuf) => Expected::Protobuf,
                Err(_) => Expected::NotAcceptable,
            };
            assert_eq!(actual, expected, "accept={accept:?}, supported={supported}");
        }
    }

    #[test]
    fn repeated_accept_values_preserve_global_client_order() {
        let accepts = [
            AcceptValue::Text("application/vnd.kubernetes.protobuf;q=0.8"),
            AcceptValue::Text("application/json;q=0.8"),
        ];
        assert_eq!(
            negotiate_unary_response(accepts, true).unwrap(),
            ResponseFormat::Protobuf
        );
    }

    #[test]
    fn watch_negotiation_validates_stream_parameter_and_invalid_headers() {
        assert_eq!(
            negotiate_watch_response(
                [AcceptValue::Text(
                    "application/vnd.kubernetes.protobuf;stream=watch",
                )],
                true,
            )
            .unwrap(),
            ResponseFormat::Protobuf
        );
        assert!(
            negotiate_watch_response(
                [AcceptValue::Text(
                    "application/vnd.kubernetes.protobuf;stream=other",
                )],
                true,
            )
            .is_err()
        );
        assert!(
            negotiate_watch_response([AcceptValue::Invalid], true).is_err(),
            "an invalid but present watch Accept header remains non-acceptable"
        );
        assert!(
            negotiate_watch_response(
                [AcceptValue::Text(
                    "application/vnd.kubernetes.protobuf;q=\"1\"",
                )],
                true,
            )
            .is_err(),
            "watch negotiation retains its historical rejection of quoted quality"
        );
        assert_eq!(
            negotiate_unary_response([AcceptValue::Invalid], true).unwrap(),
            ResponseFormat::Json,
            "unary negotiation retains its historical invalid-header default"
        );
    }
}
