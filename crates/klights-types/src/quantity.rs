//! Kubernetes resource quantity parsing and formatting.

use num_bigint::BigInt;
use num_traits::{One, ToPrimitive, Zero};

#[derive(Clone, Copy)]
struct Suffix {
    text: &'static str,
    num: i128,
    den: i128,
}

#[derive(Clone)]
struct QuantityRational {
    numerator: BigInt,
    scale10: u32,
    exponent10: i32,
}

const CPU_SUFFIXES: [Suffix; 9] = [
    Suffix {
        text: "n",
        num: 1,
        den: 1_000_000_000,
    },
    Suffix {
        text: "u",
        num: 1,
        den: 1_000_000,
    },
    Suffix {
        text: "m",
        num: 1,
        den: 1000,
    },
    Suffix {
        text: "k",
        num: 1_000,
        den: 1,
    },
    Suffix {
        text: "M",
        num: 1_000_000,
        den: 1,
    },
    Suffix {
        text: "G",
        num: 1_000_000_000,
        den: 1,
    },
    Suffix {
        text: "T",
        num: 1_000_000_000_000,
        den: 1,
    },
    Suffix {
        text: "P",
        num: 1_000_000_000_000_000,
        den: 1,
    },
    Suffix {
        text: "E",
        num: 1_000_000_000_000_000_000,
        den: 1,
    },
];

const DECIMAL_SUFFIXES: [Suffix; 9] = [
    Suffix {
        text: "E",
        num: 1_000_000_000_000_000_000,
        den: 1,
    },
    Suffix {
        text: "P",
        num: 1_000_000_000_000_000,
        den: 1,
    },
    Suffix {
        text: "T",
        num: 1_000_000_000_000,
        den: 1,
    },
    Suffix {
        text: "G",
        num: 1_000_000_000,
        den: 1,
    },
    Suffix {
        text: "M",
        num: 1_000_000,
        den: 1,
    },
    Suffix {
        text: "k",
        num: 1000,
        den: 1,
    },
    Suffix {
        text: "m",
        num: 1,
        den: 1000,
    },
    Suffix {
        text: "u",
        num: 1,
        den: 1_000_000,
    },
    Suffix {
        text: "n",
        num: 1,
        den: 1_000_000_000,
    },
];

const BINARY_SUFFIXES: [Suffix; 6] = [
    Suffix {
        text: "Ki",
        num: 1024,
        den: 1,
    },
    Suffix {
        text: "Mi",
        num: 1024_i128.pow(2),
        den: 1,
    },
    Suffix {
        text: "Gi",
        num: 1024_i128.pow(3),
        den: 1,
    },
    Suffix {
        text: "Ti",
        num: 1024_i128.pow(4),
        den: 1,
    },
    Suffix {
        text: "Pi",
        num: 1024_i128.pow(5),
        den: 1,
    },
    Suffix {
        text: "Ei",
        num: 1024_i128.pow(6),
        den: 1,
    },
];

pub fn parse_cpu_milli(raw: &str) -> Option<i64> {
    parse_quantity_with_suffixes(raw, &CPU_SUFFIXES, 1)
}

pub fn is_binary_quantity_resource(resource_key: &str) -> bool {
    resource_key == "memory"
        || resource_key == "ephemeral-storage"
        || resource_key.contains("storage")
        || resource_key.starts_with("hugepages-")
}

pub fn parse_memory_bytes(raw: &str) -> Option<i64> {
    if raw.is_empty() || raw.trim() != raw {
        return None;
    }

    if let Some((number, suffix)) = split_suffix(raw, &BINARY_SUFFIXES) {
        return apply_quantity(parse_decimal_rational(number, false)?, Some(suffix), 1000);
    }
    if let Some((number, suffix)) = split_suffix(raw, &DECIMAL_SUFFIXES) {
        return apply_quantity(parse_decimal_rational(number, false)?, Some(suffix), 1000);
    }

    parse_quantity_with_suffixes(raw, &[], 1000)
}

pub fn parse_decimal_si_quantity(_resource_key: &str, raw: &str) -> Option<i64> {
    parse_quantity_with_suffixes(raw, &DECIMAL_SUFFIXES, 1000)
}

pub fn parse_resource_quantity(resource_key: &str, quantity: &str) -> Option<i64> {
    if resource_key == "cpu" {
        parse_cpu_milli(quantity)
    } else if is_binary_quantity_resource(resource_key) {
        parse_memory_bytes(quantity)
    } else {
        parse_decimal_si_quantity(resource_key, quantity)
    }
}

/// Kubernetes Pod effective resource usage for one requests/limits key.
///
/// Regular containers add together, while init containers contribute their
/// maximum. The effective value is the larger of those two totals.
pub fn calculate_pod_effective_resource_for_key(
    pod: &serde_json::Value,
    bucket: &str,
    resource_key: &str,
) -> i64 {
    let regular_sum = pod
        .pointer("/spec/containers")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|container| {
            container
                .get("resources")
                .and_then(|resources| resources.get(bucket))
                .and_then(|resources| resources.get(resource_key))
                .and_then(serde_json::Value::as_str)
                .and_then(|quantity| parse_resource_quantity(resource_key, quantity))
        })
        .sum::<i64>();

    let init_max = pod
        .pointer("/spec/initContainers")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|container| {
            container
                .get("resources")
                .and_then(|resources| resources.get(bucket))
                .and_then(|resources| resources.get(resource_key))
                .and_then(serde_json::Value::as_str)
                .and_then(|quantity| parse_resource_quantity(resource_key, quantity))
        })
        .max()
        .unwrap_or(0);

    regular_sum.max(init_max)
}

pub fn format_cpu_milli(milli: i64) -> String {
    if milli % 1000 == 0 {
        (milli / 1000).to_string()
    } else {
        format!("{milli}m")
    }
}

pub fn format_memory_bytes(bytes: i64) -> String {
    for (suffix, size) in [
        ("Ei", 1024_i64.pow(6)),
        ("Pi", 1024_i64.pow(5)),
        ("Ti", 1024_i64.pow(4)),
        ("Gi", 1024_i64.pow(3)),
        ("Mi", 1024_i64.pow(2)),
        ("Ki", 1024_i64),
    ] {
        if bytes >= size && bytes % size == 0 {
            return format!("{}{}", bytes / size, suffix);
        }
    }
    bytes.to_string()
}

pub fn format_resource_quantity(resource_key: &str, value: i64) -> String {
    if resource_key == "cpu" {
        format_cpu_milli(value)
    } else if is_binary_quantity_resource(resource_key) {
        format_memory_bytes(value)
    } else {
        value.to_string()
    }
}

fn parse_quantity_with_suffixes(raw: &str, suffixes: &[Suffix], final_div: i128) -> Option<i64> {
    if raw.is_empty() || raw.trim() != raw {
        return None;
    }

    let (number, suffix) = match split_suffix(raw, suffixes) {
        Some((number, suffix)) => (number, Some(suffix)),
        None => (raw, None),
    };

    apply_quantity(
        parse_decimal_rational(number, suffix.is_none())?,
        suffix,
        final_div,
    )
}

fn parse_decimal_rational(raw: &str, allow_exponent: bool) -> Option<QuantityRational> {
    if raw.is_empty() || raw.starts_with('-') {
        return None;
    }

    let raw = raw.strip_prefix('+').unwrap_or(raw);
    if raw.is_empty() {
        return None;
    }
    if !allow_exponent && raw.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
        return None;
    }

    let (number, exponent) = split_exponent(raw)?;
    let (numerator, scale10) = parse_decimal_rational_no_exponent(number)?;
    Some(QuantityRational {
        numerator,
        scale10,
        exponent10: exponent,
    })
}

fn parse_decimal_rational_no_exponent(raw: &str) -> Option<(BigInt, u32)> {
    let (int_part, mut frac_part) = match raw.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (raw, ""),
    };
    let frac_was_empty = frac_part.is_empty();
    frac_part = frac_part.trim_end_matches('0');

    if int_part.is_empty() && frac_part.is_empty() && (!raw.contains('.') || frac_was_empty) {
        return None;
    }
    if int_part.chars().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    if frac_part.chars().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    let int_value: BigInt = if int_part.is_empty() {
        BigInt::zero()
    } else {
        parse_decimal_digits(int_part)?
    };
    let scale10 = frac_part.len().try_into().ok()?;
    let frac_scale = pow10_big(scale10)?;
    let frac_value = if frac_part.is_empty() {
        BigInt::zero()
    } else {
        parse_decimal_digits(frac_part)?
    };
    let numerator = int_value * &frac_scale + frac_value;
    Some((numerator, scale10))
}

fn parse_decimal_digits(raw: &str) -> Option<BigInt> {
    let mut value = BigInt::zero();
    for byte in raw.bytes() {
        let digit = byte.checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        value = value * 10 + BigInt::from(digit);
    }
    Some(value)
}

fn split_exponent(raw: &str) -> Option<(&str, i32)> {
    let mut marker: Option<usize> = None;
    for (idx, ch) in raw.char_indices() {
        if ch == 'e' || ch == 'E' {
            if marker.is_some() {
                return None;
            }
            marker = Some(idx);
        }
    }

    let Some(idx) = marker else {
        return Some((raw, 0));
    };

    let (number, exponent_text) = raw.split_at(idx);
    if number.is_empty() || exponent_text.len() <= 1 {
        return None;
    }

    let exponent = exponent_text[1..].parse::<i32>().ok()?;
    Some((number, exponent))
}

fn split_suffix<'a>(raw: &'a str, suffixes: &'a [Suffix]) -> Option<(&'a str, &'a Suffix)> {
    for suffix in suffixes {
        if let Some(number) = raw.strip_suffix(suffix.text) {
            if number.is_empty() {
                continue;
            }
            return Some((number, suffix));
        }
    }
    None
}

fn apply_quantity(
    rational: QuantityRational,
    suffix: Option<&Suffix>,
    final_div: i128,
) -> Option<i64> {
    let numerator = rational.numerator
        * BigInt::from(1000_i128)
        * BigInt::from(suffix.map_or(1, |suffix| suffix.num));
    // Any valid zero coefficient remains zero for every representable
    // exponent; short-circuit before allocating a huge power of ten.
    if numerator.is_zero() {
        return Some(0);
    }

    let denominator_scale10 = rational
        .scale10
        .checked_add(decimal_power_scale(final_div)?)?
        .checked_add(decimal_power_scale(suffix.map_or(1, |suffix| suffix.den))?)?;
    let (mut numerator, mut denominator_scale10, exponent10) =
        normalize_decimal_exponent(numerator, denominator_scale10, rational.exponent10)?;
    match exponent10 {
        exp if exp > 0 => {
            // The cap now applies to the *reduced* exponent. A raw exponent
            // above 4096 is no longer sufficient evidence of overflow because
            // the decimal denominator may have cancelled it.
            if exp > 4096 {
                return Some(i64::MAX);
            }
            numerator *= pow10_big(exp.try_into().ok()?)?;
        }
        exp if exp < 0 => {
            let magnitude = exp.checked_neg()?.try_into().ok()?;
            let numerator_scale_digits: u32 = numerator.to_str_radix(10).len().try_into().ok()?;
            if magnitude > numerator_scale_digits + 4096 {
                return Some(1);
            }
            denominator_scale10 = denominator_scale10.checked_add(magnitude)?;
        }
        _ => {}
    }

    let denominator = pow10_big(denominator_scale10)?;
    let scaled = ceil_div_bigint(&numerator, &denominator)?;
    bounded_big_i64(&scaled)
}

/// Cancel a positive exponent against the explicit base-10 denominator scale
/// before any overflow classification.
///
/// The denominator is always a product of powers of ten (decimal scale, final
/// scaling, and SI suffix factors), so cancelling `min(exponent, valuation)`
/// factors reduces both the exponent and the denominator. This must happen
/// before the large-positive-exponent cap: a quantity such as
/// `0.` + 4,999 zeros + `1e5000` has a raw exponent of 5000 but a decimal
/// denominator of `10^5000`, so the reduced exponent is zero and the value is
/// exactly one. Tracking the scale as an integer makes cancellation constant
/// time and avoids constructing and repeatedly dividing a huge `BigInt`.
fn normalize_decimal_exponent(
    numerator: BigInt,
    scale10: u32,
    exponent10: i32,
) -> Option<(BigInt, u32, i32)> {
    if exponent10 <= 0 {
        return Some((numerator, scale10, exponent10));
    }
    let positive_exponent: u32 = exponent10.try_into().ok()?;
    let cancelled = scale10.min(positive_exponent);
    Some((
        numerator,
        scale10 - cancelled,
        exponent10.checked_sub(cancelled.try_into().ok()?)?,
    ))
}

fn decimal_power_scale(mut value: i128) -> Option<u32> {
    if value <= 0 {
        return None;
    }
    let mut scale = 0u32;
    while value > 1 && value % 10 == 0 {
        value /= 10;
        scale = scale.checked_add(1)?;
    }
    (value == 1).then_some(scale)
}

fn ceil_div_bigint(value: &BigInt, divisor: &BigInt) -> Option<BigInt> {
    if value < &BigInt::zero() || divisor <= &BigInt::zero() {
        return None;
    }
    let quotient = value / divisor;
    if value % divisor == BigInt::zero() {
        Some(quotient)
    } else {
        Some(quotient + BigInt::one())
    }
}

fn bounded_big_i64(value: &BigInt) -> Option<i64> {
    if value < &BigInt::zero() {
        return None;
    }
    Some(value.to_i64().unwrap_or(i64::MAX))
}

fn pow10_big(exp: u32) -> Option<BigInt> {
    Some(BigInt::from(10u8).pow(exp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kubernetes_quantity_examples() {
        assert_eq!(
            parse_resource_quantity("storage", "1Gi"),
            Some(1_073_741_824)
        );
        assert_eq!(
            parse_resource_quantity("storage", "512Mi"),
            Some(536_870_912)
        );
        assert_eq!(
            parse_resource_quantity("storage", "1024Mi"),
            Some(1_073_741_824)
        );
        assert_eq!(
            parse_resource_quantity("storage", "1.5Gi"),
            Some(1_610_612_736)
        );
        assert_eq!(
            parse_resource_quantity("storage", "1G"),
            Some(1_000_000_000)
        );
        assert_eq!(parse_resource_quantity("storage", "1e3"), Some(1000));
        assert_ne!(
            parse_resource_quantity("storage", "1G"),
            parse_resource_quantity("storage", "1Gi")
        );
        assert_eq!(parse_resource_quantity("storage", "1.000"), Some(1));
        assert_eq!(
            parse_resource_quantity("storage", ".25Gi"),
            Some(268_435_456)
        );
        assert_eq!(parse_resource_quantity("storage", ".0Gi"), Some(0));
        assert_eq!(parse_resource_quantity("storage", "+.0"), Some(0));
        assert_eq!(parse_resource_quantity("memory", ".000Gi"), Some(0));
        assert_eq!(parse_resource_quantity("memory", "+.0Gi"), Some(0));
        assert_eq!(parse_resource_quantity("storage", "1e-39Gi"), None);
        assert_eq!(parse_resource_quantity("storage", "1e3M"), None);
        assert_eq!(parse_resource_quantity("cpu", "1e3m"), None);
        assert_eq!(
            parse_resource_quantity("storage", "1000000000000000000000000000000000000000e-39"),
            Some(1)
        );
        assert_eq!(
            parse_resource_quantity("storage", "1.2345Gi"),
            Some(1_325_534_282)
        );
        assert_eq!(
            parse_resource_quantity("storage", "1.2348Gi"),
            Some(1_325_856_405)
        );
        assert_eq!(
            parse_resource_quantity("storage", "+1Gi"),
            Some(1_073_741_824)
        );
        assert_eq!(parse_resource_quantity("storage", "1k"), Some(1000));
        assert_eq!(parse_resource_quantity("storage", "1m"), Some(1));
        assert_eq!(parse_resource_quantity("storage", "1u"), Some(1));
        assert_eq!(parse_resource_quantity("storage", "1n"), Some(1));
        assert_eq!(parse_resource_quantity("storage", "1K"), None);
        assert_eq!(parse_resource_quantity("storage", "1U"), None);
        assert_eq!(parse_resource_quantity("storage", "1N"), None);
        assert_eq!(
            parse_resource_quantity("storage", "1.000000000000000000000000000000000000000Gi"),
            Some(1_073_741_824)
        );
        assert_eq!(
            parse_resource_quantity("memory", "1.000000000000000000000000000000000000000Gi"),
            Some(1_073_741_824)
        );
        assert_eq!(
            parse_resource_quantity("storage", "1P"),
            Some(1_000_000_000_000_000)
        );
        assert_eq!(
            parse_resource_quantity("storage", "1E"),
            Some(1_000_000_000_000_000_000)
        );
        assert_eq!(
            parse_resource_quantity("storage", "0.12345678901234567890123456789012345678E"),
            Some(123_456_789_012_345_679)
        );
    }

    #[test]
    fn parse_storage_rejects_malformed_and_caps_overflow() {
        assert_eq!(parse_resource_quantity("storage", "-1Gi"), None);
        assert_eq!(parse_resource_quantity("storage", "1GiB"), None);
        assert_eq!(parse_resource_quantity("storage", ""), None);
        assert_eq!(parse_resource_quantity("storage", " 1Gi"), None);
        assert_eq!(parse_resource_quantity("storage", "1Gi "), None);
        assert_eq!(parse_resource_quantity("cpu", " 1"), None);
        assert_eq!(parse_resource_quantity("memory", "1Gi "), None);
        assert_eq!(parse_resource_quantity("memory", "1e3Gi"), None);
        assert_eq!(parse_resource_quantity("example", "1e3M"), None);
        assert_eq!(parse_resource_quantity("cpu", "1e-6m"), None);
        assert_eq!(
            parse_resource_quantity("storage", "1e-9223372036854775808"),
            None
        );
        assert_eq!(
            parse_resource_quantity("storage", "18446744073709551616"),
            Some(i64::MAX)
        );
        assert_eq!(
            parse_resource_quantity("storage", "170141183460469231731687303715884105727"),
            Some(i64::MAX)
        );
        assert_eq!(parse_resource_quantity("cpu", "1e39"), Some(i64::MAX));
        assert_eq!(parse_resource_quantity("storage", "1e39"), Some(i64::MAX));
        assert_eq!(parse_resource_quantity("storage", "1e5000"), Some(i64::MAX));
        assert_eq!(parse_resource_quantity("storage", "1e-5000"), Some(1));
        let large_coefficient_with_negative_exponent = format!("1{}e-5000", "0".repeat(5004));
        assert_eq!(
            parse_resource_quantity("storage", &large_coefficient_with_negative_exponent),
            Some(10_000)
        );
        assert_eq!(parse_resource_quantity("cpu", "0e5000"), Some(0));
        assert_eq!(
            parse_resource_quantity("storage", "10000000000000000000000Ei"),
            Some(i64::MAX)
        );
        assert_eq!(parse_resource_quantity("storage", "10Ei"), Some(i64::MAX));
    }

    #[test]
    fn parse_cpu_examples_are_integer_milli() {
        assert_eq!(parse_resource_quantity("cpu", "1"), Some(1000));
        assert_eq!(parse_resource_quantity("cpu", "500m"), Some(500));
        assert_eq!(parse_resource_quantity("cpu", "1u"), Some(1));
        assert_eq!(parse_resource_quantity("cpu", "1n"), Some(1));
        assert_eq!(parse_resource_quantity("memory", "1u"), Some(1));
        assert_eq!(parse_resource_quantity("memory", "1n"), Some(1));
        assert_eq!(parse_resource_quantity("cpu", "2.5"), Some(2500));
        assert_eq!(parse_resource_quantity("cpu", "1.5k"), Some(1_500_000));
        assert_eq!(parse_resource_quantity("cpu", "1e-3"), Some(1));
        assert_eq!(parse_resource_quantity("cpu", "1e-39"), Some(1));
        assert_eq!(
            parse_resource_quantity("cpu", "0.99999999999999999999999999999999999999e1"),
            Some(10_000)
        );
        assert_eq!(
            parse_resource_quantity("cpu", "0.0000000000000000000001e36"),
            Some(100_000_000_000_000_000)
        );
    }

    #[test]
    fn parse_decimal_scalar_ceil_behavior() {
        assert_eq!(parse_resource_quantity("example", "1.5k"), Some(1500));
        assert_eq!(parse_resource_quantity("example", "1.5"), Some(2));
        assert_eq!(parse_resource_quantity("example", "1.0001"), Some(2));
        assert_eq!(parse_resource_quantity("example", "1.2345"), Some(2));
    }

    #[test]
    fn parse_resource_zero_cases() {
        assert_eq!(parse_resource_quantity("storage", "0"), Some(0));
        assert_eq!(parse_resource_quantity("cpu", "0"), Some(0));
        assert_eq!(parse_resource_quantity("memory", "0"), Some(0));
    }

    #[test]
    fn formatting_contract_is_stable() {
        let cases = [
            ("cpu", 2_000, "2"),
            ("cpu", 2_500, "2500m"),
            ("cpu", -500, "-500m"),
            ("memory", 1024_i64.pow(3), "1Gi"),
            ("memory", 1536, "1536"),
            ("example.com/widgets", 7, "7"),
        ];

        for (resource, value, expected) in cases {
            assert_eq!(format_resource_quantity(resource, value), expected);
        }
    }

    /// Positive decimal exponents must cancel against the decimal denominator
    /// before any overflow cap is applied. A quantity like
    /// `0.` + 4,999 zeros + `1e5000` equals exactly one, not `i64::MAX`.
    #[test]
    fn normalize_decimal_exponent_cancels_huge_positive_exponent() {
        // 0.<4999 zeros>1e5000 == 1 (5000 fractional digits cancel 5000 powers)
        let one_with_huge_exp = format!("0.{}1e5000", "0".repeat(4999));
        assert_eq!(
            parse_resource_quantity("storage", &one_with_huge_exp),
            Some(1)
        );
        // exponent 4999 leaves one outstanding fractional digit -> sub-unit ceil
        let sub_unit = format!("0.{}1e4999", "0".repeat(4999));
        assert_eq!(parse_resource_quantity("storage", &sub_unit), Some(1));
        // exponent 5001 leaves one integer digit -> 10
        let ten = format!("0.{}1e5001", "0".repeat(4999));
        assert_eq!(parse_resource_quantity("storage", &ten), Some(10));
        // a coefficient whose decimal scale only partially cancels
        let partial = format!("0.{}12e5000", "0".repeat(4998));
        assert_eq!(parse_resource_quantity("storage", &partial), Some(12));
        // genuine overflow that remains huge after cancellation still caps
        assert_eq!(parse_resource_quantity("storage", "1e5000"), Some(i64::MAX));
        // zero coefficient with huge exponents stays zero without huge allocation
        assert_eq!(parse_resource_quantity("storage", "0e2147483647"), Some(0));
        assert_eq!(parse_resource_quantity("storage", "0e-2147483647"), Some(0));
        // equivalent long-exponent and ordinary spellings compare equal
        assert_eq!(
            parse_resource_quantity("storage", &one_with_huge_exp),
            parse_resource_quantity("storage", "1")
        );

        for (resource, expected) in [
            ("cpu", 1000),
            ("storage", 1),
            ("memory", 1),
            ("example.com/widgets", 1),
        ] {
            assert_eq!(
                parse_resource_quantity(resource, &one_with_huge_exp),
                Some(expected),
                "resource scaling must remain exact for {resource}"
            );
        }

        assert_eq!(
            parse_resource_quantity("storage", "9223372036854775806"),
            Some(i64::MAX - 1)
        );
        assert_eq!(
            parse_resource_quantity("storage", "9223372036854775807"),
            Some(i64::MAX)
        );
        assert_eq!(
            parse_resource_quantity("storage", "9223372036854775808"),
            Some(i64::MAX)
        );
        assert_eq!(parse_resource_quantity("storage", "1e3M"), None);
        assert_eq!(parse_resource_quantity("memory", "1e3Gi"), None);
    }

    #[test]
    fn normalize_decimal_exponent_reduces_scale_without_bigint_digit_walk() {
        let (numerator, scale10, exponent10) =
            normalize_decimal_exponent(BigInt::from(7), 100_000, 99_999).unwrap();
        assert_eq!(numerator, BigInt::from(7));
        assert_eq!(scale10, 1);
        assert_eq!(exponent10, 0);
    }
}
