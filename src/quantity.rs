const MAX_I64: i128 = i64::MAX as i128;

#[derive(Clone, Copy)]
struct Suffix {
    text: &'static str,
    num: i128,
    den: i128,
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
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    if let Some((number, suffix)) = split_suffix(raw, &BINARY_SUFFIXES) {
        return apply_quantity(parse_decimal_rational(number)?, Some(suffix), 1000);
    }
    if let Some((number, suffix)) = split_suffix(raw, &DECIMAL_SUFFIXES) {
        return apply_quantity(parse_decimal_rational(number)?, Some(suffix), 1000);
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
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let (number, suffix) = match split_suffix(raw, suffixes) {
        Some((number, suffix)) => (number, Some(suffix)),
        None => (raw, None),
    };

    apply_quantity(parse_decimal_rational(number)?, suffix, final_div)
}

fn parse_decimal_rational(raw: &str) -> Option<(i128, i128)> {
    if raw.is_empty() || raw.starts_with('-') {
        return None;
    }

    let raw = raw.strip_prefix('+').unwrap_or(raw);
    if raw.is_empty() {
        return None;
    }

    let (number, exponent) = split_exponent(raw)?;
    let (mut numerator, mut denominator) = parse_decimal_rational_no_exponent(number)?;
    match exponent {
        exp if exp >= 0 => {
            numerator = numerator.checked_mul(pow10(exp.try_into().ok()?)?)?;
        }
        exp => {
            let divisor = pow10(u32::try_from(exp.checked_neg()?).ok()?)?;
            denominator = denominator.checked_mul(divisor)?;
        }
    }
    Some((numerator, denominator))
}

fn parse_decimal_rational_no_exponent(raw: &str) -> Option<(i128, i128)> {
    let (int_part, mut frac_part) = match raw.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (raw, ""),
    };
    frac_part = frac_part.trim_end_matches('0');

    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if int_part.chars().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    if frac_part.chars().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    let int_value: i128 = if int_part.is_empty() {
        0
    } else {
        parse_decimal_digits(int_part)?
    };
    let frac_scale = pow10(frac_part.len().try_into().ok()?)?;
    let frac_value = if frac_part.is_empty() {
        0
    } else {
        parse_decimal_digits(frac_part)?
    };
    let numerator = int_value.checked_mul(frac_scale)?.checked_add(frac_value)?;
    Some((numerator, frac_scale))
}

fn parse_decimal_digits(raw: &str) -> Option<i128> {
    let mut value = 0_i128;
    for byte in raw.bytes() {
        let digit = i128::from(byte.checked_sub(b'0')?);
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(digit)?;
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
    (numerator, denominator): (i128, i128),
    suffix: Option<&Suffix>,
    final_div: i128,
) -> Option<i64> {
    let numerator = numerator.checked_mul(1000)?;
    let (numerator, denominator) = if let Some(suffix) = suffix {
        (
            numerator.checked_mul(suffix.num)?,
            denominator.checked_mul(suffix.den)?,
        )
    } else {
        (numerator, denominator)
    };
    let denominator = denominator.checked_mul(final_div)?;
    let scaled = ceil_div_positive(numerator, denominator)?;
    bounded_i64(scaled)
}

fn ceil_div_positive(value: i128, divisor: i128) -> Option<i128> {
    debug_assert!(divisor > 0);
    debug_assert!(value >= 0);
    if divisor <= 0 || value < 0 {
        return None;
    }
    let quotient = value / divisor;
    if value % divisor == 0 {
        Some(quotient)
    } else {
        quotient.checked_add(1)
    }
}

fn bounded_i64(value: i128) -> Option<i64> {
    (0..=MAX_I64).contains(&value).then_some(value as i64)
}

fn pow10(exp: u32) -> Option<i128> {
    let mut value = 1_i128;
    for _ in 0..exp {
        value = value.checked_mul(10)?;
    }
    Some(value)
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
    }

    #[test]
    fn parse_storage_rejects_negative_malformed_or_overflow() {
        assert_eq!(parse_resource_quantity("storage", "-1Gi"), None);
        assert_eq!(parse_resource_quantity("storage", "1GiB"), None);
        assert_eq!(parse_resource_quantity("storage", ""), None);
        assert_eq!(
            parse_resource_quantity("storage", "1e-9223372036854775808"),
            None
        );
        assert_eq!(
            parse_resource_quantity("storage", "18446744073709551616"),
            None
        );
        assert_eq!(
            parse_resource_quantity("storage", "170141183460469231731687303715884105727"),
            None
        );
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
}
