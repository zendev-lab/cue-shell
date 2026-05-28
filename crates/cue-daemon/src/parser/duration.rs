use std::time::Duration;

pub(super) fn parse_duration_str(s: &str) -> Option<Duration> {
    if s.ends_with("ms") {
        let n: u64 = s.strip_suffix("ms")?.parse().ok()?;
        return Some(Duration::from_millis(n));
    }

    for (suffix, multiplier) in [("s", 1u64), ("m", 60), ("h", 3600)] {
        if s.ends_with(suffix) {
            let n: u64 = s.strip_suffix(suffix)?.parse().ok()?;
            return n.checked_mul(multiplier).map(Duration::from_secs);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_duration_units() {
        assert_eq!(
            parse_duration_str("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration_str("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration_str("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration_str("1h"), Some(Duration::from_secs(3600)));
    }

    #[test]
    fn rejects_invalid_or_overflowing_duration_values() {
        assert_eq!(parse_duration_str("5x"), None);
        assert_eq!(parse_duration_str("xs"), None);
        assert_eq!(parse_duration_str(&format!("{}h", u64::MAX)), None);
    }
}
