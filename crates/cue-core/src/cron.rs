use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Lifecycle state for a persisted cron entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CronStatus {
    Scheduled,
    Paused,
    Completed,
    Expired,
    Failed,
}

/// Cron schedule expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronSchedule {
    /// `every 5m` — repeating interval.
    Interval(Duration),
    /// `at 09:00 [on weekdays]` — specific time with optional day filter.
    TimeOfDay {
        /// Seconds from midnight.
        time_secs: u32,
        days: Option<DayFilter>,
    },
    /// `in 30s` — one-shot delay, auto-removed after trigger.
    Delay(Duration),
    /// `daily`, `hourly`, `weekly`, `monthly`.
    Preset(CronPreset),
    /// `cron "*/5 * * * *"` — validated standard crontab expression.
    Crontab(CrontabSchedule),
}

/// Named schedule presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronPreset {
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

/// Day-of-week filter for `at` schedules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DayFilter {
    pub days: Vec<Weekday>,
}

/// Days of the week.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Weekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl CronSchedule {
    /// Whether this is a one-shot schedule (should be removed after trigger).
    pub fn is_oneshot(&self) -> bool {
        matches!(self, Self::Delay(_))
    }
}

impl CronStatus {
    pub fn is_runnable(self) -> bool {
        matches!(self, Self::Scheduled)
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Expired | Self::Failed)
    }
}

impl CronSchedule {
    /// Human-readable display string.
    pub fn display(&self) -> String {
        match self {
            Self::Interval(d) => format!("every {}", format_duration_short(*d)),
            Self::Delay(d) => format!("in {}", format_duration_short(*d)),
            Self::TimeOfDay { time_secs, days } => {
                let h = time_secs / 3600;
                let m = (time_secs % 3600) / 60;
                let time_str = format!("{h:02}:{m:02}");
                match days {
                    Some(days) => format!("at {time_str} on {}", days.display()),
                    None => format!("at {time_str}"),
                }
            }
            Self::Preset(p) => format!("{p:?}").to_lowercase(),
            Self::Crontab(expr) => format!("cron {}", expr.as_str()),
        }
    }
}

/// Parse the persisted/user-facing cron schedule text into a core schedule.
pub fn parse_schedule_text(text: &str) -> Option<CronSchedule> {
    let text = text.trim();
    let words: Vec<&str> = text.split_whitespace().collect();
    let keyword = *words.first()?;
    match keyword {
        "every" if words.len() == 2 => Some(CronSchedule::Interval(parse_schedule_duration(
            words.get(1)?,
        )?)),
        "in" if words.len() == 2 => {
            Some(CronSchedule::Delay(parse_schedule_duration(words.get(1)?)?))
        }
        "at" => {
            let time_secs = parse_time_of_day(words.get(1)?)?;
            let days = if words.get(2) == Some(&"on") {
                Some(parse_day_filter(words.get(3)?)?)
            } else {
                None
            };
            if !(words.len() == 2 || words.len() == 4 && words.get(2) == Some(&"on")) {
                return None;
            }
            Some(CronSchedule::TimeOfDay { time_secs, days })
        }
        "daily" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Daily)),
        "hourly" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Hourly)),
        "weekly" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Weekly)),
        "monthly" if words.len() == 1 => Some(CronSchedule::Preset(CronPreset::Monthly)),
        "cron" if words.len() == 6 => {
            let expr = words.get(1..6)?.join(" ");
            Some(CronSchedule::Crontab(CrontabSchedule::parse(&expr)?))
        }
        _ => Some(CronSchedule::Crontab(CrontabSchedule::parse(text)?)),
    }
}

pub fn parse_time_of_day(input: &str) -> Option<u32> {
    let normalized = input.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "midnight" => return Some(0),
        "noon" => return Some(12 * 3600),
        _ => {}
    }

    let (core, meridiem) = if let Some(stripped) = normalized.strip_suffix("am") {
        (stripped, Some("am"))
    } else if let Some(stripped) = normalized.strip_suffix("pm") {
        (stripped, Some("pm"))
    } else {
        (normalized.as_str(), None)
    };

    let (mut hour, minute) = if let Some((hour, minute)) = core.split_once(':') {
        (parse_ascii_u32(hour)?, parse_ascii_u32(minute)?)
    } else {
        (parse_ascii_u32(core)?, 0)
    };
    if minute >= 60 {
        return None;
    }

    match meridiem {
        Some("am") => {
            if hour == 12 {
                hour = 0;
            } else if hour > 11 {
                return None;
            }
        }
        Some("pm") => {
            if hour < 12 {
                hour += 12;
            } else if hour > 12 {
                return None;
            }
        }
        None if hour > 23 => return None,
        None => {}
        _ => return None,
    }

    Some(hour * 3600 + minute * 60)
}

pub fn parse_day_filter(input: &str) -> Option<DayFilter> {
    let normalized = input.trim().to_ascii_lowercase();
    let days = match normalized.as_str() {
        "daily" => Weekday::ORDERED.to_vec(),
        "weekdays" => vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
        ],
        "weekends" => vec![Weekday::Sat, Weekday::Sun],
        _ => {
            let mut out = Vec::new();
            for part in normalized.split(',') {
                let part = part.trim();
                if let Some((start, end)) = part.split_once('-') {
                    out.extend(expand_weekday_range(
                        Weekday::parse_name(start)?,
                        Weekday::parse_name(end)?,
                    ));
                } else {
                    out.push(Weekday::parse_name(part)?);
                }
            }
            out
        }
    };
    Some(DayFilter { days })
}

impl DayFilter {
    pub fn display(&self) -> String {
        match self.days.as_slice() {
            days if days == Weekday::ORDERED => "daily".to_string(),
            [
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ] => "weekdays".to_string(),
            [Weekday::Sat, Weekday::Sun] => "weekends".to_string(),
            days => days
                .iter()
                .map(|day| day.short_name())
                .collect::<Vec<_>>()
                .join(","),
        }
    }
}

impl Weekday {
    const ORDERED: [Weekday; 7] = [
        Weekday::Mon,
        Weekday::Tue,
        Weekday::Wed,
        Weekday::Thu,
        Weekday::Fri,
        Weekday::Sat,
        Weekday::Sun,
    ];

    fn parse_name(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "mon" | "monday" => Some(Weekday::Mon),
            "tue" | "tues" | "tuesday" => Some(Weekday::Tue),
            "wed" | "wednesday" => Some(Weekday::Wed),
            "thu" | "thur" | "thurs" | "thursday" => Some(Weekday::Thu),
            "fri" | "friday" => Some(Weekday::Fri),
            "sat" | "saturday" => Some(Weekday::Sat),
            "sun" | "sunday" => Some(Weekday::Sun),
            _ => None,
        }
    }

    fn short_name(self) -> &'static str {
        match self {
            Weekday::Mon => "mon",
            Weekday::Tue => "tue",
            Weekday::Wed => "wed",
            Weekday::Thu => "thu",
            Weekday::Fri => "fri",
            Weekday::Sat => "sat",
            Weekday::Sun => "sun",
        }
    }
}

fn expand_weekday_range(start: Weekday, end: Weekday) -> Vec<Weekday> {
    let start_idx = Weekday::ORDERED
        .iter()
        .position(|day| *day == start)
        .expect("known weekday");
    let end_idx = Weekday::ORDERED
        .iter()
        .position(|day| *day == end)
        .expect("known weekday");
    if start_idx <= end_idx {
        Weekday::ORDERED[start_idx..=end_idx].to_vec()
    } else {
        Weekday::ORDERED[start_idx..]
            .iter()
            .chain(Weekday::ORDERED[..=end_idx].iter())
            .copied()
            .collect()
    }
}

fn format_duration_short(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    if d.subsec_nanos() > 0 && d.as_nanos().is_multiple_of(1_000_000) {
        return format!("{}ms", d.as_millis());
    }
    if secs == 0 {
        return "0s".into();
    }
    if secs.is_multiple_of(86400) {
        return format!("{}d", secs / 86400);
    }
    if secs.is_multiple_of(3600) {
        return format!("{}h", secs / 3600);
    }
    if secs.is_multiple_of(60) {
        return format!("{}m", secs / 60);
    }
    format!("{secs}s")
}

fn parse_duration_short_text(text: &str) -> Option<std::time::Duration> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }

    if let Some(num_part) = text.strip_suffix("ms") {
        return parse_ascii_u64(num_part).map(std::time::Duration::from_millis);
    }

    for (suffix, multiplier) in [("s", 1_u64), ("m", 60), ("h", 3_600), ("d", 86_400)] {
        if let Some(num_part) = text.strip_suffix(suffix) {
            return parse_ascii_u64(num_part)?
                .checked_mul(multiplier)
                .map(std::time::Duration::from_secs);
        }
    }

    None
}

fn parse_schedule_duration(text: &str) -> Option<std::time::Duration> {
    let duration = parse_duration_short_text(text)?;
    (!duration.is_zero()).then_some(duration)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CrontabExpr {
    minute: Vec<u32>,
    hour: Vec<u32>,
    day_of_month: Vec<u32>,
    day_of_month_any: bool,
    month: Vec<u32>,
    day_of_week: Vec<u32>,
    day_of_week_any: bool,
}

/// Validated five-field crontab schedule text plus its parsed matcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrontabSchedule {
    expr: String,
    matcher: CrontabExpr,
}

impl CrontabSchedule {
    pub fn parse(expr: &str) -> Option<Self> {
        let expr = normalize_crontab_expr(expr)?;
        let matcher = parse_crontab_expr(&expr)?;
        Some(Self { expr, matcher })
    }

    pub fn as_str(&self) -> &str {
        &self.expr
    }

    /// Match against minute, hour, day-of-month, month, and day-of-week.
    ///
    /// `day_of_week` uses cron numbering: Sunday is `0`.
    /// When both day fields are restricted, standard cron treats them as OR.
    pub fn matches(
        &self,
        minute: u32,
        hour: u32,
        day_of_month: u32,
        month: u32,
        day_of_week: u32,
    ) -> bool {
        self.matcher
            .matches(minute, hour, day_of_month, month, day_of_week)
    }
}

fn normalize_crontab_expr(expr: &str) -> Option<String> {
    let fields = expr.split_whitespace().collect::<Vec<_>>();
    (fields.len() == 5).then(|| fields.join(" "))
}

impl Serialize for CrontabSchedule {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.expr)
    }
}

impl<'de> Deserialize<'de> for CrontabSchedule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expr = String::deserialize(deserializer)?;
        Self::parse(&expr).ok_or_else(|| de::Error::custom("invalid crontab expression"))
    }
}

impl CrontabExpr {
    /// Match against minute, hour, day-of-month, month, and day-of-week.
    ///
    /// `day_of_week` uses cron numbering: Sunday is `0`.
    /// When both day fields are restricted, standard cron treats them as OR.
    pub fn matches(
        &self,
        minute: u32,
        hour: u32,
        day_of_month: u32,
        month: u32,
        day_of_week: u32,
    ) -> bool {
        let day_of_month_matches = self.day_of_month.contains(&day_of_month);
        let day_of_week_matches = self.day_of_week.contains(&day_of_week);
        let day_matches = if self.day_of_month_any || self.day_of_week_any {
            day_of_month_matches && day_of_week_matches
        } else {
            day_of_month_matches || day_of_week_matches
        };

        self.minute.contains(&minute)
            && self.hour.contains(&hour)
            && self.month.contains(&month)
            && day_matches
    }
}

fn parse_crontab_expr(expr: &str) -> Option<CrontabExpr> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return None;
    }
    let day_of_month = parse_cron_field(fields[2], 1, 31, &[])?;
    let mut day_of_week = parse_cron_field(
        fields[4],
        0,
        7,
        &[
            ("sun", 0),
            ("mon", 1),
            ("tue", 2),
            ("wed", 3),
            ("thu", 4),
            ("fri", 5),
            ("sat", 6),
        ],
    )?
    .into_iter()
    .map(|value| if value == 7 { 0 } else { value })
    .collect::<Vec<_>>();
    day_of_week.sort_unstable();
    day_of_week.dedup();
    let day_of_month_any = covers_full_range(&day_of_month, 1, 31);
    let day_of_week_any = covers_full_range(&day_of_week, 0, 6);

    Some(CrontabExpr {
        minute: parse_cron_field(fields[0], 0, 59, &[])?,
        hour: parse_cron_field(fields[1], 0, 23, &[])?,
        day_of_month,
        day_of_month_any,
        month: parse_cron_field(
            fields[3],
            1,
            12,
            &[
                ("jan", 1),
                ("feb", 2),
                ("mar", 3),
                ("apr", 4),
                ("may", 5),
                ("jun", 6),
                ("jul", 7),
                ("aug", 8),
                ("sep", 9),
                ("oct", 10),
                ("nov", 11),
                ("dec", 12),
            ],
        )?,
        day_of_week,
        day_of_week_any,
    })
}

fn covers_full_range(values: &[u32], min: u32, max: u32) -> bool {
    values.len() == (max - min + 1) as usize
        && values
            .iter()
            .copied()
            .zip(min..=max)
            .all(|(value, expected)| value == expected)
}

fn parse_cron_field(field: &str, min: u32, max: u32, names: &[(&str, u32)]) -> Option<Vec<u32>> {
    let normalized = field.trim().to_ascii_lowercase();
    let mut values = Vec::new();
    for part in normalized.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let expanded = if part == "*" {
            (min..=max).collect::<Vec<_>>()
        } else if let Some(step_text) = part.strip_prefix("*/") {
            let step = parse_ascii_u32(step_text)?;
            if step == 0 {
                return None;
            }
            (min..=max).step_by(step as usize).collect::<Vec<_>>()
        } else {
            parse_cron_part(part, min, max, names)?
        };
        values.extend(expanded);
    }
    values.sort_unstable();
    values.dedup();
    Some(values)
}

fn parse_cron_part(part: &str, min: u32, max: u32, names: &[(&str, u32)]) -> Option<Vec<u32>> {
    let (range_part, step) = if let Some((range, step)) = part.split_once('/') {
        let step = parse_ascii_u32(step)?;
        if step == 0 {
            return None;
        }
        (range, Some(step))
    } else {
        (part, None)
    };

    let mut values = if let Some((start, end)) = range_part.split_once('-') {
        let start = parse_cron_value(start, names)?;
        let end = parse_cron_value(end, names)?;
        if start > end || start < min || end > max {
            return None;
        }
        (start..=end).collect::<Vec<_>>()
    } else {
        let value = parse_cron_value(range_part, names)?;
        if value < min || value > max {
            return None;
        }
        vec![value]
    };

    if let Some(step) = step {
        values = values
            .into_iter()
            .enumerate()
            .filter_map(|(idx, value)| (idx as u32).is_multiple_of(step).then_some(value))
            .collect();
    }
    Some(values)
}

fn parse_cron_value(input: &str, names: &[(&str, u32)]) -> Option<u32> {
    parse_ascii_u32(input).or_else(|| {
        names
            .iter()
            .find_map(|(name, value)| (*name == input).then_some(*value))
    })
}

fn parse_ascii_u32(input: &str) -> Option<u32> {
    if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    input.parse().ok()
}

fn parse_ascii_u64(input: &str) -> Option<u64> {
    if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    input.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crontab_schedule(expr: &str) -> CronSchedule {
        CronSchedule::Crontab(CrontabSchedule::parse(expr).expect("valid crontab schedule"))
    }

    #[test]
    fn parse_named_times() {
        assert_eq!(parse_time_of_day("midnight"), Some(0));
        assert_eq!(parse_time_of_day("noon"), Some(12 * 3600));
        assert_eq!(parse_time_of_day("9:30pm"), Some(21 * 3600 + 30 * 60));
        assert_eq!(parse_time_of_day("24:00"), None);
        assert_eq!(parse_time_of_day("+9am"), None);
        assert_eq!(parse_time_of_day("9:+30"), None);
    }

    #[test]
    fn parse_day_filters() {
        assert_eq!(
            parse_day_filter("monday,wednesday").map(|filter| filter.days),
            Some(vec![Weekday::Mon, Weekday::Wed])
        );
        assert_eq!(
            parse_day_filter("fri-mon").map(|filter| filter.days),
            Some(vec![Weekday::Fri, Weekday::Sat, Weekday::Sun, Weekday::Mon])
        );
        assert!(parse_day_filter("noday").is_none());
    }

    #[test]
    fn displays_specific_day_filter_without_widening_it() {
        let schedule = CronSchedule::TimeOfDay {
            time_secs: 9 * 3600,
            days: Some(DayFilter {
                days: vec![Weekday::Mon],
            }),
        };

        assert_eq!(schedule.display(), "at 09:00 on mon");
        assert_eq!(parse_schedule_text(&schedule.display()), Some(schedule));
    }

    #[test]
    fn parse_schedule_every() {
        assert_eq!(
            parse_schedule_text("every 5m"),
            Some(CronSchedule::Interval(std::time::Duration::from_secs(300)))
        );
    }

    #[test]
    fn parse_schedule_preserves_millisecond_duration() {
        let schedule = CronSchedule::Interval(std::time::Duration::from_millis(500));

        assert_eq!(schedule.display(), "every 500ms");
        assert_eq!(parse_schedule_text(&schedule.display()), Some(schedule));
    }

    #[test]
    fn parse_schedule_in() {
        assert_eq!(
            parse_schedule_text("in 30s"),
            Some(CronSchedule::Delay(std::time::Duration::from_secs(30)))
        );
    }

    #[test]
    fn parse_schedule_hours() {
        assert_eq!(
            parse_schedule_text("every 2h"),
            Some(CronSchedule::Interval(std::time::Duration::from_secs(7200)))
        );
    }

    #[test]
    fn parse_schedule_at_on_weekdays() {
        assert_eq!(
            parse_schedule_text("at 9am on weekdays"),
            Some(CronSchedule::TimeOfDay {
                time_secs: 9 * 3600,
                days: Some(DayFilter {
                    days: vec![
                        Weekday::Mon,
                        Weekday::Tue,
                        Weekday::Wed,
                        Weekday::Thu,
                        Weekday::Fri,
                    ],
                }),
            })
        );
    }

    #[test]
    fn parse_schedule_crontab() {
        assert_eq!(
            parse_schedule_text("cron */5 * * * *"),
            Some(crontab_schedule("*/5 * * * *"))
        );
        assert_eq!(
            parse_schedule_text("  */5 * * * *  "),
            Some(crontab_schedule("*/5 * * * *"))
        );
    }

    #[test]
    fn crontab_schedule_validates_and_normalizes_source() {
        let schedule = CrontabSchedule::parse(" */5   * * *  mon-fri ").unwrap();

        assert_eq!(schedule.as_str(), "*/5 * * * mon-fri");
        assert!(schedule.matches(10, 0, 1, 1, 1));
        assert!(CrontabSchedule::parse("cron */5 * * * *").is_none());
        assert!(CrontabSchedule::parse("60 * * * *").is_none());
    }

    #[test]
    fn crontab_schedule_deserialize_rejects_invalid_source() {
        let schedule: CrontabSchedule =
            serde_json::from_str("\"*/5 * * * *\"").expect("valid crontab JSON");
        assert_eq!(schedule.as_str(), "*/5 * * * *");

        let error = serde_json::from_str::<CrontabSchedule>("\"60 * * * *\"")
            .expect_err("invalid crontab must not deserialize");
        assert!(error.to_string().contains("invalid crontab expression"));
    }

    #[test]
    fn parse_schedule_rejects_invalid_crontab_fields() {
        assert!(parse_schedule_text("cron 60 * * * *").is_none());
        assert!(parse_schedule_text("cron */0 * * * *").is_none());
        assert!(parse_schedule_text("cron * * * bad *").is_none());
        assert!(parse_schedule_text("cron +1 * * * *").is_none());
    }

    #[test]
    fn crontab_schedule_matches_supported_steps_names_and_sunday_alias() {
        let weekday_workday = CrontabSchedule::parse("*/15 9-17 * jan mon-fri").unwrap();
        assert!(weekday_workday.matches(30, 10, 12, 1, 1));
        assert!(!weekday_workday.matches(31, 10, 12, 1, 1));
        assert!(!weekday_workday.matches(30, 10, 12, 1, 0));

        let sunday_alias = CrontabSchedule::parse("0 0 * * 7").unwrap();
        assert!(sunday_alias.matches(0, 0, 1, 1, 0));
    }

    #[test]
    fn crontab_schedule_uses_standard_day_of_month_or_day_of_week_semantics() {
        let first_of_month_or_monday = CrontabSchedule::parse("0 0 1 * mon").unwrap();
        assert!(first_of_month_or_monday.matches(0, 0, 1, 1, 2));
        assert!(first_of_month_or_monday.matches(0, 0, 2, 1, 1));
        assert!(!first_of_month_or_monday.matches(0, 0, 2, 1, 2));

        let monday_only = CrontabSchedule::parse("0 0 * * mon").unwrap();
        assert!(monday_only.matches(0, 0, 1, 1, 1));
        assert!(!monday_only.matches(0, 0, 1, 1, 2));
    }

    #[test]
    fn parse_schedule_invalid() {
        assert!(parse_schedule_text("every").is_none());
        assert!(parse_schedule_text("every 0s").is_none());
        assert!(parse_schedule_text("in 0s").is_none());
        assert!(parse_schedule_text("at").is_none());
        assert!(parse_schedule_text("cron * * * *").is_none());
        assert!(parse_schedule_text("every 30m 9am-5pm weekdays").is_none());
    }

    #[test]
    fn failed_cron_status_is_terminal_not_runnable() {
        assert!(CronStatus::Failed.is_terminal());
        assert!(!CronStatus::Failed.is_runnable());
    }
}
