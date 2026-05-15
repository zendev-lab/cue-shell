use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Lifecycle state for a persisted cron entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CronStatus {
    Scheduled,
    Paused,
    Completed,
    Expired,
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
    /// `cron "*/5 * * * *"` — standard crontab expression.
    Crontab(String),
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
        matches!(self, Self::Completed | Self::Expired)
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
                    Some(_) => format!("at {time_str} on weekdays"),
                    None => format!("at {time_str}"),
                }
            }
            Self::Preset(p) => format!("{p:?}").to_lowercase(),
            Self::Crontab(expr) => format!("cron {expr}"),
        }
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
        (hour.parse::<u32>().ok()?, minute.parse::<u32>().ok()?)
    } else {
        (core.parse::<u32>().ok()?, 0)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_named_times() {
        assert_eq!(parse_time_of_day("midnight"), Some(0));
        assert_eq!(parse_time_of_day("noon"), Some(12 * 3600));
        assert_eq!(parse_time_of_day("9:30pm"), Some(21 * 3600 + 30 * 60));
        assert_eq!(parse_time_of_day("24:00"), None);
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
}
