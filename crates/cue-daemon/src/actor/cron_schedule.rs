use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Local, LocalResult, NaiveTime, TimeZone,
    Timelike,
};
use std::time::Duration;
use tokio::time::Instant;

use cue_core::cron::{CronPreset, CronSchedule, CrontabSchedule, DayFilter, Weekday};

const CRONTAB_LOOKAHEAD_MINUTES: usize = 5 * 366 * 24 * 60;

pub(super) fn next_trigger_instant(schedule: &CronSchedule, elapsed: Duration) -> Option<Instant> {
    match schedule {
        CronSchedule::Interval(duration) => Some(Instant::now() + *duration),
        CronSchedule::Delay(duration) => {
            let remaining = duration.saturating_sub(elapsed);
            if remaining.is_zero() && !elapsed.is_zero() {
                None
            } else {
                Some(Instant::now() + remaining)
            }
        }
        CronSchedule::TimeOfDay { time_secs, days } => Some(instant_from_local(
            next_time_of_day_occurrence(Local::now(), *time_secs, days.as_ref())?,
        )),
        CronSchedule::Preset(preset) => Some(instant_from_local(next_preset_occurrence(
            Local::now(),
            *preset,
        )?)),
        CronSchedule::Crontab(expr) => Some(instant_from_local(next_crontab_occurrence(
            Local::now(),
            expr,
        )?)),
    }
}

fn instant_from_local(target: DateTime<Local>) -> Instant {
    let delay = match (target - Local::now()).to_std() {
        Ok(delay) => delay,
        Err(_) => Duration::ZERO,
    };
    Instant::now() + delay
}

fn next_time_of_day_occurrence(
    now: DateTime<Local>,
    time_secs: u32,
    days: Option<&DayFilter>,
) -> Option<DateTime<Local>> {
    next_time_of_day_occurrence_with(now, time_secs, days, local_datetime)
}

fn next_time_of_day_occurrence_with(
    now: DateTime<Local>,
    time_secs: u32,
    days: Option<&DayFilter>,
    mut resolve_local: impl FnMut(i32, u32, u32, u32, u32) -> Option<DateTime<Local>>,
) -> Option<DateTime<Local>> {
    let time = NaiveTime::from_num_seconds_from_midnight_opt(time_secs, 0)?;
    for day_offset in 0..14 {
        let date = now.date_naive() + ChronoDuration::days(day_offset);
        let weekday = chrono_weekday_to_core(date.weekday());
        if days.is_none_or(|filter| filter.days.contains(&weekday)) {
            let Some(candidate) = resolve_local(
                date.year(),
                date.month(),
                date.day(),
                time.hour(),
                time.minute(),
            ) else {
                continue;
            };
            if candidate > now {
                return Some(candidate);
            }
        }
    }
    None
}

fn next_preset_occurrence(now: DateTime<Local>, preset: CronPreset) -> Option<DateTime<Local>> {
    match preset {
        CronPreset::Hourly => {
            let next =
                now.with_minute(0)?.with_second(0)?.with_nanosecond(0)? + ChronoDuration::hours(1);
            Some(next)
        }
        CronPreset::Daily => {
            let date = now.date_naive() + ChronoDuration::days(1);
            local_datetime(date.year(), date.month(), date.day(), 0, 0)
        }
        CronPreset::Weekly => {
            let today = now.date_naive();
            let days_until_monday = (8 - today.weekday().number_from_monday()) % 7;
            let offset = if days_until_monday == 0 {
                7
            } else {
                days_until_monday
            };
            let date = today + ChronoDuration::days(offset.into());
            local_datetime(date.year(), date.month(), date.day(), 0, 0)
        }
        CronPreset::Monthly => {
            let (year, month) = if now.month() == 12 {
                (now.year() + 1, 1)
            } else {
                (now.year(), now.month() + 1)
            };
            local_datetime(year, month, 1, 0, 0)
        }
    }
}

fn next_crontab_occurrence(
    now: DateTime<Local>,
    expr: &CrontabSchedule,
) -> Option<DateTime<Local>> {
    let mut candidate = now.with_second(0)?.with_nanosecond(0)? + ChronoDuration::minutes(1);
    for _ in 0..CRONTAB_LOOKAHEAD_MINUTES {
        let weekday = match candidate.weekday() {
            chrono::Weekday::Sun => 0,
            other => other.number_from_monday(),
        };
        if expr.matches(
            candidate.minute(),
            candidate.hour(),
            candidate.day(),
            candidate.month(),
            weekday,
        ) {
            return Some(candidate);
        }
        candidate += ChronoDuration::minutes(1);
    }
    None
}

fn chrono_weekday_to_core(day: chrono::Weekday) -> Weekday {
    match day {
        chrono::Weekday::Mon => Weekday::Mon,
        chrono::Weekday::Tue => Weekday::Tue,
        chrono::Weekday::Wed => Weekday::Wed,
        chrono::Weekday::Thu => Weekday::Thu,
        chrono::Weekday::Fri => Weekday::Fri,
        chrono::Weekday::Sat => Weekday::Sat,
        chrono::Weekday::Sun => Weekday::Sun,
    }
}

fn local_datetime(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
) -> Option<DateTime<Local>> {
    match Local.with_ymd_and_hms(year, month, day, hour, minute, 0) {
        LocalResult::Single(dt) => Some(dt),
        LocalResult::Ambiguous(early, _) => Some(early),
        LocalResult::None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overdue_delay_has_no_next_trigger() {
        let schedule = CronSchedule::Delay(std::time::Duration::from_secs(5));

        assert!(next_trigger_instant(&schedule, Duration::from_secs(6)).is_none());
        assert!(next_trigger_instant(&schedule, Duration::ZERO).is_some());
    }

    #[test]
    fn past_local_target_schedules_immediately() {
        let before = Instant::now();
        let instant = instant_from_local(Local::now() - ChronoDuration::seconds(1));
        let after = Instant::now();

        assert!(instant >= before);
        assert!(instant <= after + Duration::from_millis(10));
    }

    #[test]
    fn time_of_day_skips_nonexistent_local_candidate() {
        let now = local_datetime(2026, 1, 1, 8, 0).unwrap();
        let time_secs = 9 * 60 * 60;
        let mut first_match = true;

        let next = next_time_of_day_occurrence_with(
            now,
            time_secs,
            None,
            |year, month, day, hour, minute| {
                if first_match {
                    first_match = false;
                    return None;
                }
                local_datetime(year, month, day, hour, minute)
            },
        )
        .unwrap();

        assert_eq!(
            next.date_naive(),
            now.date_naive() + ChronoDuration::days(1)
        );
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn crontab_next_trigger_handles_leap_day_schedule() {
        let now = local_datetime(2026, 3, 1, 0, 0).unwrap();
        let schedule = CrontabSchedule::parse("0 0 29 feb *").unwrap();
        let next = next_crontab_occurrence(now, &schedule).unwrap();

        assert_eq!(next.year(), 2028);
        assert_eq!(next.month(), 2);
        assert_eq!(next.day(), 29);
        assert_eq!(next.hour(), 0);
        assert_eq!(next.minute(), 0);
    }
}
