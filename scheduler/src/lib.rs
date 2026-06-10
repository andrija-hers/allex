use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone,
    Timelike, Utc,
};
use chrono_tz::Tz;
use serde::Deserialize;
use std::{cmp, collections::HashMap, error::Error as StdError};
use thiserror::Error;

const DST_SEARCH_LIMIT_MINUTES: i64 = 24 * 60;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Schedule {
    pub timezone: String,
    pub weekdays: Vec<u8>,
    pub time: [u32; 2],
}

pub trait ScheduleTrait {
    fn timezone(&self) -> &str;
    fn weekdays(&self) -> &[u8];
    fn time(&self) -> [u32; 2];

    fn as_schedule(&self) -> Schedule {
        Schedule {
            timezone: self.timezone().to_owned(),
            weekdays: self.weekdays().to_vec(),
            time: self.time(),
        }
    }

    fn analyze_at(&self, instant: DateTime<Utc>) -> Result<ScheduleAnalysis, ScheduleError> {
        self.as_schedule().analyze_at(instant)
    }

    fn next_scheduled_time_at(
        &self,
        instant: DateTime<Utc>,
    ) -> Result<NextScheduledTime, ScheduleError> {
        self.as_schedule().next_scheduled_time_at(instant)
    }

    fn scheduled_time_on_day(&self, date: NaiveDate) -> Result<DateTime<Utc>, ScheduleError> {
        self.as_schedule().scheduled_time_on_day(date)
    }
}

impl ScheduleTrait for Schedule {
    fn timezone(&self) -> &str {
        &self.timezone
    }

    fn weekdays(&self) -> &[u8] {
        &self.weekdays
    }

    fn time(&self) -> [u32; 2] {
        self.time
    }
}

impl Schedule {
    pub fn analyze_at(&self, instant: DateTime<Utc>) -> Result<ScheduleAnalysis, ScheduleError> {
        PreparedSchedule::try_from(self)?.analyze_at(instant)
    }

    pub fn next_scheduled_time_at(
        &self,
        instant: DateTime<Utc>,
    ) -> Result<NextScheduledTime, ScheduleError> {
        PreparedSchedule::try_from(self)?.next_scheduled_time_at(instant)
    }

    pub fn scheduled_time_on_day(&self, date: NaiveDate) -> Result<DateTime<Utc>, ScheduleError> {
        Ok(PreparedSchedule::try_from(self)?
            .scheduled_instant(date)?
            .with_timezone(&Utc))
    }

    pub fn parsed_timezone(&self) -> Result<Tz, ScheduleError> {
        self.timezone
            .parse()
            .map_err(|_| ScheduleError::InvalidTimezone(self.timezone.clone()))
    }

    pub fn parsed_time(&self) -> Result<NaiveTime, ScheduleError> {
        let [hour, minute] = self.time;

        NaiveTime::from_hms_opt(hour, minute, 0).ok_or(ScheduleError::InvalidTime { hour, minute })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleAnalysis {
    pub last_ticked_day: NaiveDate,
    pub earliest_unticked_day: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextScheduledTime {
    pub date: NaiveDate,
    pub instant: DateTime<Utc>,
}

impl ScheduleAnalysis {
    pub fn last_ticked_day_yyyymmdd(&self) -> String {
        yyyymmdd(self.last_ticked_day)
    }

    pub fn earliest_unticked_day_yyyymmdd(&self) -> String {
        yyyymmdd(self.earliest_unticked_day)
    }
}

pub fn analyze_schedule_at(
    schedule: &Schedule,
    instant: DateTime<Utc>,
) -> Result<ScheduleAnalysis, ScheduleError> {
    schedule.analyze_at(instant)
}

pub trait ScheduleProcessor<T: ScheduleTrait> {
    type Error: StdError + 'static;

    fn start_processing(&mut self) {}

    fn process_schedule(
        &mut self,
        schedule_id: &str,
        schedule: &T,
        last_ticked_day: NaiveDate,
    ) -> Result<(), Self::Error>;

    fn finish_processing(&mut self) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleCheck {
    pub schedule_id: String,
    pub last_ticked_day: NaiveDate,
}

#[derive(Debug)]
pub struct Scheduler<T, P>
where
    T: ScheduleTrait,
    P: ScheduleProcessor<T>,
{
    schedules: HashMap<String, T>,
    processor: P,
}

impl<T, P> Scheduler<T, P>
where
    T: ScheduleTrait,
    P: ScheduleProcessor<T>,
{
    pub fn new(processor: P) -> Self {
        Self {
            schedules: HashMap::new(),
            processor,
        }
    }

    pub fn add_schedule(&mut self, schedule_id: impl Into<String>, schedule: T) -> Option<T> {
        self.schedules.insert(schedule_id.into(), schedule)
    }

    pub fn update_schedule(&mut self, schedule_id: impl Into<String>, schedule: T) -> Option<T> {
        self.schedules.insert(schedule_id.into(), schedule)
    }

    pub fn remove_schedule(&mut self, schedule_id: &str) -> Option<T> {
        self.schedules.remove(schedule_id)
    }

    pub fn schedule(&self, schedule_id: &str) -> Option<&T> {
        self.schedules.get(schedule_id)
    }

    pub fn processor(&self) -> &P {
        &self.processor
    }

    pub fn processor_mut(&mut self) -> &mut P {
        &mut self.processor
    }

    pub fn recheck_all(
        &mut self,
        instant: DateTime<Utc>,
    ) -> Result<Vec<ScheduleCheck>, SchedulerError<P::Error>> {
        let (schedules, processor) = (&self.schedules, &mut self.processor);
        let mut schedule_ids = schedules.keys().cloned().collect::<Vec<_>>();
        let mut checks = Vec::with_capacity(schedule_ids.len());
        schedule_ids.sort();

        processor.start_processing();
        for schedule_id in schedule_ids {
            let schedule = schedules
                .get(schedule_id.as_str())
                .expect("schedule id came from the scheduler map");
            let analysis = ScheduleTrait::analyze_at(schedule, instant).map_err(|source| {
                SchedulerError::InvalidSchedule {
                    schedule_id: schedule_id.clone(),
                    source,
                }
            })?;
            let last_ticked_day = analysis.last_ticked_day;

            processor
                .process_schedule(&schedule_id, schedule, last_ticked_day)
                .map_err(|source| SchedulerError::Processor {
                    schedule_id: schedule_id.clone(),
                    source,
                })?;

            checks.push(ScheduleCheck {
                schedule_id,
                last_ticked_day,
            });
        }

        processor.finish_processing();
        Ok(checks)
    }
}

pub fn yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

#[derive(Debug, Error)]
pub enum SchedulerError<E>
where
    E: StdError + 'static,
{
    #[error("schedule `{schedule_id}` could not be analyzed: {source}")]
    InvalidSchedule {
        schedule_id: String,
        source: ScheduleError,
    },
    #[error("schedule processor failed for `{schedule_id}`: {source}")]
    Processor { schedule_id: String, source: E },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("timezone `{0}` is not a valid IANA timezone")]
    InvalidTimezone(String),
    #[error("time {hour:02}:{minute:02} is invalid")]
    InvalidTime { hour: u32, minute: u32 },
    #[error("weekday `{0}` is invalid; expected values are 0 (Sunday) through 6 (Saturday)")]
    InvalidWeekday(u8),
    #[error("schedule must contain at least one weekday")]
    EmptyWeekdays,
    #[error(
        "could not resolve local schedule time {hour:02}:{minute:02} on {date} in timezone `{timezone}`"
    )]
    UnresolvableLocalTime {
        timezone: String,
        date: NaiveDate,
        hour: u32,
        minute: u32,
    },
    #[error("date arithmetic overflowed while analyzing the schedule")]
    DateOverflow,
}

#[derive(Debug, Clone)]
struct PreparedSchedule {
    timezone: Tz,
    weekdays: [bool; 7],
    time: NaiveTime,
}

impl TryFrom<&Schedule> for PreparedSchedule {
    type Error = ScheduleError;

    fn try_from(schedule: &Schedule) -> Result<Self, Self::Error> {
        let timezone = schedule.parsed_timezone()?;
        let time = schedule.parsed_time()?;
        let mut weekdays = [false; 7];
        let mut has_weekday = false;

        for weekday in &schedule.weekdays {
            if *weekday > 6 {
                return Err(ScheduleError::InvalidWeekday(*weekday));
            }

            weekdays[*weekday as usize] = true;
            has_weekday = true;
        }

        if !has_weekday {
            return Err(ScheduleError::EmptyWeekdays);
        }

        Ok(Self {
            timezone,
            weekdays,
            time,
        })
    }
}

impl PreparedSchedule {
    fn analyze_at(&self, instant: DateTime<Utc>) -> Result<ScheduleAnalysis, ScheduleError> {
        let local_now = instant.with_timezone(&self.timezone);
        let today = local_now.date_naive();
        let today_is_active = self.is_active_day(today);
        let today_has_ticked = if today_is_active {
            instant >= self.scheduled_instant(today)?.with_timezone(&Utc)
        } else {
            false
        };

        let last_ticked_day = if today_has_ticked {
            today
        } else {
            self.previous_active_day_before(today)?
        };

        let earliest_unticked_day = if today_is_active && !today_has_ticked {
            today
        } else {
            self.next_active_day_after(today)?
        };

        Ok(ScheduleAnalysis {
            last_ticked_day,
            earliest_unticked_day,
        })
    }

    fn next_scheduled_time_at(
        &self,
        instant: DateTime<Utc>,
    ) -> Result<NextScheduledTime, ScheduleError> {
        let analysis = self.analyze_at(instant)?;
        Ok(NextScheduledTime {
            date: analysis.earliest_unticked_day,
            instant: self
                .scheduled_instant(analysis.earliest_unticked_day)?
                .with_timezone(&Utc),
        })
    }

    fn is_active_day(&self, date: NaiveDate) -> bool {
        let weekday = date.weekday().num_days_from_sunday() as usize;
        self.weekdays[weekday]
    }

    fn scheduled_instant(&self, date: NaiveDate) -> Result<DateTime<Tz>, ScheduleError> {
        let local_datetime = NaiveDateTime::new(date, self.time);

        self.resolve_local_datetime(local_datetime)
    }

    fn resolve_local_datetime(
        &self,
        local_datetime: NaiveDateTime,
    ) -> Result<DateTime<Tz>, ScheduleError> {
        match self.timezone.from_local_datetime(&local_datetime) {
            LocalResult::Single(datetime) => Ok(datetime),
            LocalResult::Ambiguous(first, second) => Ok(cmp::min(first, second)),
            LocalResult::None => {
                let mut candidate = local_datetime;

                for _ in 0..DST_SEARCH_LIMIT_MINUTES {
                    candidate += Duration::minutes(1);

                    match self.timezone.from_local_datetime(&candidate) {
                        LocalResult::Single(datetime) => return Ok(datetime),
                        LocalResult::Ambiguous(first, second) => {
                            return Ok(cmp::min(first, second));
                        }
                        LocalResult::None => {}
                    }
                }

                Err(ScheduleError::UnresolvableLocalTime {
                    timezone: self.timezone.to_string(),
                    date: local_datetime.date(),
                    hour: self.time.hour(),
                    minute: self.time.minute(),
                })
            }
        }
    }

    fn previous_active_day_before(&self, date: NaiveDate) -> Result<NaiveDate, ScheduleError> {
        for days_back in 1..=7 {
            let candidate = date
                .checked_sub_signed(Duration::days(days_back))
                .ok_or(ScheduleError::DateOverflow)?;

            if self.is_active_day(candidate) {
                return Ok(candidate);
            }
        }

        Err(ScheduleError::EmptyWeekdays)
    }

    fn next_active_day_after(&self, date: NaiveDate) -> Result<NaiveDate, ScheduleError> {
        for days_forward in 1..=7 {
            let candidate = date
                .checked_add_signed(Duration::days(days_forward))
                .ok_or(ScheduleError::DateOverflow)?;

            if self.is_active_day(candidate) {
                return Ok(candidate);
            }
        }

        Err(ScheduleError::EmptyWeekdays)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    fn schedule(timezone: &str, weekdays: &[u8], hour: u32, minute: u32) -> Schedule {
        Schedule {
            timezone: timezone.to_owned(),
            weekdays: weekdays.to_vec(),
            time: [hour, minute],
        }
    }

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .expect("expected a valid UTC timestamp")
    }

    #[test]
    fn returns_previous_business_day_before_trigger_and_today_after_trigger() {
        let schedule = schedule("America/New_York", &[1, 2, 3, 4, 5], 20, 0);

        let before_trigger = schedule
            .analyze_at(utc(2024, 5, 17, 23, 59))
            .expect("analysis should succeed");
        assert_eq!(before_trigger.last_ticked_day_yyyymmdd(), "20240516");
        assert_eq!(before_trigger.earliest_unticked_day_yyyymmdd(), "20240517");

        let after_trigger = schedule
            .analyze_at(utc(2024, 5, 18, 0, 0))
            .expect("analysis should succeed");
        assert_eq!(after_trigger.last_ticked_day_yyyymmdd(), "20240517");
        assert_eq!(after_trigger.earliest_unticked_day_yyyymmdd(), "20240520");
    }

    #[test]
    fn next_scheduled_time_returns_the_earliest_upcoming_trigger() {
        let schedule = schedule("America/New_York", &[1, 2, 3, 4, 5], 20, 0);

        let next = schedule
            .next_scheduled_time_at(utc(2024, 5, 18, 0, 0))
            .expect("next scheduled time should succeed");

        assert_eq!(yyyymmdd(next.date), "20240520");
        assert_eq!(next.instant, utc(2024, 5, 21, 0, 0));
    }

    #[test]
    fn spring_forward_gap_ticks_at_the_first_valid_minute_after_the_gap() {
        let schedule = schedule("America/New_York", &[0], 2, 30);

        let before_gap_resolution = schedule
            .analyze_at(utc(2024, 3, 10, 6, 59))
            .expect("analysis should succeed");
        assert_eq!(before_gap_resolution.last_ticked_day_yyyymmdd(), "20240303");
        assert_eq!(
            before_gap_resolution.earliest_unticked_day_yyyymmdd(),
            "20240310"
        );

        let after_gap_resolution = schedule
            .analyze_at(utc(2024, 3, 10, 7, 0))
            .expect("analysis should succeed");
        assert_eq!(after_gap_resolution.last_ticked_day_yyyymmdd(), "20240310");
        assert_eq!(
            after_gap_resolution.earliest_unticked_day_yyyymmdd(),
            "20240317"
        );
    }

    #[test]
    fn fall_back_uses_the_first_ambiguous_occurrence() {
        let schedule = schedule("America/New_York", &[0], 1, 30);

        let before_first_occurrence = schedule
            .analyze_at(utc(2024, 11, 3, 5, 29))
            .expect("analysis should succeed");
        assert_eq!(
            before_first_occurrence.last_ticked_day_yyyymmdd(),
            "20241027"
        );
        assert_eq!(
            before_first_occurrence.earliest_unticked_day_yyyymmdd(),
            "20241103"
        );

        let during_second_clock_hour = schedule
            .analyze_at(utc(2024, 11, 3, 6, 15))
            .expect("analysis should succeed");
        assert_eq!(
            during_second_clock_hour.last_ticked_day_yyyymmdd(),
            "20241103"
        );
        assert_eq!(
            during_second_clock_hour.earliest_unticked_day_yyyymmdd(),
            "20241110"
        );
    }

    #[test]
    fn london_spring_forward_gap_is_resolved_using_local_timezone_rules() {
        let schedule = schedule("Europe/London", &[0], 1, 30);

        let before_gap_resolution = schedule
            .analyze_at(utc(2024, 3, 31, 0, 59))
            .expect("analysis should succeed");
        assert_eq!(before_gap_resolution.last_ticked_day_yyyymmdd(), "20240324");
        assert_eq!(
            before_gap_resolution.earliest_unticked_day_yyyymmdd(),
            "20240331"
        );

        let after_gap_resolution = schedule
            .analyze_at(utc(2024, 3, 31, 1, 0))
            .expect("analysis should succeed");
        assert_eq!(after_gap_resolution.last_ticked_day_yyyymmdd(), "20240331");
        assert_eq!(
            after_gap_resolution.earliest_unticked_day_yyyymmdd(),
            "20240407"
        );
    }

    #[derive(Debug, Clone)]
    struct ExtendedSchedule {
        timezone: String,
        weekdays: Vec<u8>,
        time: [u32; 2],
        label: String,
    }

    impl ScheduleTrait for ExtendedSchedule {
        fn timezone(&self) -> &str {
            &self.timezone
        }

        fn weekdays(&self) -> &[u8] {
            &self.weekdays
        }

        fn time(&self) -> [u32; 2] {
            self.time
        }
    }

    #[derive(Debug, Default)]
    struct RecordingProcessor {
        entries: Vec<(String, String, NaiveDate)>,
    }

    impl ScheduleProcessor<ExtendedSchedule> for RecordingProcessor {
        type Error = Infallible;

        fn process_schedule(
            &mut self,
            schedule_id: &str,
            schedule: &ExtendedSchedule,
            last_ticked_day: NaiveDate,
        ) -> Result<(), Self::Error> {
            self.entries.push((
                schedule_id.to_owned(),
                schedule.label.clone(),
                last_ticked_day,
            ));
            Ok(())
        }
    }

    #[test]
    fn scheduler_feeds_processor_with_extended_schedule_data() {
        let mut scheduler = Scheduler::new(RecordingProcessor::default());
        scheduler.add_schedule(
            "payroll",
            ExtendedSchedule {
                timezone: "America/New_York".to_owned(),
                weekdays: vec![1, 2, 3, 4, 5],
                time: [20, 0],
                label: "Payroll run".to_owned(),
            },
        );

        let checks = scheduler
            .recheck_all(utc(2024, 5, 18, 0, 0))
            .expect("recheck should succeed");

        assert_eq!(
            checks,
            vec![ScheduleCheck {
                schedule_id: "payroll".to_owned(),
                last_ticked_day: NaiveDate::from_ymd_opt(2024, 5, 17).expect("expected valid date"),
            }]
        );
        assert_eq!(
            scheduler.processor().entries,
            vec![(
                "payroll".to_owned(),
                "Payroll run".to_owned(),
                NaiveDate::from_ymd_opt(2024, 5, 17).expect("expected valid date"),
            )]
        );
    }

    #[test]
    fn scheduler_can_update_and_remove_schedules() {
        let mut scheduler = Scheduler::new(RecordingProcessor::default());
        scheduler.add_schedule(
            "daily-report",
            ExtendedSchedule {
                timezone: "UTC".to_owned(),
                weekdays: vec![1],
                time: [9, 0],
                label: "Old report".to_owned(),
            },
        );
        scheduler.update_schedule(
            "daily-report",
            ExtendedSchedule {
                timezone: "UTC".to_owned(),
                weekdays: vec![5],
                time: [9, 0],
                label: "Friday report".to_owned(),
            },
        );
        scheduler.add_schedule(
            "obsolete",
            ExtendedSchedule {
                timezone: "UTC".to_owned(),
                weekdays: vec![5],
                time: [9, 0],
                label: "Obsolete".to_owned(),
            },
        );

        let removed = scheduler.remove_schedule("obsolete");

        assert!(removed.is_some());
        assert!(scheduler.schedule("obsolete").is_none());

        let checks = scheduler
            .recheck_all(utc(2024, 5, 17, 10, 0))
            .expect("recheck should succeed");

        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].schedule_id, "daily-report");
        assert_eq!(
            checks[0].last_ticked_day,
            NaiveDate::from_ymd_opt(2024, 5, 17).expect("expected valid date")
        );
        assert_eq!(scheduler.processor().entries[0].1, "Friday report");
    }
}
