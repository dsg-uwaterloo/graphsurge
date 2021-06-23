use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::ops::{Add, AddAssign, Deref};
use std::time::Duration;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct GsTimer {
    instant: Instant,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct GsDuration {
    duration: Duration,
}

impl GsTimer {
    pub fn now() -> Self {
        Self { instant: Instant::now() }
    }

    pub fn elapsed(&self) -> GsDuration {
        GsDuration { duration: self.instant.elapsed() }
    }
}

impl GsDuration {
    pub fn millis_string(&self) -> String {
        const MICRO_PER_MILLI: u128 = 1_000;
        format!(
            "{}.{:03} ms",
            self.duration.as_micros() / MICRO_PER_MILLI,
            self.duration.as_micros() % MICRO_PER_MILLI
        )
    }

    pub fn seconds_string(&self) -> String {
        format!("{}.{:06} s", self.duration.as_secs(), self.duration.subsec_micros())
    }
}

impl Debug for GsDuration {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{:?}", self.duration)
    }
}

impl Deref for GsDuration {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.duration
    }
}

impl Add for GsDuration {
    type Output = GsDuration;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.duration += rhs.duration;
        self
    }
}

impl AddAssign for GsDuration {
    fn add_assign(&mut self, rhs: Self) {
        self.duration += rhs.duration;
    }
}

#[cfg(test)]
mod tests {
    use crate::util::timer::GsDuration;
    use std::time::Duration;

    #[test]
    fn string_format() {
        let inputs = vec![
            (0, 7_106_780, "0.007106 s", "7.106 ms"),
            (152, 628_093_000, "152.628093 s", "152628.093 ms"),
        ];
        for (sec, nano, sec_str, milli_str) in inputs {
            let duration = GsDuration { duration: Duration::new(sec, nano) };
            assert_eq!(duration.seconds_string(), sec_str);
            assert_eq!(duration.millis_string(), milli_str);
        }
    }
}
