use std::collections::VecDeque;
use std::time::Duration;

pub trait Timestamped {
    fn timestamp_ms(&self) -> u64;
}

#[derive(Debug, Clone)]
pub struct RollingWindow<T>
where
    T: Timestamped,
{
    retention: Duration,
    samples: VecDeque<T>,
}

impl<T> RollingWindow<T>
where
    T: Timestamped,
{
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            samples: VecDeque::new(),
        }
    }

    pub fn push(&mut self, sample: T) {
        let ts = sample.timestamp_ms();

        self.samples.push_back(sample);

        let cutoff = ts.saturating_sub(self.retention.as_millis() as u64);

        while let Some(front) = self.samples.front() {
            if front.timestamp_ms() < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn clear(&mut self) {
        self.samples.clear();
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn latest(&self) -> Option<&T> {
        self.samples.back()
    }

    pub fn oldest(&self) -> Option<&T> {
        self.samples.front()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.samples.iter()
    }

    pub fn retain_recent(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(self.retention.as_millis() as u64);

        while let Some(front) = self.samples.front() {
            if front.timestamp_ms() < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn samples(&self) -> &VecDeque<T> {
        &self.samples
    }

    pub fn retention(&self) -> Duration {
        self.retention
    }
}
