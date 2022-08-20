use std::time::Duration;

use rand::Rng;

pub struct RandomizedExponentialBackoff {
    max_delay: Duration,
    delay: Duration,
    random_factor: f64,
}

const DEFAULT_MIN_DELAY: Duration = Duration::from_millis(50);
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(10);
const DEFAULT_RANDOM_FACTOR: f64 = 0.2;

impl Default for RandomizedExponentialBackoff {
    fn default() -> Self {
        Self::new(DEFAULT_MIN_DELAY, DEFAULT_MAX_DELAY, DEFAULT_RANDOM_FACTOR)
    }
}
impl RandomizedExponentialBackoff {
    pub fn new(min_delay: Duration, max_delay: Duration, random_factor: f64) -> Self {
        Self {
            max_delay,
            delay: min_delay,
            random_factor,
        }
    }

    pub fn next(&mut self) -> Duration {
        let mut rng = rand::thread_rng();
        let random_delay = (self.delay.as_millis() as f64 * self.random_factor) as i64;
        let dur = Duration::from_millis(
            (self.delay.as_millis() as i64 + rng.gen_range(-random_delay..=random_delay)) as u64,
        );

        self.delay = self.delay * 3 / 2;
        if self.delay > self.max_delay {
            self.delay = self.max_delay;
        }

        dur
    }

    pub async fn wait(&mut self) {
        tokio::time::sleep(self.next()).await;
    }
}
