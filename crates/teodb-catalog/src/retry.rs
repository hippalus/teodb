use std::time::Duration;

use backon::ExponentialBuilder;

/// Configuration for commit retry with exponential backoff + jitter.
#[derive(Debug, Clone, bon::Builder)]
pub struct RetryConfig {
    #[builder(default = 8)]
    pub max_retries: u32,
    #[builder(default = Duration::from_millis(50))]
    pub initial_backoff: Duration,
    #[builder(default = Duration::from_secs(5))]
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 8,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryConfig {
    pub(crate) fn backoff_builder(&self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(self.initial_backoff)
            .with_max_delay(self.max_backoff)
            .with_max_times(self.max_retries as usize)
            .with_jitter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = RetryConfig::default();
        assert_eq!(cfg.max_retries, 8);
        assert_eq!(cfg.initial_backoff, Duration::from_millis(50));
        assert_eq!(cfg.max_backoff, Duration::from_secs(5));
    }

    #[test]
    fn builder_overrides_retry_policy() {
        let cfg = RetryConfig::builder()
            .max_retries(3)
            .initial_backoff(Duration::from_millis(100))
            .max_backoff(Duration::from_secs(2))
            .build();

        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.initial_backoff, Duration::from_millis(100));
        assert_eq!(cfg.max_backoff, Duration::from_secs(2));
    }
}
