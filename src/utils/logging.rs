use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

pub fn init_logging(log_level: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info")); // Fallback if config is malformed

    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("Logging initialized with log level {}", log_level);
}
