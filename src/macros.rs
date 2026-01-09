#[macro_export]
macro_rules! lazy_debug {
    ($($arg:tt)+) => {
        if ::tracing::enabled!(::tracing::Level::DEBUG) {
            ::tracing::debug!($($arg)+);
        }
    };
}

#[macro_export]
macro_rules! lazy_trace {
    ($($arg:tt)+) => {
        if ::tracing::enabled!(::tracing::Level::TRACE) {
            ::tracing::trace!($($arg)+);
        }
    };
}
