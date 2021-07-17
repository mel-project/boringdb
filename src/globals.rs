use std::sync::atomic::AtomicU64;

// Global pseudotime counter
pub(crate) static GLOBAL_PSEUDO_TIME: AtomicU64 = AtomicU64::new(0);