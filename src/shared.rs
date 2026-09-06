use atomicbox::AtomicOptionBox;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::LoggedEvent;

#[repr(transparent)]
#[derive(Debug, Default)]
pub struct FetchedSubchannelFlags(u8);

impl FetchedSubchannelFlags {
    #[allow(dead_code)]
    pub fn connected(&self) -> bool {
        (self.0 & 1) != 0
    }

    pub fn healthy(&self) -> Option<bool> {
        match self.0 & 6 {
            0b000 => Some(false),
            0b010 => None,
            0b110 => Some(true),
            _ => {
                panic!("Corrupted health flag");
            }
        }
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct SubchannelFlags(AtomicU8);

impl SubchannelFlags {
    pub fn set_connected(&self) {
        self.0.fetch_or(1, Ordering::Release);
    }

    pub fn set_healthy(&self) {
        self.0.fetch_or(6, Ordering::Release);
    }

    pub fn set_unhealthy(&self) {
        self.0.fetch_and(!6, Ordering::Release);
    }

    pub fn get(&self) -> FetchedSubchannelFlags {
        FetchedSubchannelFlags(self.0.load(Ordering::Acquire))
    }
}

impl Default for SubchannelFlags {
    fn default() -> Self {
        Self(2.into())
    }
}

impl crate::report::Clear for SubchannelFlags {
    fn clear(&self) {
        self.0.store(2, Ordering::Release);
    }
}

clearable! {
    #[derive(Debug, Default)]
    pub struct SubchannelShared {
        pub flags: SubchannelFlags,
        pub error: AtomicOptionBox<LoggedEvent<String>>,
    }
}
