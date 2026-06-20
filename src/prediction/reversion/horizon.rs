use serde::{Deserialize, Serialize};

/// A prediction time horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Horizon {
    S5,   //  5 seconds
    S10,  // 10 seconds
    S30,  // 30 seconds
    M1,   //  1 minute
    M5,   //  5 minutes
}

impl Horizon {
    pub fn seconds(self) -> f64 {
        match self {
            Self::S5  => 5.0,
            Self::S10 => 10.0,
            Self::S30 => 30.0,
            Self::M1  => 60.0,
            Self::M5  => 300.0,
        }
    }

    pub fn all() -> &'static [Horizon] {
        &[Horizon::S5, Horizon::S10, Horizon::S30, Horizon::M1, Horizon::M5]
    }
}

impl std::fmt::Display for Horizon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::S5  => write!(f, "5s"),
            Self::S10 => write!(f, "10s"),
            Self::S30 => write!(f, "30s"),
            Self::M1  => write!(f, "1m"),
            Self::M5  => write!(f, "5m"),
        }
    }
}
