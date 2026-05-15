use std::fmt;

use serde::{Deserialize, Serialize};

/// Job sequence number, displayed as J1, J2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub u32);

/// Cron sequence number, displayed as C1, C2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CronId(pub u32);

/// Chain sequence number, displayed as CH1, CH2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChainId(pub u32);

/// Script submission sequence number, displayed as R1, R2, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScriptId(pub u32);

/// Content-addressed scope hash (blake3), displayed as S0@a3f1...
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeHash(pub [u8; 32]);

/// Unified entity reference for commands like :fg, :kill, :out
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityRef {
    Job(JobId),
    Cron(CronId),
    Scope(ScopeHash),
}

// --- Display impls ---

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "J{}", self.0)
    }
}

impl fmt::Display for CronId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "C{}", self.0)
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CH{}", self.0)
    }
}

impl fmt::Display for ScriptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "R{}", self.0)
    }
}

impl fmt::Display for ScopeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Show first 4 bytes (8 hex chars) as short form
        let hex: String = self.0[..4].iter().map(|b| format!("{b:02x}")).collect();
        write!(f, "S@{hex}")
    }
}

impl fmt::Debug for ScopeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScopeHash({self})")
    }
}

impl fmt::Display for EntityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(id) => write!(f, "{id}"),
            Self::Cron(id) => write!(f, "{id}"),
            Self::Scope(hash) => write!(f, "{hash}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_ids() {
        assert_eq!(JobId(1).to_string(), "J1");
        assert_eq!(CronId(3).to_string(), "C3");
        assert_eq!(ChainId(7).to_string(), "CH7");
        assert_eq!(ScriptId(9).to_string(), "R9");
    }

    #[test]
    fn display_scope_hash() {
        let mut h = [0u8; 32];
        h[0] = 0xa3;
        h[1] = 0xf1;
        h[2] = 0x00;
        h[3] = 0xff;
        let s = ScopeHash(h);
        assert_eq!(s.to_string(), "S@a3f100ff");
    }

    #[test]
    fn entity_ref_display() {
        assert_eq!(EntityRef::Job(JobId(5)).to_string(), "J5");
    }
}
