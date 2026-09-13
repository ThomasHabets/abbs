use std::{fmt, str::FromStr};

use anyhow::{Result, bail};

/// A normalized amateur-radio callsign used as a BBS identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Callsign(String);

impl Callsign {
    pub fn parse(input: &str) -> Result<Self> {
        input.parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn to_agw_call(&self) -> Result<agw::Call> {
        self.0
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid callsign {}: {error}", self.0))
    }
}

impl FromStr for Callsign {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self> {
        let normalized = input.trim().to_ascii_uppercase();
        if normalized.is_empty() {
            bail!("callsign cannot be empty");
        }

        agw::Call::from_bytes(normalized.as_bytes())
            .map_err(|error| anyhow::anyhow!("invalid callsign: {error}"))?;

        Ok(Self(normalized))
    }
}

impl fmt::Display for Callsign {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::Callsign;

    #[test]
    fn callsigns_are_normalized_and_validated() {
        assert_eq!(Callsign::parse(" m0abc-7 ").unwrap().as_str(), "M0ABC-7");
        assert!(Callsign::parse("").is_err());
        assert!(Callsign::parse("M0 ABC").is_err());
        assert!(Callsign::parse("TOO-LONG-11").is_err());
    }
}
