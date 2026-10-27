//! Portable coordination primitives and transactional storage boundary.
pub mod lease;
pub mod session;

/// Provisional synchronous, single-authority transaction view. Implementations
/// must roll back all writes when the callback returns an error. No awaits here.
pub trait Transaction {
    fn get(&self, key: &str) -> Result<Option<String>, String>;
    fn put(&mut self, key: &str, value: &str) -> Result<(), String>;
}

pub fn counter(value: &str) -> Result<u64, String> {
    let number = value.parse::<u64>().map_err(|_| "invalid counter")?;
    if number.to_string() != value {
        return Err("non-canonical counter".into());
    }
    Ok(number)
}

#[cfg(test)]
mod tests {
    use super::counter;
    #[test]
    fn counters_are_canonical_and_lossless() {
        for n in [0, (1 << 53) - 1, 1 << 53, (1 << 53) + 1, u64::MAX] {
            assert_eq!(counter(&n.to_string()).unwrap(), n);
        }
        for value in [
            "",
            "00",
            "01",
            "+1",
            "-1",
            "1.0",
            "1e3",
            " 1",
            "1 ",
            "١",
            "18446744073709551616",
        ] {
            assert!(counter(value).is_err(), "{value}");
        }
    }
}
