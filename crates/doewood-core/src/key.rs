//! The master key that encrypts the instance's secrets.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use zeroize::Zeroizing;

use crate::Error;

/// Length of the master key in bytes (XChaCha20-Poly1305 uses a 256-bit key).
pub const MASTER_KEY_LEN: usize = 32;

/// A 256-bit master key. It is wiped from memory on drop, and its `Debug` never shows it.
#[derive(Clone)]
pub struct MasterKey(Zeroizing<[u8; MASTER_KEY_LEN]>);

impl MasterKey {
    /// Wraps raw key bytes.
    pub fn from_bytes(bytes: [u8; MASTER_KEY_LEN]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Parses standard base64 (padding optional, surrounding whitespace ignored) of exactly
    /// 32 bytes.
    pub fn from_base64(text: &str) -> Result<Self, Error> {
        let text = text.trim().trim_end_matches('=');
        let decoded = Zeroizing::new(
            base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(text)
                .map_err(|_| Error::InvalidMasterKey("not valid base64"))?,
        );
        let bytes: [u8; MASTER_KEY_LEN] = decoded
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidMasterKey("must decode to exactly 32 bytes"))?;
        Ok(Self::from_bytes(bytes))
    }

    /// The key as padded standard base64, the format of `DOEWOOD_MASTER_KEY` and of the key file.
    pub fn to_base64(&self) -> Zeroizing<String> {
        Zeroizing::new(STANDARD.encode(self.0.as_slice()))
    }

    /// The raw key bytes.
    pub fn as_bytes(&self) -> &[u8; MASTER_KEY_LEN] {
        &self.0
    }
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MasterKey(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trip() {
        let key = MasterKey::from_bytes([7; 32]);
        let text = key.to_base64();
        assert_eq!(MasterKey::from_base64(&text).unwrap().as_bytes(), &[7; 32]);
        // Without padding, and with whitespace around it.
        let unpadded = format!("  {}\n", text.trim_end_matches('='));
        assert_eq!(
            MasterKey::from_base64(&unpadded).unwrap().as_bytes(),
            &[7; 32]
        );
    }

    #[test]
    fn wrong_length_and_garbage_are_refused() {
        assert!(MasterKey::from_base64(&STANDARD.encode([1u8; 16])).is_err());
        assert!(MasterKey::from_base64(&STANDARD.encode([1u8; 33])).is_err());
        assert!(MasterKey::from_base64("not base64 at all!").is_err());
        assert!(MasterKey::from_base64("").is_err());
    }

    #[test]
    fn debug_is_redacted() {
        let key = MasterKey::from_bytes([0xAB; 32]);
        let shown = format!("{key:?}");
        assert_eq!(shown, "MasterKey(<redacted>)");
    }
}
