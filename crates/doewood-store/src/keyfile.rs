//! The development key file: `<data>/master.key`, used when `DOEWOOD_MASTER_KEY` is unset.
//!
//! It holds the key as one line of base64. It is created with mode 0600 on Unix and never
//! overwritten: a key file that does not parse is an error, because replacing it would make
//! every stored secret unreadable.

use std::fs::OpenOptions;
use std::io::{ErrorKind, Write as _};
use std::path::Path;

use chacha20poly1305::aead::OsRng;
use chacha20poly1305::aead::rand_core::RngCore as _;
use doewood_core::MasterKey;
use doewood_core::key::MASTER_KEY_LEN;
use zeroize::Zeroizing;

use crate::StoreError;

/// What [`load_or_generate`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyFileState {
    /// A new key was generated and written.
    Generated,
    /// An existing key file was read.
    Loaded {
        /// On Unix: the file is readable or writable by group or others.
        loose_permissions: bool,
    },
}

/// A fresh random master key from the operating system's generator.
pub fn generate_key() -> MasterKey {
    let mut bytes = Zeroizing::new([0u8; MASTER_KEY_LEN]);
    OsRng.fill_bytes(bytes.as_mut_slice());
    MasterKey::from_bytes(*bytes)
}

/// Reads the key file, or creates it with a new random key (mode 0600 on Unix) when it does
/// not exist.
pub fn load_or_generate(path: &Path) -> Result<(MasterKey, KeyFileState), StoreError> {
    let io = |source| StoreError::KeyFile {
        path: path.to_owned(),
        source,
    };
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    match options.open(path) {
        Ok(mut file) => {
            let key = generate_key();
            let text = key.to_base64();
            file.write_all(text.as_bytes()).map_err(io)?;
            file.write_all(b"\n").map_err(io)?;
            file.sync_all().map_err(io)?;
            Ok((key, KeyFileState::Generated))
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            let text = Zeroizing::new(std::fs::read_to_string(path).map_err(io)?);
            let key = MasterKey::from_base64(&text).map_err(|e| StoreError::BadKeyFile {
                path: path.to_owned(),
                reason: e.to_string(),
            })?;
            Ok((
                key,
                KeyFileState::Loaded {
                    loose_permissions: loose_permissions(path),
                },
            ))
        }
        Err(e) => Err(io(e)),
    }
}

#[cfg(unix)]
fn loose_permissions(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o077 != 0)
}

#[cfg(not(unix))]
fn loose_permissions(_path: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_once_then_loads_the_same_key() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("master.key");
        let (k1, s1) = load_or_generate(&path).unwrap();
        assert_eq!(s1, KeyFileState::Generated);
        let (k2, s2) = load_or_generate(&path).unwrap();
        assert_eq!(
            s2,
            KeyFileState::Loaded {
                loose_permissions: false
            }
        );
        assert_eq!(k1.as_bytes(), k2.as_bytes());
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            MasterKey::from_base64(&text).unwrap().as_bytes(),
            k1.as_bytes()
        );
    }

    #[test]
    fn keys_are_random() {
        assert_ne!(generate_key().as_bytes(), generate_key().as_bytes());
    }

    #[cfg(unix)]
    #[test]
    fn file_mode_is_0600_and_loose_modes_are_reported() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("master.key");
        load_or_generate(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let (_, state) = load_or_generate(&path).unwrap();
        assert_eq!(
            state,
            KeyFileState::Loaded {
                loose_permissions: true
            }
        );
    }

    #[test]
    fn a_corrupt_key_file_is_never_overwritten() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("master.key");
        std::fs::write(&path, "garbage").unwrap();
        assert!(matches!(
            load_or_generate(&path),
            Err(StoreError::BadKeyFile { .. })
        ));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "garbage");
    }
}
