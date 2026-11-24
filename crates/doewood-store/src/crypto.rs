//! Sealing secrets: XChaCha20-Poly1305 with a random 192-bit nonce per write. The secret's
//! name is bound as associated data, so a ciphertext copied into another row does not open.

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use doewood_core::MasterKey;
use zeroize::Zeroizing;

use crate::StoreError;

/// Length of an XChaCha20 nonce.
pub(crate) const NONCE_LEN: usize = 24;

const AAD_PREFIX: &[u8] = b"doewood.secret.v1\0";

pub(crate) struct Cipher(XChaCha20Poly1305);

impl Cipher {
    pub(crate) fn new(key: &MasterKey) -> Self {
        Self(XChaCha20Poly1305::new(key.as_bytes().into()))
    }

    pub(crate) fn seal(
        &self,
        name: &str,
        plaintext: &[u8],
    ) -> Result<([u8; NONCE_LEN], Vec<u8>), StoreError> {
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aad = aad(name);
        let ciphertext = self
            .0
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| StoreError::Random)?;
        Ok((nonce.into(), ciphertext))
    }

    pub(crate) fn open(
        &self,
        name: &str,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, StoreError> {
        let fail = || StoreError::Decrypt(name.to_owned());
        let nonce: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| fail())?;
        let aad = aad(name);
        self.0
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: ciphertext,
                    aad: &aad,
                },
            )
            .map(Zeroizing::new)
            .map_err(|_| fail())
    }
}

fn aad(name: &str) -> Vec<u8> {
    [AAD_PREFIX, name.as_bytes()].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher(b: u8) -> Cipher {
        Cipher::new(&MasterKey::from_bytes([b; 32]))
    }

    #[test]
    fn round_trip() {
        let c = cipher(1);
        let (nonce, ct) = c.seal("s3/main", b"AKIA-secret").unwrap();
        assert_ne!(ct.as_slice(), b"AKIA-secret".as_slice());
        assert_eq!(
            c.open("s3/main", &nonce, &ct).unwrap().as_slice(),
            b"AKIA-secret"
        );
    }

    #[test]
    fn nonces_are_fresh() {
        let c = cipher(1);
        let (n1, c1) = c.seal("a", b"same").unwrap();
        let (n2, c2) = c.seal("a", b"same").unwrap();
        assert_ne!(n1, n2);
        assert_ne!(c1, c2);
    }

    #[test]
    fn tampering_is_detected() {
        let c = cipher(1);
        let (nonce, ct) = c.seal("name", b"value").unwrap();
        for i in 0..ct.len() {
            let mut bad = ct.clone();
            bad[i] ^= 0x01;
            assert!(c.open("name", &nonce, &bad).is_err(), "byte {i}");
        }
        let mut bad_nonce = nonce;
        bad_nonce[0] ^= 0x80;
        assert!(c.open("name", &bad_nonce, &ct).is_err());
        assert!(c.open("name", &nonce[..23], &ct).is_err());
        assert!(c.open("name", &nonce, &ct[..ct.len() - 1]).is_err());
        // Bound to the name: moving the row does not work.
        assert!(c.open("other", &nonce, &ct).is_err());
        // And to the key.
        assert!(cipher(2).open("name", &nonce, &ct).is_err());
    }
}
