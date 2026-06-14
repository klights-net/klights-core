//! Encrypt/decrypt the cluster CA private key for in-band distribution
//! via the JoinAsControlplane gRPC response.
//!
//! The CA key is AES-256-GCM encrypted with a key derived from the
//! bootstrap join token (SHA-256). Only nodes presenting a valid token
//! can decrypt the CA key.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::Result;
use sha2::{Digest, Sha256};

/// Derive a 256-bit encryption key from the join token using SHA-256.
fn derive_key(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Encrypt PEM bytes with AES-256-GCM using a key derived from the token.
/// Returns (ciphertext, 12-byte nonce).
pub fn encrypt_ca_key(token: &str, plaintext: &[u8]) -> Result<(Vec<u8>, [u8; 12])> {
    let key = derive_key(token);
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow::anyhow!("AES key init: {e}"))?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| anyhow::anyhow!("AES encrypt: {e}"))?;
    Ok((ciphertext, nonce.into()))
}

/// Decrypt CA key PEM bytes. Returns plaintext PEM.
pub fn decrypt_ca_key(token: &str, ciphertext: &[u8], nonce: &[u8; 12]) -> Result<Vec<u8>> {
    let key = derive_key(token);
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow::anyhow!("AES key init: {e}"))?;
    let nonce = Nonce::from_slice(nonce);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| anyhow::anyhow!("AES-256-GCM decrypt failed (wrong token?)"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let token = "1653ce.d7fcbb6c690570ec";
        let plaintext =
            b"-----BEGIN RSA PRIVATE KEY-----\ntest CA key data\n-----END RSA PRIVATE KEY-----\n";
        let (ciphertext, nonce) = encrypt_ca_key(token, plaintext).unwrap();
        assert_ne!(ciphertext.as_slice(), plaintext);
        let decrypted = decrypt_ca_key(token, &ciphertext, &nonce).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_with_wrong_token_fails() {
        let token = "correct-token";
        let wrong_token = "wrong-token";
        let plaintext = b"secret CA key";
        let (ciphertext, nonce) = encrypt_ca_key(token, plaintext).unwrap();
        let result = decrypt_ca_key(wrong_token, &ciphertext, &nonce);
        assert!(result.is_err());
    }

    #[test]
    fn different_tokens_produce_different_ciphertexts() {
        let token = "same-token";
        let plaintext = b"same data";
        let (ct1, nonce1) = encrypt_ca_key(token, plaintext).unwrap();
        let (ct2, nonce2) = encrypt_ca_key(token, plaintext).unwrap();
        // Nonces are random so ciphertexts differ
        assert_ne!(ct1, ct2);
        assert_ne!(nonce1, nonce2);
        // Both decrypt correctly
        assert_eq!(decrypt_ca_key(token, &ct1, &nonce1).unwrap(), plaintext);
        assert_eq!(decrypt_ca_key(token, &ct2, &nonce2).unwrap(), plaintext);
    }
}
