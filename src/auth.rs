use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rsa::pkcs8::DecodePrivateKey;
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

/// Handles Snowflake Key-Pair Authentication
#[derive(Clone)]
pub struct KeyPairAuth {
    account_identifier: String,
    user: String,
    private_key: RsaPrivateKey,
    public_key_fingerprint: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

impl KeyPairAuth {
    pub fn new(
        account_identifier: String,
        user: String,
        private_key_path: &str,
        passphrase: Option<&str>,
    ) -> Result<Self> {
        let key_content = fs::read_to_string(private_key_path)
            .with_context(|| format!("Failed to read private key at {}", private_key_path))?;

        // Decrypt if encrypted, otherwise load directly
        let private_key = if let Some(pass) = passphrase {
            RsaPrivateKey::from_pkcs8_encrypted_pem(&key_content, pass)
                .context("Failed to decrypt private key")?
        } else {
            RsaPrivateKey::from_pkcs8_pem(&key_content)
                .context("Failed to decode private key")?
        };

        // Calculate public key fingerprint (SHA256 of DER-encoded public key)
        // Calculate public key fingerprint (SHA256 of Subject Public Key Info - SPKI)
        use rsa::pkcs8::EncodePublicKey;
        let public_key = private_key.to_public_key();
        let public_key_der = public_key.to_public_key_der()
            .context("Failed to encode public key to DER")?;
        
        let mut hasher = Sha256::new();
        hasher.update(public_key_der.as_bytes());
        let hash = hasher.finalize();
        let fingerprint = BASE64.encode(hash);
        let public_key_fingerprint = format!("SHA256:{}", fingerprint);

        // Format account: uppercase, underscores to hyphens
        let formatted_account = account_identifier.to_uppercase().replace('_', "-");
        let formatted_user = user.to_uppercase();

        println!("=== Snowflake Auth ===");
        println!("Account: {}", formatted_account);
        println!("User: {}", formatted_user);
        println!("Fingerprint: {}", public_key_fingerprint);
        println!("======================");

        Ok(Self {
            account_identifier: formatted_account,
            user: formatted_user,
            private_key,
            public_key_fingerprint,
        })
    }

    pub fn generate_token(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs();

        let exp = now + 3540; // 59 minutes

        // Snowflake JWT format:
        // iss = ACCOUNT.USER.SHA256:FINGERPRINT
        // sub = ACCOUNT.USER
        let sub = format!("{}.{}", self.account_identifier, self.user);
        let iss = format!("{}.{}", sub, self.public_key_fingerprint);
        
        let claims = Claims {
            iss,
            sub,
            iat: now,
            exp,
        };

        // Convert to PKCS1 PEM for jsonwebtoken
        let pem = self.private_key.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)?;
        let encoding_key = EncodingKey::from_rsa_pem(pem.as_bytes())?;

        let token = encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &encoding_key,
        )?;

        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;
    use rand::rngs::OsRng;

    fn generate_test_key_pem() -> String {
        use rsa::pkcs8::EncodePrivateKey;
        let key = RsaPrivateKey::new(&mut OsRng, 2048).expect("Failed to generate key");
        key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap().to_string()
    }

    #[test]
    fn test_auth_creation() {
        let pem = generate_test_key_pem();
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(pem.as_bytes()).unwrap();
        
        let auth = KeyPairAuth::new(
            "TEST_ACCOUNT".to_string(),
            "TEST_USER".to_string(),
            temp_file.path().to_str().unwrap(),
            None,
        );
        assert!(auth.is_ok(), "Should create auth successfully");
    }

    #[test]
    fn test_jwt_generation() {
        let pem = generate_test_key_pem();
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(pem.as_bytes()).unwrap();
        
        let auth = KeyPairAuth::new(
            "TEST_ACCOUNT".to_string(),
            "TEST_USER".to_string(),
            temp_file.path().to_str().unwrap(),
            None,
        ).unwrap();
        
        let token = auth.generate_token();
        assert!(token.is_ok(), "Should generate token");
        let token_str = token.unwrap();
        let parts: Vec<&str> = token_str.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 parts");
    }
}
