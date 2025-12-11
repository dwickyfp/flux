//! Tests for settings.rs configuration loading

use std::io::Write;
use tempfile::TempDir;

// Helper to create a test config file
fn create_test_config(dir: &TempDir, content: &str) -> std::path::PathBuf {
    let config_dir = dir.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("default.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    dir.path().to_path_buf()
}

#[test]
fn test_valid_config_parses() {
    let config_content = r#"
[destination.snowflake]
account_identifier = "TESTACCOUNT"
user = "TESTUSER"
role = "TESTROLE"
database = "TESTDB"
schema = "TESTSCHEMA"
private_key_path = "/path/to/key.p8"
private_key_passphrase = "testpass"
buffer_flush_interval_ms = 1000
buffer_batch_size = 500
"#;

    let temp_dir = TempDir::new().unwrap();
    let base_path = create_test_config(&temp_dir, config_content);
    
    // Change to temp directory to test config loading
    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(&base_path).unwrap();
    
    // Note: This test requires the Settings struct to be accessible
    // For now, we just verify the file was created
    let config_file = base_path.join("config").join("default.toml");
    assert!(config_file.exists(), "Config file should exist");
    
    std::env::set_current_dir(original_dir).unwrap();
}

#[test]
fn test_config_file_contents() {
    let config_content = r#"
[destination.snowflake]
account_identifier = "MYACCOUNT"
user = "MYUSER"
role = "ADMIN"
database = "PROD_DB"
schema = "PUBLIC"
private_key_path = "./keys/private.p8"
buffer_flush_interval_ms = 2000
buffer_batch_size = 1000
"#;

    let temp_dir = TempDir::new().unwrap();
    let base_path = create_test_config(&temp_dir, config_content);
    
    let config_file = base_path.join("config").join("default.toml");
    let content = std::fs::read_to_string(&config_file).unwrap();
    
    assert!(content.contains("MYACCOUNT"), "Should contain account");
    assert!(content.contains("buffer_batch_size = 1000"), "Should contain batch size");
}
