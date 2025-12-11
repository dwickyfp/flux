// Integration test for Snowflake connection using config from default.toml

// For this test, I will assume we can access the modules. If not, I'll copy the minimal structs needed.
// Actually, it's better to act as an integration test using the public API if possible, or submodule.
// Since we are in `tests/`, we can access `flux` (the crate name) public items.

use flux::auth::KeyPairAuth;
use flux::snowflake::client::SnowflakeClient;
use flux::settings::Settings;
// use std::env;

use tokio;

#[tokio::test]
async fn test_snowflake_connection_e2e() {
    // Load settings from config/default.toml
    let settings = match Settings::new() {
        Ok(s) => s,
        Err(e) => {
            println!("Skipping test: Failed to load settings - {}", e);
            return;
        }
    };

    let snowflake_config = settings.destination.snowflake;
    
    // Check if key exists
    if !std::path::Path::new(&snowflake_config.private_key_path).exists() {
        println!("Skipping test: Private key not found at {}", snowflake_config.private_key_path);
        return;
    }

    let auth = KeyPairAuth::new(
        snowflake_config.account_identifier.clone(),
        snowflake_config.user.clone(),
        &snowflake_config.private_key_path,
        snowflake_config.private_key_passphrase.as_deref(),
    ).expect("Failed to create auth");

    let client = SnowflakeClient::new(snowflake_config, auth);

    // Test connection by ensuring a table exists
    let result = client.ensure_table_exists("E2E_TEST_TABLE").await;
    
    match result {
        Ok(_) => println!("Connection successful! Table ensured."),
        Err(e) => panic!("Connection failed: {}", e),
    }

    // Cleanup: Drop the test table
    let drop_result = client.drop_table("E2E_TEST_TABLE").await;
    match drop_result {
        Ok(_) => println!("Cleanup successful! Test table dropped."),
        Err(e) => eprintln!("Warning: Failed to drop test table: {}", e),
    }
}
