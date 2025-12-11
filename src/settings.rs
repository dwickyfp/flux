use config::{Config, ConfigError, File, Environment};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub destination: DestinationConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationConfig {
    pub snowflake: SnowflakeConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct SnowflakeConfig {
    pub account_identifier: String,
    pub user: String,
    pub role: String,
    pub database: String,
    pub schema: String,
    pub warehouse: String,
    pub private_key_path: String,
    pub private_key_passphrase: Option<String>,
    pub buffer_flush_interval_ms: u64,
    pub buffer_batch_size: usize,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::with_name("config/default").required(false))
            // Add in the current environment file
            // Default to 'development' env
            // Note that this file is _optional_
            .add_source(File::with_name(&format!("config/{}", run_mode)).required(false))
            // Add in a local configuration file
            // This file shouldn't be checked in to git
            // .add_source(File::with_name("config/local").required(false))
            // Add in settings from the environment (with a prefix of APP)
            // Eg.. `APP_DEBUG=1` would set `debug` key
            .add_source(Environment::with_prefix("APP").separator("__"))
            .add_source(File::with_name("config").required(false))
            .build()?;

        // You can deserialize (and thus freeze) the entire configuration as
        // deserialized from sources 
        s.try_deserialize()
    }
}
