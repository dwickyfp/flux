use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;
use std::error::Error;
use crate::management::SourceConfig;
use crate::settings::Settings;
use crate::destination::snowflake::{SnowflakeDestination, ColumnSchema};
use crate::auth::KeyPairAuth;
use tokio_postgres::NoTls;
use std::collections::HashMap;


pub async fn run_pipeline(config: SourceConfig, settings: Settings) -> Result<(), Box<dyn Error + Send + Sync>> {
     // Configure Postgres connection.
    let pg_connection_config = PgConnectionConfig {
        host: config.host.clone(),
        port: config.port,
        name: config.dbname.clone(),
        username: config.username.clone(),
        password: Some(config.password.clone().into()),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    };

    // Fetch table Name mapping from source
    println!("Fetching table mapping for publication: {}", config.publication_name);
    let conn_str = format!("host={} port={} user={} password={} dbname={}", 
        config.host, config.port, config.username, config.password, config.dbname);
    
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Source DB connection error: {}", e);
        }
    });

    let rows = client.query(
        "SELECT c.oid::TEXT, c.relname, a.attname, t.typname
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_publication_tables p ON p.tablename = c.relname AND p.schemaname = n.nspname
         JOIN pg_attribute a ON a.attrelid = c.oid
         JOIN pg_type t ON t.oid = a.atttypid
         WHERE p.pubname = $1 AND a.attnum > 0 AND NOT a.attisdropped
         ORDER BY c.oid, a.attnum", 
        &[&config.publication_name]
    ).await?;

    let mut table_mapping = HashMap::new();
    let mut schema_mapping: HashMap<String, Vec<ColumnSchema>> = HashMap::new();

    for row in rows {
        let oid: String = row.get(0);
        let name: String = row.get(1);
        let col_name: String = row.get(2);
        let col_type: String = row.get(3);

        table_mapping.insert(oid.clone(), name.clone());
        schema_mapping.entry(oid).or_default().push(ColumnSchema {
            name: col_name,
            pg_type: col_type,
        });
    }
    
    println!("Mapped {} tables with schema information.", table_mapping.len());


    let pipeline_config = PipelineConfig {
        id: config.replication_id, // Pipeline ID
        publication_name: config.publication_name,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: 1000,
            max_fill_ms: 5000,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };

    let store = MemoryStore::new();

    // Setup Snowflake Auth
    let auth = KeyPairAuth::new(
        settings.destination.snowflake.account_identifier.clone(),
        settings.destination.snowflake.user.clone(),
        &settings.destination.snowflake.private_key_path,
        settings.destination.snowflake.private_key_passphrase.as_deref(),
    ).map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)) as Box<dyn Error + Send + Sync>)?;

    let destination = SnowflakeDestination::new(settings.destination.snowflake.clone(), auth, table_mapping, schema_mapping);
    
    // Attempt to fetch last offset (for informational purposes or manual slot advancement if we had access)
    match destination.get_last_offset(config.id).await {
        Ok(Some(lsn)) => println!("Found last processed LSN for pipeline {}: {}", config.id, lsn),
        Ok(None) => println!("No previous offset found for pipeline {}. Starting fresh.", config.id),
        Err(e) => eprintln!("Failed to fetch last offset: {}", e),
    }

    println!("Pipeline {} ({}) started", config.id, config.replication_id);

    let mut pipeline = Pipeline::new(pipeline_config, store, destination.clone());
    
    // Start the pipeline
    if let Err(e) = pipeline.start().await {
        return Err(Box::new(e));
    }

    // let config_id = config.id;
    // let replication_id = config.replication_id;

    // Wait for the pipeline indefinitely
    if let Err(e) = pipeline.wait().await {
        return Err(Box::new(e));
    }
    
    println!("Pipeline {} ({}) stopped", config.id, config.replication_id);
    Ok(())
}
