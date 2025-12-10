use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use etl::destination::memory::MemoryDestination;
use etl::store::both::memory::MemoryStore;
use std::error::Error;
use crate::management::SourceConfig;
use std::time::Duration;

pub async fn run_pipeline(config: SourceConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
     // Configure Postgres connection.
    let pg_connection_config = PgConnectionConfig {
        host: config.host,
        port: config.port,
        name: config.dbname,
        username: config.username,
        password: Some(config.password.into()),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    };

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
    let destination = MemoryDestination::new();
    
    // Clean logging: Just log start and stop.
    // Ideally, we could subscribe to events here if needed, but per request "keep clean".
    
    println!("Pipeline {} ({}) started", config.id, config.replication_id);

    let mut pipeline = Pipeline::new(pipeline_config, store, destination.clone());
    
    // Start the pipeline
    if let Err(e) = pipeline.start().await {
        return Err(Box::new(e));
    }

    let config_id = config.id;
    let replication_id = config.replication_id;

    // Smart logger task that polls for NEW events only
    let logger = async move {
        let mut last_event_count = 0;
        loop {
             // Check events
             let events = destination.events().await;
             if events.len() > last_event_count {
                 for event in &events[last_event_count..] {
                     println!("Pipeline {} ({}): Transaction Event: {:?}", config_id, replication_id, event);
                 }
                 last_event_count = events.len();
             }

             // Log summary occasionally if rows changed significantly or time passed? 
             // For now just keep it clean, maybe just log events as requested.

             tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };

    // Wait for the pipeline indefinitely OR logger (logger runs forever so select waits for pipeline)
    tokio::select! {
        res = pipeline.wait() => {
            if let Err(e) = res {
                return Err(Box::new(e));
            }
        }
        _ = logger => {
            // Should not happen unless panic
        }
    }
    
    println!("Pipeline {} ({}) stopped", config.id, config.replication_id);
    Ok(())
}
