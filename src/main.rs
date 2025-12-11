use flux::management::{ensure_table_exists, fetch_configs, SourceConfig};
use flux::worker::run_pipeline;
use flux::settings::Settings;


use std::error::Error;
use std::collections::{HashMap, HashSet};
use tokio_postgres::NoTls;
use tokio::task::JoinHandle;
use dotenv::dotenv;
use std::env;
use std::time::Duration;

// Imports moved to top using flux:: path


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    dotenv().ok();
    
    // Connect to Management DB
    let database_url = env::var("MANAGEMENT_DATABASE_URL")
        .unwrap_or_else(|_| "host=localhost user=postgres password=postgres dbname=postgres".to_string());
        
    println!("Flux Manager v0.1.0-FIXED");
    println!("Connecting to management db: {}", database_url);
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Management DB connection error: {}", e);
        }
    });

    ensure_table_exists(&client).await?;
    println!("Management table ensured.");

    let settings = Settings::new().expect("Failed to load settings");


    let mut running_pipelines: HashMap<i32, (JoinHandle<()>, SourceConfig)> = HashMap::new();

    loop {
        match fetch_configs(&client).await {
            Ok(configs) => {
                let current_ids: HashSet<i32> = configs.iter().map(|c| c.id).collect();
                
                // 1. Check for stale pipelines (removed from DB)
                let ids_to_remove: Vec<i32> = running_pipelines.keys()
                    .filter(|id| !current_ids.contains(id))
                    .cloned()
                    .collect();
                
                for id in ids_to_remove {
                    println!("Pipeline {} removed from config. Stopping...", id);
                    if let Some((handle, _)) = running_pipelines.remove(&id) {
                        handle.abort();
                    }
                }

                // 2. Check for new or updated pipelines
                for config in configs {
                    let needs_restart = match running_pipelines.get(&config.id) {
                        Some((_, old_config)) => old_config != &config,
                        None => true,
                    };

                    if needs_restart {
                        if let Some((handle, _)) = running_pipelines.remove(&config.id) {
                             println!("Pipeline {} config changed. Restarting...", config.id);
                             handle.abort();
                        } else {
                             println!("Pipeline {} new config found. Starting...", config.id);
                        }

                        let config_clone = config.clone();
                        let settings_clone = settings.clone();
                        let handle = tokio::spawn(async move {
                            // Run the pipeline worker
                            // Worker will run indefinitely unless error occurs.
                            // If error occurs, we retry in this task loop.
                            loop {
                                if let Err(e) = run_pipeline(config_clone.clone(), settings_clone.clone()).await {
                                    eprintln!("Pipeline {} failed: {}. Retrying in 5s...", config_clone.id, e);
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                } else {
                                    // If run_pipeline returns Ok(), it means it stopped gracefully (e.g. intentionally).
                                    // In this setup, it likely means connection closed or similar. 
                                    // We'll restart it.
                                    println!("Pipeline {} stopped gracefully. Restarting in 5s...", config_clone.id);
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                            }
                        });
                        running_pipelines.insert(config.id, (handle, config));
                    }
                }
            }
            Err(e) => eprintln!("Failed to fetch configs: {}", e),
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}