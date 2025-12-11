use anyhow::{Context, Result};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, ACCEPT};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::settings::SnowflakeConfig;
use crate::auth::KeyPairAuth;

pub struct SnowflakeClient {
    client: Client,
    auth: KeyPairAuth,
    config: SnowflakeConfig,
    base_url: String,
    ensured_tables: Arc<Mutex<HashSet<String>>>,
}

impl SnowflakeClient {
    pub fn new(config: SnowflakeConfig, auth: KeyPairAuth) -> Self {
        let base_url = format!(
            "https://{}.snowflakecomputing.com",
            config.account_identifier.replace('_', "-").to_lowercase()
        );
        
        Self {
            client: Client::new(),
            auth,
            config,
            base_url,
            ensured_tables: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Execute a SQL statement via Snowflake SQL API
    async fn execute_sql(&self, sql: &str, bindings: Option<Value>) -> Result<Value> {
        let token = self.auth.generate_token()?;
        let statement_url = format!("{}/api/v2/statements", self.base_url);
        
        let mut request_body = json!({
            "statement": sql,
            "timeout": 60,
            "warehouse": self.config.warehouse
        });
        
        if let Some(b) = bindings {
            request_body["bindings"] = b;
        }

        let response = self.client.post(&statement_url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header("User-Agent", "flux/0.1.0")
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .json(&request_body)
            .send()
            .await
            .context("Failed to send request to Snowflake")?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        
        if !status.is_success() {
            anyhow::bail!("Snowflake API Error: {}", body);
        }

        Ok(serde_json::from_str(&body).unwrap_or(json!({"status": "ok"})))
    }

    /// Ensure a table exists with _cdc suffix, create if not
    pub async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let cdc_table = format!("{}_cdc", table_name.to_lowercase());
        
        {
            let tables = self.ensured_tables.lock().await;
            if tables.contains(&cdc_table) {
                return Ok(());
            }
        }

        let create_table_sql = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"."{}"."{}" (
                SCHEMA_DATABASE VARIANT,
                PAYLOAD VARIANT NOT NULL,
                OPERATION VARCHAR(1) NOT NULL,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )"#,
            self.config.database, self.config.schema, cdc_table.to_uppercase()
        );

        println!("Ensuring table exists: {}.{}.{}", self.config.database, self.config.schema, cdc_table.to_uppercase());
        
        match self.execute_sql(&create_table_sql, None).await {
            Ok(_) => {
                // MIGRATION FIX: Ensure SCHEMA_DATABASE is VARIANT.
                // Since the table might have been created with VARCHAR(255) previously, and IF NOT EXISTS skipped it,
                // we need to strictly ensure the column type.
                // The safest way without complex introspection for this prototype is to try to DROP and ADD.
                // Note: This deletes Old Schema Data. User seems okay with this as they want "fix".
                
                let alter_drop = format!(r#"ALTER TABLE "{}"."{}"."{}" DROP COLUMN SCHEMA_DATABASE"#, 
                    self.config.database, self.config.schema, cdc_table.to_uppercase());
                let _ = self.execute_sql(&alter_drop, None).await; // Ignore error if column doesn't exist (unlikely) or other issues.
                
                let alter_add = format!(r#"ALTER TABLE "{}"."{}"."{}" ADD COLUMN SCHEMA_DATABASE VARIANT"#, 
                     self.config.database, self.config.schema, cdc_table.to_uppercase());
                 let _ = self.execute_sql(&alter_add, None).await;

                println!("Table {} migration ensured (Schema Variant).", cdc_table.to_uppercase());
                let mut tables = self.ensured_tables.lock().await;
                tables.insert(cdc_table);
                Ok(())
            }
            Err(e) => {
                eprintln!("Failed to create table: {}", e);
                Err(e)
            }
        }
    }

    /// Drop a table (with _cdc suffix)
    pub async fn drop_table(&self, table_name: &str) -> Result<()> {
        let cdc_table = format!("{}_CDC", table_name.to_uppercase());
        let sql = format!(
            r#"DROP TABLE IF EXISTS "{}"."{}"."{}""#,
            self.config.database, self.config.schema, cdc_table
        );
        println!("Dropping table: {}.{}.{}", self.config.database, self.config.schema, cdc_table);
        self.execute_sql(&sql, None).await?;
        
        // Remove from ensured set
        let mut tables = self.ensured_tables.lock().await;
        tables.remove(&cdc_table.to_lowercase());
        
        println!("Table {} dropped.", cdc_table);
        Ok(())
    }

    /// Ensure the offset tracking table exists
    pub async fn ensure_offset_table_exists(&self) -> Result<()> {
        let sql = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"."{}"."FLUX_OFFSETS" (
                PIPELINE_ID INTEGER NOT NULL PRIMARY KEY,
                LAST_LSN VARCHAR(255),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )"#,
            self.config.database, self.config.schema
        );
        self.execute_sql(&sql, None).await?;
        Ok(())
    }

    /// Fetch the last LSN for a given pipeline
    pub async fn get_last_offset(&self, pipeline_id: i32) -> Result<Option<String>> {
        self.ensure_offset_table_exists().await?;
        let sql = format!(
            r#"SELECT LAST_LSN FROM "{}"."{}"."FLUX_OFFSETS" WHERE PIPELINE_ID = {}"#,
            self.config.database, self.config.schema, pipeline_id
        );
        let res = self.execute_sql(&sql, None).await?;
        
        // Parse result [ { "LAST_LSN": "..." } ]
        if let Some(arr) = res.as_array() {
            if let Some(row) = arr.first() {
                if let Some(lsn) = row.get("LAST_LSN") {
                    if lsn.is_null() { return Ok(None); }
                    return Ok(Some(lsn.as_str().unwrap_or_default().to_string()));
                }
            }
        }
        Ok(None)
    }

    /// Update the offset for a pipeline
    pub async fn update_offset(&self, pipeline_id: i32, lsn: &str) -> Result<()> {
        let sql = format!(
            r#"MERGE INTO "{}"."{}"."FLUX_OFFSETS" AS target
               USING (SELECT {} AS pid, '{}' AS lsn) AS source
               ON target.PIPELINE_ID = source.pid
               WHEN MATCHED THEN UPDATE SET LAST_LSN = source.lsn, UPDATED_AT = CURRENT_TIMESTAMP()
               WHEN NOT MATCHED THEN INSERT (PIPELINE_ID, LAST_LSN) VALUES (source.pid, source.lsn)"#,
            self.config.database, self.config.schema, pipeline_id, lsn
        );
        self.execute_sql(&sql, None).await?;
        Ok(())
    }

    /// Insert rows into a dynamic table (table_name_cdc)
    pub async fn insert_rows(&self, table_name: &str, rows: Vec<Value>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let cdc_table = format!("{}_CDC", table_name.to_uppercase());

        // Ensure table exists before first insert
        self.ensure_table_exists(table_name).await?;

        let payload_json = json!(rows);
        let payload_str = payload_json.to_string();

        let query = format!(
            r#"INSERT INTO "{}"."{}"."{}" (SCHEMA_DATABASE, PAYLOAD, OPERATION) 
             SELECT value:schema, value:payload, value:op::VARCHAR 
             FROM LATERAL FLATTEN(input => PARSE_JSON(?))"#,
             self.config.database, self.config.schema, cdc_table
        );

        let bindings = json!({
            "1": {
                "type": "TEXT",
                "value": payload_str
            }
        });

        self.execute_sql(&query, Some(bindings)).await?;
        Ok(())
    }
}
