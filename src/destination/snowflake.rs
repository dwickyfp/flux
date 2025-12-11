use crate::settings::SnowflakeConfig;
use crate::snowflake::client::SnowflakeClient;
use crate::auth::KeyPairAuth;
use crate::snowflake::mapper::{map_postgres_to_snowflake};
use etl::destination::Destination;
use etl::error::EtlError;
use etl::types::{Event, TableId, TableRow};

use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{Duration, Instant};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ColumnSchema {
    pub name: String,
    pub pg_type: String,
}

#[derive(Clone)]
pub struct SnowflakeDestination {
    client: Arc<SnowflakeClient>, 
    buffer: Arc<Mutex<Vec<(String, Value)>>>,  // (table_name, row)
    config: SnowflakeConfig,
    last_flush: Arc<Mutex<Instant>>,
    table_mapping: Arc<HashMap<String, String>>,
    schema_mapping: Arc<HashMap<String, Vec<ColumnSchema>>>,
}

impl SnowflakeDestination {
    pub fn new(
        config: SnowflakeConfig, 
        auth: KeyPairAuth, 
        table_mapping: HashMap<String, String>,
        schema_mapping: HashMap<String, Vec<ColumnSchema>>
    ) -> Self {
        Self {
            client: Arc::new(SnowflakeClient::new(config.clone(), auth)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            config,
            last_flush: Arc::new(Mutex::new(Instant::now())),
            table_mapping: Arc::new(table_mapping),
            schema_mapping: Arc::new(schema_mapping),
        }
    }

    pub async fn flush(&self) -> Result<(), EtlError> {
        let mut buffer = self.buffer.lock().await;
        if buffer.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut *buffer);
        drop(buffer);

        // Group by table name
        let mut table_rows: HashMap<String, Vec<Value>> = HashMap::new();
        for (table, row) in batch {
            table_rows.entry(table).or_default().push(row);
        }

        // Insert into each table
        for (table_name, rows) in table_rows {
            if let Err(e) = self.client.insert_rows(&table_name, rows).await {
                eprintln!("Snowflake Insert Error for {}_cdc: {}. Batch dropped.", table_name, e);
            }
        }

        let mut last = self.last_flush.lock().await;
        *last = Instant::now();
        
        Ok(())
    }
    
    async fn buffer_impl(&self, table_name: &str, rows: Vec<Value>) -> Result<(), EtlError> {
        let mut buffer = self.buffer.lock().await;
        for row in rows {
            buffer.push((table_name.to_string(), row));
        }
        
        let should_flush = buffer.len() >= self.config.buffer_batch_size;
        drop(buffer);

        if should_flush {
            self.flush().await?;
        } else {
            let last = self.last_flush.lock().await;
            if last.elapsed() >= Duration::from_millis(self.config.buffer_flush_interval_ms) {
                drop(last);
                self.flush().await?;
            }
        }
        Ok(())
    }

    /// Extract table name from TableId and lookup in mapping
    fn extract_table_name(&self, table_id: &TableId) -> String {
        // TableId debug format is typically "TableId { schema: X, table: Y }" or just ID
        // We want to extract the identifier (OID) and map it
        let debug_str = format!("{:?}", table_id);
        
        // Extract the raw ID/Descriptor
        let raw_id = if let Some(start) = debug_str.find("table:") {
            let rest = &debug_str[start + 6..];
            if let Some(end) = rest.find([',', '}', '"']) {
                rest[..end].trim().trim_matches('"').to_string()
            } else {
                rest.trim().trim_matches('"').to_string()
            }
        } else {
            // Fallback: use full debug string cleaned up
            debug_str.replace(['(', ')', '{', '}', '"', ' '], "").replace("TableId", "")
        };

        // Lookup in mapping
        if let Some(name) = self.table_mapping.get(&raw_id) {
            return name.clone();
        }

        println!("Warning: TableId {} not found in mapping. Using raw ID.", raw_id);
        raw_id
    }
}

impl Destination for SnowflakeDestination {
    fn name() -> &'static str {
        "snowflake"
    }

    async fn truncate_table(&self, _table_id: TableId) -> Result<(), EtlError> {
        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> Result<(), EtlError> {
        let table_name = self.extract_table_name(&table_id);
        let oid = self.extract_oid(&table_id);
        
        let columns = self.schema_mapping.get(&oid);

                let json_rows: Vec<Value> = rows.into_iter().map(|r| {
            // Transform payload to proper JSON Key-Value
            let payload_json = if let Some(cols) = columns {
                 let r_debug = format!("{:?}", r);
                 
                 let content = r_debug
                    .trim_start_matches("TableRow")
                    .trim_matches(|c| c == ' ' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}');
                 
                 parse_table_row_content(content, Some(cols))
            } else {
                 json!({"data": format!("{:?}", r)})
            };

            // Enhanced Debezium-like schema

            // Enhanced Debezium-like schema
            let schema_json = if let Some(cols) = columns {
                json!({
                    "type": "struct",
                    "fields": cols.iter().map(|c| {
                        json!({
                            "field": c.name,
                            "type": map_postgres_to_snowflake(&c.pg_type),
                            "optional": true 
                        })
                    }).collect::<Vec<_>>(),
                    "name": table_name
                })
            } else {
                json!({})
            };

            json!({
                "schema": schema_json,
                "payload": payload_json,
                "op": "c",
                "ts_ms": chrono::Utc::now().timestamp_millis()
            })
        }).collect();
        
        self.buffer_impl(&table_name, json_rows).await
    }

    async fn write_events(&self, events: Vec<Event>) -> Result<(), EtlError> {
        // Group events by table and insert
        for e in events {
            let event_str = format!("{:?}", e);
            
            // Skip non-data events (Relation, Begin, Commit, etc.)
            // We only care about Insert, Update, Delete for CDC data buffering
            let lower = event_str.to_lowercase();
            if lower.starts_with("relation") || lower.starts_with("begin") || lower.starts_with("commit") || lower.starts_with("type") {
                continue;
            }

            let op = if lower.contains("insert") { "c" }
                     else if lower.contains("update") { "u" }
                     else if lower.contains("delete") { "d" }
                     else { continue }; // Skip unknown
            
            let oid = self.extract_oid_from_debug(&event_str);
            
            // Fallback: If OID extraction failed (returned empty or full string), try one more time or default
            let final_oid = if oid.len() > 20 || oid.is_empty() {
                if let Some(idx) = event_str.find("table_id: ") {
                     let rest = &event_str[idx+10..];
                     rest.split(|c: char| !c.is_numeric()).next().unwrap_or("0").to_string()
                } else if let Some(idx) = event_str.find("TableId(") {
                     let rest = &event_str[idx+8..];
                     rest.split(|c: char| !c.is_numeric()).next().unwrap_or("0").to_string()
                } else {
                    "0".to_string()
                }
            } else {
                oid
            };

            let table_name = self.table_mapping.get(&final_oid).map(|s| s.as_str()).unwrap_or("unknown_table");

            if table_name == "unknown_table" {
                continue; 
            }

            // ATTEMPT TO PARSE PAYLOAD
            // Debug string likely contains "values: [I32(23), String("foo")]"
            let values_json = if let Some(start) = event_str.find("values: [") {
                let rest = &event_str[start + 9..];
                if let Some(end) = rest.find(']') {
                     let content = &rest[..end];
                     // Use helper to parse content
                     let columns = self.schema_mapping.get(&final_oid);
                     parse_table_row_content(content, columns)
                } else {
                    json!({ "raw": event_str })
                }
            } else {
                 json!({ "raw": event_str })
            };

            // Build full Debezium-like schema
            let columns = self.schema_mapping.get(&final_oid);
            let schema_json = if let Some(cols) = columns {
                json!({
                    "type": "struct",
                    "fields": cols.iter().map(|c| {
                        json!({
                            "field": c.name,
                            "type": map_postgres_to_snowflake(&c.pg_type),
                            "optional": true
                        })
                    }).collect::<Vec<_>>(),
                    "name": table_name
                })
            } else {
                json!({"name": table_name})
            };

            let row = json!({
                "schema": schema_json,
                "payload": values_json,
                "op": op,
                "ts_ms": chrono::Utc::now().timestamp_millis()
            });
            
            self.buffer_impl(table_name, vec![row]).await?;
        }
        Ok(())
    }
}

impl SnowflakeDestination {
    // Proxy for fetching last offset
    pub async fn get_last_offset(&self, pipeline_id: i32) -> Result<Option<String>, anyhow::Error> {
         self.client.get_last_offset(pipeline_id).await
    }
}

impl SnowflakeDestination {
    fn extract_oid(&self, table_id: &TableId) -> String {
        let debug_str = format!("{:?}", table_id);
        self.extract_oid_from_debug(&debug_str)
    }

    fn extract_oid_from_debug(&self, debug_str: &str) -> String {
        // Look for "TableId { ... table: 1234 ... }" or "TableId(1234)"
        // PATTERN 1: "table: 1234"
        if let Some(pos) = debug_str.find("table:") {
            let after = &debug_str[pos + 6..]; // skip "table:"
            // read digits
            let digits: String = after.chars()
                .skip_while(|c| c.is_whitespace())
                .take_while(|c| c.is_numeric())
                .collect();
            if !digits.is_empty() { return digits; }
        }

        // PATTERN 2: "TableId(1234"
        if let Some(pos) = debug_str.find("TableId") {
            let after = &debug_str[pos + 7..]; // skip "TableId"
            let digits: String = after.chars()
                .skip_while(|c| !c.is_numeric()) // skip ( or { or space
                .take_while(|c| c.is_numeric())
                .collect();
             if !digits.is_empty() { return digits; }
        }

        // PATTERN 3: Just search for any sequence of numbers if string is short
        if debug_str.len() < 50 {
             let digits: String = debug_str.chars().filter(|c| c.is_numeric()).collect();
             if !digits.is_empty() { return digits; }
        }

        // Fallback: If we can't find an ID, return "0" to avoid creating huge table names
        // Logging might be noisy so we just default.
        "0".to_string()
    }
}


fn clean_datum_debug(debug_str: &str) -> Value {
    // Basic heuristics for etl::types::Datum equivalents
    if let Some(inner) = debug_str.splitn(2, '(').nth(1) {
        let content = inner.trim_end_matches(')');
        // Try parsing number
        if let Ok(num) = content.parse::<i64>() {
            return json!(num);
        }
        if let Ok(num) = content.parse::<f64>() {
            return json!(num);
        }
        // Remove quotes if present
        let s = content.trim_matches('"');
        return json!(s);
    }
    // Fallback
    json!(debug_str)
}

fn parse_table_row_content(content: &str, columns: Option<&Vec<ColumnSchema>>) -> Value {
     let mut map = serde_json::Map::new();
     // naive split by comma
     // Note: this breaks if data contains ", ".
     // Improving this would require a real tokenizer.
     let parts: Vec<&str> = content.split(", ").collect();
     
     for (i, part) in parts.iter().enumerate() {
         let json_val = clean_datum_debug(part);
         
         if let Some(cols) = columns {
             if let Some(col) = cols.get(i) {
                 map.insert(col.name.clone(), json_val);
             } else {
                 map.insert(format!("col_{}", i), json_val);
             }
         } else {
              map.insert(format!("col_{}", i), json_val);
         }
     }
     Value::Object(map)
}
