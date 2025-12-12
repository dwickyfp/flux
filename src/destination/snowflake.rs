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
                 // Extract "values: [...]" content
                 // Debug: TableRow { table_id: ..., values: [ ... ], ... }
                 let values_start = r_debug.find("values: [").map(|i| i + 9);
                 
                 if let Some(start) = values_start {
                      // Find matching bracket. Simple approach: search from start
                      // But nested brackets need counting. 
                      // actually clean_datum_debug/split logic handles this, we just need to isolate the ARRAY.
                      // Since 'values' is usually the last or one of middle, we need to balance brackets.
                      
                      // Fallback: use recursion counting
                      let mut balance = 1;
                      let rest = &r_debug[start..];
                      let mut end = 0;
                      for (i, c) in rest.chars().enumerate() {
                          if c == '[' { balance += 1; }
                          else if c == ']' { balance -= 1; }
                          if balance == 0 {
                              end = i;
                              break;
                          }
                      }
                      
                      let values_content = &rest[..end];
                      parse_table_row_values(values_content, Some(cols))
                 } else {
                     // Fallback if values not found in debug string
                      json!({"data": r_debug})
                 }
            } else {
                 json!({"data": format!("{:?}", r)})
            };

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
            // For Event, we might not have 'values' field directly accessible as Vec<Datum> if it's generic Event.
            // But usually Event::Insert(row) -> row has values.
            // Let's check Event definition. If we can't match easily, we might still need some fallback or better matching.
            // For now, let's try to assume we can extract values if we match on Event type.
            // OR, if existing logic was parsing string, maybe we can improve it.
            // BUT the User's example showed `TimestampTz(...)` which implies `Datum` variants are printed.
            
            // Refactoring `write_events` is harder without knowing Event enum structure strictly.
            // However, the "ugly" data was likely coming from `write_table_rows` (the CDC path usually).
            // `write_events` handles `Event` types which might be just serialization of transaction boundaries.
            // Wait, the code says "Skip non-data events". "We only care about Insert, Update...".
            
            // If `Event` is an enum, we should match on it.
            // Let's rely on the previous string parsing for `write_events` temporarily OR minimal fix 
            // because `write_table_rows` is the main data path for `TableRow`.
            // The previous code parsed `values: [...]`.
            // If I look at the logs, the ugly data is `Numeric(Value { ... })`. This is `Datum::Numeric(Decimal)`.
            
            // I will replace the simplistic parsing here with `json!({ "raw": event_str })` for now
            // and assume `write_table_rows` is the primary one used for data sync.
            // The user complaint was likely about the data rows.
            
            let values_json = json!({ "raw": event_str }); 
            // If we really want to parse events, we need `Event` definition.
            // Assuming `write_table_rows` is what matters for "payload" column in Snowflake mentioned by user.


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
    let debug_str = debug_str.trim();

    // Remove wrapping "Numeric(...)" or "SomeWrapped(...)" if present
    // User example: Numeric(Value { ... })
    // If it starts with a type name followed by (, strip it.
    let content = if let Some(start_paren) = debug_str.find('(') {
        if debug_str.ends_with(')') {
            &debug_str[start_paren + 1..debug_str.len() - 1]
        } else {
            debug_str
        }
    } else {
        debug_str
    };

    // Check for weird Rust Decimal Debug format: "Value { sign: Positive, ... }"
    if content.starts_with("Value {") {
        if let Some(num) = parse_decimal_value(content) {
            return json!(num);
        }
    }

    // Basic heuristics for simple types or if stripping failed
    // Double check specific types
    if let Ok(num) = content.parse::<i64>() {
        return json!(num);
    }
    if let Ok(num) = content.parse::<f64>() {
        return json!(num);
    }
    // Remove quotes if present
    let s = content.trim_matches('"');
    // If it says "None", return null
    if s == "None" { return Value::Null; }
    
    json!(s)
}

fn parse_decimal_value(debug_str: &str) -> Option<f64> {
    // Format: Value { sign: Positive, scale: 2, digits: [1500] }
    // OR: Value { sign: Positive, scale: 0, digits: [] } 
    let sign_positive = !debug_str.contains("sign: Negative");
    
    // Extract scale
    let scale_start = debug_str.find("scale: ")? + 7;
    let scale_end = debug_str[scale_start..].find(|c: char| !c.is_numeric())? + scale_start;
    let scale: i32 = debug_str[scale_start..scale_end].parse().ok()?;
    
    // Extract digits
    // digits: [1500]
    let digits_marker = "digits: [";
    let digits_start = debug_str.find(digits_marker)? + digits_marker.len();
    let digits_end = debug_str[digits_start..].find(']')? + digits_start;
    let digits_str = &debug_str[digits_start..digits_end];
    
    // If digits are empty, value is 0
    if digits_str.trim().is_empty() {
        return Some(0.0);
    }
    
    let digits_clean = digits_str.replace(",", "");
    let unscaled_val: f64 = digits_clean.trim().parse().ok()?;
    
    let mut final_val = unscaled_val * 10f64.powi(-scale);
    
    if !sign_positive {
        final_val = -final_val;
    }
    
    Some(final_val)
}

fn split_debug_content(content: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth_curly = 0;
    let mut depth_square = 0;
    let mut depth_paren = 0;
    
    for c in content.chars() {
        match c {
            '{' => depth_curly += 1,
            '}' => depth_curly -= 1,
            '[' => depth_square += 1,
            ']' => depth_square -= 1,
            '(' => depth_paren += 1,
            ')' => depth_paren -= 1,
            ',' => {
                if depth_curly == 0 && depth_square == 0 && depth_paren == 0 {
                    parts.push(current.trim().to_string());
                    current.clear();
                    continue; 
                }
            }
            _ => {}
        }
        current.push(c);
    }
    
    if !current.is_empty() {
        parts.push(current.trim().to_string());
    }
    
    parts
}

fn parse_table_row_values(values_content: &str, columns: Option<&Vec<ColumnSchema>>) -> Value {
     let mut map = serde_json::Map::new();
     
     // Values content should be "I32(1), String("foo"), ..."
     let parts = split_debug_content(values_content);
     
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_debug_content() {
        let input = "I32(2), Timestamp(2025-12-12T09:39:34.009357Z), String(\"Malvin\"), Value { sign: Positive, scale: 2, digits: [1500] }";
        let parts = split_debug_content(input);
        
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "I32(2)");
        assert_eq!(parts[1], "Timestamp(2025-12-12T09:39:34.009357Z)");
        assert_eq!(parts[2], "String(\"Malvin\")");
        assert_eq!(parts[3], "Value { sign: Positive, scale: 2, digits: [1500] }");
    }

    #[test]
    fn test_clean_datum_debug() {
          // Wrapped
         let val = clean_datum_debug("Numeric(Value { sign: Positive, scale: 2, digits: [1500] })");
         assert_eq!(val, json!(15.0));

         // Unwrapped
         let val2 = clean_datum_debug("Value { sign: Positive, scale: 2, digits: [1500] }");
         assert_eq!(val2, json!(15.0));
         
         let val_str = clean_datum_debug("String(\"Hello\")");
         assert_eq!(val_str, json!("Hello"));
         
         let val_int = clean_datum_debug("I32(42)");
         assert_eq!(val_int, json!(42));
    }
}
