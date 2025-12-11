
/// Maps a PostgreSQL type name to a Snowflake data type.
/// Based on common mapping rules and Snowflake documentation.
pub fn map_postgres_to_snowflake(pg_type: &str) -> String {
    match pg_type.to_lowercase().as_str() {
        // Integers
        "int2" | "smallint" => "NUMBER(38,0)".to_string(), // Snowflake recommends NUMBER(38,0) for integers
        "int4" | "integer" | "serial" => "NUMBER(38,0)".to_string(),
        "int8" | "bigint" | "bigserial" => "NUMBER(38,0)".to_string(),
        
        // Floating point & Decimals
        "numeric" | "decimal" => "NUMBER(38,0)".to_string(), // Default to NUMBER, potentially parameterized if precision/scale known
        "float4" | "real" => "FLOAT".to_string(),
        "float8" | "double precision" => "FLOAT".to_string(),
        
        // Characters
        "char" | "bpchar" => "VARCHAR".to_string(),
        "varchar" | "text" | "name" => "VARCHAR".to_string(),
        
        // Boolean
        "bool" | "boolean" => "BOOLEAN".to_string(),
        
        // Date & Time
        "date" => "DATE".to_string(),
        "timestamp" | "timestamp without time zone" => "TIMESTAMP_LTZ".to_string(), // Best practice: align with local/UTC
        "timestamptz" | "timestamp with time zone" => "TIMESTAMP_LTZ".to_string(),
        "time" | "time without time zone" => "TIME".to_string(),
        "timetz" | "time with time zone" => "TIME".to_string(),
        
        // Binary
        "bytea" => "BINARY".to_string(),
        
        // JSON
        "json" | "jsonb" => "VARIANT".to_string(),
        
        // UUID
        "uuid" => "VARCHAR".to_string(),
        
        // Arrays (simplified mapping, usually Variant or Array in Snowflake)
        t if t.starts_with('_') => "ARRAY".to_string(),
        
        // Default fallback
        _ => "VARCHAR".to_string(),
    }
}
