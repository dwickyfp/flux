use std::error::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceConfig {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub dbname: String,
    pub publication_name: String,
    pub replication_id: u64,
}

pub async fn ensure_table_exists(client: &tokio_postgres::Client) -> Result<(), Box<dyn Error + Send + Sync>> {
    client.batch_execute("
        CREATE TABLE IF NOT EXISTS tblsetting_etl_source (
            id SERIAL PRIMARY KEY,
            host TEXT NOT NULL,
            port INTEGER NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            dbname TEXT NOT NULL,
            publication_name TEXT NOT NULL,
            replication_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ").await?;
    Ok(())
}

pub async fn fetch_configs(client: &tokio_postgres::Client) -> Result<Vec<SourceConfig>, Box<dyn Error + Send + Sync>> {
    let rows = client.query("SELECT id, host, port, username, password, dbname, publication_name, replication_id FROM tblsetting_etl_source", &[]).await?;
    let mut configs = Vec::new();
    for row in rows {
        configs.push(SourceConfig {
            id: row.get(0),
            host: row.get(1),
            port: row.get::<_, i32>(2) as u16, // Postgres INT is i32
            username: row.get(3),
            password: row.get(4),
            dbname: row.get(5),
            publication_name: row.get(6),
            replication_id: row.get::<_, i64>(7) as u64,
        });
    }
    Ok(configs)
}
