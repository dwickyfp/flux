# Flux ⚡

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-support%20my%20work-FFDD00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/feridfpz)

**A high-performance, real-time CDC engine built in Rust to stream data from PostgreSQL to Snowflake with sub-second latency.**

Flux is a next-generation data orchestration engine designed for speed, reliability, and ease of use. It leverages Rust's zero-cost abstractions and Tokio's asynchronous runtime to handle massive throughput with minimal resource footprint.

## 🚀 Features

- **Sub-Second Latency**: Built on Rust, maximizing performance for real-time analytics.
- **Dynamic Orchestration**: 
  - Automatically detects new pipeline configurations from the management database.
  - Starts, stops, and restarts pipelines without system downtime.
  - Hot-reloading of configuration changes.
- **Fault Tolerant**: Isolated worker threads for each pipeline ensure that a failure in one replication stream does not affect others. Automatic retry logic with exponential backoff.
- **Scalable**: Uses an asynchronous event loop (Tokio) to handle thousands of concurrent replication slots efficiently.
- **Postgres Native**: Utilizes PostgreSQL's logical replication protocol for reliable Change Data Capture (CDC).
- **Batteries Included**: Comes with built-in monitoring and health checks (via standard logs).

## 🏗 Architecture

Flux follows a **Manager-Worker** architecture to ensure stability and dynamic scaling.

### 1. The Manager (Orchestrator)
The main process acts as an orchestrator. It periodically polls the configuration source (`tblsetting_etl_source`) to synchronize the state of the world.
- **Detection**: Identifies added, modified, or removed pipelines.
- **Lifecycle Management**: Spawns independent Tokio tasks for new pipelines and gracefully terminates removed ones.

### 2. The Worker (Pipeline)
Each pipeline runs in its own isolated async task.
- **Source**: Connects to PostgreSQL using `tokio-postgres` and logical replication slots.
- **Processing**: Streams WAL (Write-Ahead Log) events in real-time.
- **Destination**: Batches and loads data into Snowflake (or other supported destinations) efficiently.

```mermaid
graph TD
    DB[(Management DB)] <--> Manager[Flux Manager]
    Manager -- Spawns --> Worker1[Worker: Users Table]
    Manager -- Spawns --> Worker2[Worker: Orders Table]
    Manager -- Spawns --> Worker3[Worker: Payments Table]
    
    Worker1 <== CDC ==> source1[(Postgres: App DB)]
    Worker2 <== CDC ==> source2[(Postgres: App DB)]
    Worker3 <== CDC ==> source3[(Postgres: App DB)]
    
    Worker1 == Stream ==> Snowflake[(Snowflake DW)]
    Worker2 == Stream ==> Snowflake
    Worker3 == Stream ==> Snowflake
```

## 🛠 Getting Started

### Prerequisites
- **Rust Toolchain**: Latest stable version.
- **PostgreSQL**: Version 12+ with `wal_level = logical`.
- **Snowflake Account**: For destination testing.

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/flux.git
   cd flux
   ```

2. Configure Environment:
   Create a `.env` file based on `.env.example`.
   ```bash
   MANAGEMENT_DATABASE_URL=postgres://user:pass@localhost:5432/management_db
   ```

3. Run the Engine:
   ```bash
   cargo run --release
   ```

### Snowflake Key-Pair Authentication Setup

Flux uses JWT authentication with RSA key pairs to connect to Snowflake.

#### 1. Generate a New RSA Key Pair
```bash
# Generate encrypted private key (AES-256)
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out rsa_key.p8
# Enter a passphrase when prompted
```

#### 2. Extract the Public Key
```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
# Enter passphrase when prompted
```

#### 3. Register Public Key in Snowflake
```sql
-- Get the public key content (without BEGIN/END lines)
-- Then run as ACCOUNTADMIN:
ALTER USER YOUR_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';

-- Verify registration:
DESC USER YOUR_USER;
```

#### 4. Configure Flux
Update `config/default.toml`:
```toml
[destination.snowflake]
account_identifier = "ORG-ACCOUNT"
user = "YOUR_USER"
private_key_path = "./user/rsa_key.p8"
private_key_passphrase = "your_passphrase"
```

### 5. Application Configuration (`config/default.toml`)

You can create a `config/default.toml` file to manage your Snowflake connection settings. Here is a reference for the available options:

```toml
[destination.snowflake]
# Your Snowflake Account Identifier (e.g. ORG-ACCOUNT)
# Find this in Snowflake UI -> Admin -> Accounts
account_identifier = "ORG-ACCOUNT" 

# Snowflake User with Key-Pair Authentication configured
user = "FLUX_USER"

# Role with USAGE rights on Warehouse and INSERT rights on Database/Schema
role = "DEV_ROLE"

# Destination Database and Schema for CDC tables
database = "DEVELOPMENT"
schema = "RAW"

# Warehouse to use for loading data
warehouse = "COMPUTE_WH"

# Path to your private key (PKCS8 PEM format)
private_key_path = "./user/rsa_key.p8"

# Passphrase for the private key (if encrypted)
# Remove this line if your key is not encrypted
private_key_passphrase = "your_passphrase"

# Buffering Settings
# How often to flush the buffer to Snowflake (milliseconds)
buffer_flush_interval_ms = 1000
# Maximum number of rows to buffer before flushing
buffer_batch_size = 500
```

## ⚙️ Configuration

Flux uses a table-driven configuration approach. To add a new pipeline, simply insert a record into the management table:

```sql
INSERT INTO tblsetting_etl_source (
    host, port, username, password, dbname, 
    publication_name, replication_id
) VALUES (
    'db.example.com', 5432, 'etl_user', 'secret', 'app_db', 
    'pub_users', 'pipeline_users_01'
);
```

Flux will detect this insert and immediately start streaming data.

## 🤝 Contributing

Contributions are welcome! Please read `CONTRIBUTING.md` for details on our code of conduct and the process for submitting pull requests.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
