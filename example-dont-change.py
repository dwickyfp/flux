"""
Postgres to Snowflake Type Mapper

Maps PostgreSQL data types to their Snowflake equivalents.
"""


# Mapping from PostgreSQL types to Snowflake types
PG_TO_SF_TYPE_MAP = {
    # Numeric types
    "smallint": "SMALLINT",
    "int2": "SMALLINT",
    "integer": "INTEGER",
    "int": "INTEGER",
    "int4": "INTEGER",
    "bigint": "BIGINT",
    "int8": "BIGINT",
    "decimal": "NUMBER",
    "numeric": "NUMBER",
    "real": "FLOAT",
    "float4": "FLOAT",
    "double precision": "DOUBLE",
    "float8": "DOUBLE",
    "serial": "INTEGER",
    "serial4": "INTEGER",
    "bigserial": "BIGINT",
    "serial8": "BIGINT",
    "smallserial": "SMALLINT",
    "serial2": "SMALLINT",
    "money": "NUMBER(19,4)",
    
    # Character types
    "character varying": "VARCHAR",
    "varchar": "VARCHAR",
    "character": "CHAR",
    "char": "CHAR",
    "text": "TEXT",
    "name": "VARCHAR(64)",
    
    # Binary types
    "bytea": "BINARY",
    
    # Date/Time types
    "timestamp": "TIMESTAMP_NTZ",
    "timestamp without time zone": "TIMESTAMP_NTZ",
    "timestamp with time zone": "TIMESTAMP_TZ",
    "timestamptz": "TIMESTAMP_TZ",
    "date": "DATE",
    "time": "TIME",
    "time without time zone": "TIME",
    "time with time zone": "TIME",
    "timetz": "TIME",
    "interval": "VARCHAR",  # Snowflake doesn't have interval, store as string
    
    # Boolean type
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    
    # UUID type
    "uuid": "VARCHAR(36)",
    
    # JSON types
    "json": "VARIANT",
    "jsonb": "VARIANT",
    
    # Array types - will be handled specially
    "array": "ARRAY",
    
    # Network types
    "inet": "VARCHAR(45)",
    "cidr": "VARCHAR(45)",
    "macaddr": "VARCHAR(17)",
    "macaddr8": "VARCHAR(23)",
    
    # Geometric types - store as string/variant
    "point": "VARCHAR",
    "line": "VARCHAR",
    "lseg": "VARCHAR",
    "box": "VARCHAR",
    "path": "VARCHAR",
    "polygon": "VARCHAR",
    "circle": "VARCHAR",
    
    # Text search types
    "tsvector": "VARCHAR",
    "tsquery": "VARCHAR",
    
    # XML type
    "xml": "VARCHAR",
    
    # OID types
    "oid": "INTEGER",
    "regproc": "VARCHAR",
    "regprocedure": "VARCHAR",
    "regoper": "VARCHAR",
    "regoperator": "VARCHAR",
    "regclass": "VARCHAR",
    "regtype": "VARCHAR",
    "regrole": "VARCHAR",
    "regnamespace": "VARCHAR",
    "regconfig": "VARCHAR",
    "regdictionary": "VARCHAR",
    
    # Bit string types
    "bit": "VARCHAR",
    "bit varying": "VARCHAR",
    "varbit": "VARCHAR",
}


def map_pg_type_to_sf(pg_type: str, type_modifier: str | None = None) -> str:
    """
    Map a PostgreSQL data type to its Snowflake equivalent.
    
    Args:
        pg_type: The PostgreSQL data type name (lowercase).
        type_modifier: Optional type modifier (e.g., precision, scale, length).
        
    Returns:
        The corresponding Snowflake data type.
    """
    # Normalize the type name
    pg_type_lower = pg_type.lower().strip()
    
    # Handle array types
    if pg_type_lower.endswith("[]") or pg_type_lower.startswith("_"):
        return "ARRAY"
    
    # Handle types with modifiers like varchar(255), numeric(10,2)
    if "(" in pg_type_lower:
        base_type = pg_type_lower.split("(")[0].strip()
        modifier = pg_type_lower.split("(")[1].rstrip(")")
    else:
        base_type = pg_type_lower
        modifier = type_modifier
    
    # Look up the base type
    sf_type = PG_TO_SF_TYPE_MAP.get(base_type, "VARCHAR")
    
    # Apply modifiers for certain types
    if modifier:
        if base_type in ("character varying", "varchar"):
            sf_type = f"VARCHAR({modifier})"
        elif base_type in ("character", "char"):
            sf_type = f"CHAR({modifier})"
        elif base_type in ("numeric", "decimal"):
            sf_type = f"NUMBER({modifier})"
        elif base_type == "bit":
            sf_type = f"VARCHAR({modifier})"
    
    return sf_type


def get_column_ddl(column_name: str, pg_type: str, is_nullable: bool = True, 
                   default_value: str | None = None) -> str:
    """
    Generate Snowflake DDL for a column based on PostgreSQL column definition.
    
    Args:
        column_name: Name of the column.
        pg_type: PostgreSQL data type.
        is_nullable: Whether the column allows NULL values.
        default_value: Default value for the column.
        
    Returns:
        DDL fragment for the column definition.
    """
    sf_type = map_pg_type_to_sf(pg_type)
    
    # Quote the column name to handle reserved words and special characters
    quoted_name = f'"{column_name.upper()}"'
    
    ddl = f"{quoted_name} {sf_type}"
    
    if not is_nullable:
        ddl += " NOT NULL"
    
    # Note: Default values from PG may not be directly compatible with SF
    # We skip them for now to avoid compatibility issues
    
    return ddl

"""
Postgres to Snowflake Migrator

Core migration logic for transferring tables from PostgreSQL to Snowflake.
"""

import logging
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


logger = logging.getLogger(__name__)


class MigrationError(Exception):
    """Custom exception for migration errors."""
    pass


class PostgresConnection:
    """PostgreSQL connection manager."""
    
    def __init__(self, connection_string: str):
        """
        Initialize PostgreSQL connection.
        
        Args:
            connection_string: PostgreSQL connection URI.
                Format: postgresql://user:password@host:port/database
        """
        self.connection_string = connection_string
        self.conn = None
    
    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            logger.info("Connected to PostgreSQL successfully")
        except psycopg2.Error as e:
            raise MigrationError(f"Failed to connect to PostgreSQL: {e}")
    
    def close(self) -> None:
        """Close the PostgreSQL connection."""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SnowflakeConnection:
    """Snowflake connection manager."""
    
    def __init__(
        self,
        user: str,
        account: str,
        warehouse: str,
        database: str,
        schema: str,
        password: str | None = None,
        role: str | None = None,
        authenticator: str | None = None,
        passcode: str | None = None,
        private_key_path: str | None = None,
        private_key_passphrase: str | None = None
    ):
        """
        Initialize Snowflake connection.
        
        Args:
            user: Snowflake username.
            account: Snowflake account identifier.
            warehouse: Snowflake warehouse name.
            database: Target database name.
            schema: Target schema name.
            password: Snowflake password (optional if using private key).
            role: Optional role to use.
            authenticator: Optional authenticator.
            passcode: Optional passcode for MFA.
            private_key_path: Path to the private key file.
            private_key_passphrase: Password for the private key file.
        """
        self.user = user.upper() if user else user
        self.password = password
        self.account = account.upper() if account else account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.authenticator = authenticator
        self.passcode = passcode
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        self.conn = None
    
    def _load_private_key(self):
        """Load and serialize the private key."""
        if not self.private_key_path:
            return None
            
        passphrase = str(self.private_key_passphrase).encode() if self.private_key_passphrase is not None else None
        
        with open(self.private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase,
                backend=default_backend()
            )
            
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return pkb

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            conn_params = {
                "user": self.user,
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
            }
            
            if self.password:
                conn_params["password"] = self.password
            
            if self.role:
                conn_params["role"] = self.role
            if self.authenticator:
                conn_params["authenticator"] = self.authenticator
            if self.passcode:
                conn_params["passcode"] = self.passcode
                
            if self.private_key_path:
                conn_params["private_key"] = self._load_private_key()
            
            self.conn = snowflake.connector.connect(**conn_params)
            
            # Explicitly set the warehouse, database and schema context
            if self.warehouse:
                with self.conn.cursor() as cur:
                    cur.execute(f'USE WAREHOUSE "{self.warehouse.upper()}"')
            if self.database:
                with self.conn.cursor() as cur:
                    cur.execute(f'USE DATABASE "{self.database.upper()}"')
            if self.schema:
                with self.conn.cursor() as cur:
                    cur.execute(f'USE SCHEMA "{self.schema.upper()}"')
            
            logger.info(f"Connected to Snowflake and set context to {self.warehouse}.{self.database}.{self.schema}")
        except snowflake.connector.Error as e:
            raise MigrationError(f"Failed to connect to Snowflake or set context: {e}")
    
    def close(self) -> None:
        """Close the Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class TableMigrator:
    """Handles migration of individual tables from PostgreSQL to Snowflake."""
    
    def __init__(
        self,
        pg_conn: psycopg2.extensions.connection,
        sf_conn: snowflake.connector.SnowflakeConnection,
        chunk_size: int = 10000,
        dry_run: bool = False
    ):
        """
        Initialize the table migrator.
        
        Args:
            pg_conn: Active PostgreSQL connection.
            sf_conn: Active Snowflake connection.
            chunk_size: Number of rows to process at a time.
            dry_run: If True, only print DDL without executing.
        """
        self.pg_conn = pg_conn
        self.sf_conn = sf_conn
        self.chunk_size = chunk_size
        self.dry_run = dry_run
    
    def get_table_schema(self, table_name: str, schema: str = "public") -> list[dict[str, Any]]:
        """
        Retrieve the schema of a PostgreSQL table.
        
        Args:
            table_name: Name of the table.
            schema: Schema name (default: public).
            
        Returns:
            List of column definitions with name, type, nullable, default.
        """
        query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
                udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        
        with self.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (schema, table_name))
            columns = cur.fetchall()
        
        if not columns:
            # Table not found, try to list available tables to help the user
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name",
                    (schema,)
                )
                available = [row[0] for row in cur.fetchall()]
            
            msg = f"Table '{schema}.{table_name}' not found or has no columns."
            if available:
                msg += f"\nAvailable tables in '{schema}': {', '.join(available)}"
                # Check for case sensitivity issues
                lower_map = {t.lower(): t for t in available}
                if table_name.lower() in lower_map:
                    msg += f"\nDid you mean '{lower_map[table_name.lower()]}'?"
            else:
                msg += f"\nNo tables found in schema '{schema}'."
                
            raise MigrationError(msg)
        
        schema_info = []
        for col in columns:
            # Build the full type with modifiers
            data_type = col["data_type"]
            if col["character_maximum_length"]:
                data_type = f"{data_type}({col['character_maximum_length']})"
            elif col["numeric_precision"] and col["data_type"] in ("numeric", "decimal"):
                scale = col["numeric_scale"] or 0
                data_type = f"{data_type}({col['numeric_precision']},{scale})"
            
            # Handle array types
            if data_type == "ARRAY":
                data_type = col["udt_name"]
            
            schema_info.append({
                "column_name": col["column_name"],
                "data_type": data_type,
                "is_nullable": col["is_nullable"] == "YES",
                "column_default": col["column_default"],
            })
        
        return schema_info
    
    def get_primary_keys(self, table_name: str, schema: str = "public") -> list[str]:
        """
        Get primary key columns for a table.
        
        Args:
            table_name: Name of the table.
            schema: Schema name.
            
        Returns:
            List of primary key column names.
        """
        query = """
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary
              AND n.nspname = %s
              AND c.relname = %s
            ORDER BY array_position(i.indkey, a.attnum);
        """
        
        with self.pg_conn.cursor() as cur:
            cur.execute(query, (schema, table_name))
            return [row[0] for row in cur.fetchall()]
    
    def generate_create_table_ddl(
        self,
        table_name: str,
        schema_info: list[dict[str, Any]],
        primary_keys: list[str] | None = None
    ) -> str:
        """
        Generate Snowflake CREATE TABLE DDL.
        
        Args:
            table_name: Target table name in Snowflake.
            schema_info: Column definitions from get_table_schema.
            primary_keys: List of primary key column names.
            
        Returns:
            CREATE TABLE DDL statement.
        """
        column_defs = []
        column_names_set = set()
        for col in schema_info:
            column_names_set.add(col["column_name"].upper())
            ddl = get_column_ddl(
                column_name=col["column_name"],
                pg_type=col["data_type"],
                is_nullable=col["is_nullable"],
                default_value=col["column_default"]
            )
            column_defs.append(ddl)
        
        # Add primary key constraint if exists, but only for columns that exist
        pk_clause = ""
        if primary_keys:
            # Filter to only valid PKs that exist in the column list
            valid_pks = [pk for pk in primary_keys if pk.upper() in column_names_set]
            missing_pks = [pk for pk in primary_keys if pk.upper() not in column_names_set]
            
            if missing_pks:
                logger.warning(f"Primary key column(s) not found in schema, skipping: {missing_pks}")
            
            if valid_pks:
                pk_cols = ", ".join(f'"{pk.upper()}"' for pk in valid_pks)
                pk_clause = f",\n    PRIMARY KEY ({pk_cols})"
        
        ddl = f"""CREATE TABLE IF NOT EXISTS "{table_name.upper()}" (
    {(",\n    ".join(column_defs))}{pk_clause}
);"""
        
        logger.debug(f"Generated DDL for '{table_name}':\n{ddl}")
        
        return ddl
    
    def create_snowflake_table(self, table_name: str, ddl: str) -> None:
        """
        Execute CREATE TABLE DDL in Snowflake.
        
        Args:
            table_name: Name of the table being created.
            ddl: The DDL statement to execute.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would execute DDL:\n{ddl}")
            print(f"\n--- DDL for {table_name} ---\n{ddl}\n")
            return
        
        try:
            with self.sf_conn.cursor() as cur:
                cur.execute(ddl)
            logger.info(f"Created table '{table_name}' in Snowflake")
        except snowflake.connector.Error as e:
            raise MigrationError(f"Failed to create table '{table_name}': {e}")
    
    def migrate_data(
        self,
        table_name: str,
        pg_schema: str = "public",
        sf_table_name: str | None = None
    ) -> int:
        """
        Migrate data from PostgreSQL to Snowflake.
        
        Args:
            table_name: Source table name in PostgreSQL.
            pg_schema: PostgreSQL schema name.
            sf_table_name: Target table name in Snowflake (defaults to source name).
            
        Returns:
            Number of rows migrated.
        """
        sf_table_name = sf_table_name or table_name
        
        if self.dry_run:
            # Count rows in dry run mode
            with self.pg_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {pg_schema}.{table_name}")
                count = cur.fetchone()[0]
            logger.info(f"[DRY RUN] Would migrate {count} rows from '{table_name}'")
            return count
        
        total_rows = 0
        offset = 0
        
        logger.info(f"Starting data migration for table '{table_name}'")
        
        while True:
            # Read chunk from PostgreSQL
            query = f"""
                SELECT * FROM {pg_schema}.{table_name}
                ORDER BY 1
                LIMIT {self.chunk_size} OFFSET {offset}
            """
            
            df = pd.read_sql(query, self.pg_conn)
            
            if df.empty:
                break
            
            rows_in_chunk = len(df)
            
            # Convert column names to uppercase for Snowflake
            df.columns = [col.upper() for col in df.columns]
            
            # Write to Snowflake
            try:
                success, num_chunks, num_rows, _ = write_pandas(
                    self.sf_conn,
                    df,
                    sf_table_name.upper(),
                    auto_create_table=False
                )
                
                if not success:
                    raise MigrationError(f"Failed to write chunk to Snowflake")
                
                total_rows += rows_in_chunk
                offset += self.chunk_size
                
                logger.info(f"Migrated {total_rows} rows for '{table_name}'")
                
            except Exception as e:
                raise MigrationError(f"Error writing to Snowflake: {e}")
        
        logger.info(f"Completed migration of {total_rows} rows for table '{table_name}'")
        return total_rows
    
    def migrate_table(
        self,
        table_name: str,
        pg_schema: str = "public",
        drop_existing: bool = False
    ) -> dict[str, Any]:
        """
        Perform full migration of a table from PostgreSQL to Snowflake.
        
        Args:
            table_name: Name of the table to migrate.
            pg_schema: PostgreSQL schema name.
            drop_existing: If True, drop existing table in Snowflake first.
            
        Returns:
            Migration result with table name, row count, and status.
        """
        logger.info(f"Starting migration of table '{pg_schema}.{table_name}'")
        
        # Get source schema
        schema_info = self.get_table_schema(table_name, pg_schema)
        primary_keys = self.get_primary_keys(table_name, pg_schema)
        
        logger.info(f"Found {len(schema_info)} columns, {len(primary_keys)} primary key(s)")
        
        # Generate and execute DDL
        ddl = self.generate_create_table_ddl(table_name, schema_info, primary_keys)
        
        if drop_existing and not self.dry_run:
            with self.sf_conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table_name.upper()}"')
            logger.info(f"Dropped existing table '{table_name}'")
        
        self.create_snowflake_table(table_name, ddl)
        
        # Migrate data
        row_count = self.migrate_data(table_name, pg_schema)
        
        return {
            "table_name": table_name,
            "columns": len(schema_info),
            "primary_keys": primary_keys,
            "rows_migrated": row_count,
            "status": "success"
        }
#!/usr/bin/env python3
"""
Postgres to Snowflake Migration Tool

A tool to migrate tables from PostgreSQL to Snowflake,
preserving metadata and data types.

Configure the settings below and run: python main.py
"""

import logging
import sys
from datetime import datetime


# =============================================================================
# CONFIGURATION - Edit these values
# =============================================================================

# PostgreSQL Connection
PG_URI = "postgresql://ebs.admin:zwPEO<x21700@10.201.14.115:5432/silkroad_j"
PG_SCHEMA = "public"

# Snowflake Connection
SF_ACCOUNT = "IWHDQUE-BA01775"      # e.g., "xy12345.us-east-1"
SF_USER = "KAFKA_CONNECTOR_USER"
SF_WAREHOUSE = "SNOWFLAKE_STREAM_WH"
SF_DATABASE = "SILKROAD"
SF_SCHEMA = "BRONZE"
SF_ROLE = "KAFKA_CONNECTOR_ROLE"       

# Authentication: Choose Password OR Private Key
# Option 1: Password / MFA
SF_PASSWORD = None                      # Password if using password auth
SF_AUTHENTICATOR = None                 # Optional: 'username_password_mfa'
SF_PASSCODE = None                      # Optional: MFA Passcode

# Option 2: Private Key
SF_PRIVATE_KEY_PATH = "./rsa_key.p8"  # Path to private key file
SF_PRIVATE_KEY_PASSPHRASE = 123456            # Passphrase if key is encrypted

# Tables to migrate (list of table names)
TABLES = [
    "tblmdm_chief_org"
]


# Migration Options
CHUNK_SIZE = 10000                      # Rows per batch
DROP_EXISTING = False                   # Drop existing tables before migration
DRY_RUN = False                         # True = only print DDL, don't execute
VERBOSE = True                          # Enable detailed logging

# =============================================================================
# END CONFIGURATION
# =============================================================================


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def main() -> int:
    """Main entry point for the migration tool."""
    setup_logging(VERBOSE)
    
    logger = logging.getLogger(__name__)
    
    # Validate table list
    table_names = [t.strip() for t in TABLES if t.strip()]
    
    if not table_names:
        logger.error("No tables specified for migration")
        return 1
    
    logger.info(f"Starting migration of {len(table_names)} table(s)")
    logger.info(f"Tables: {', '.join(table_names)}")
    
    if DRY_RUN:
        logger.info("DRY RUN MODE - No changes will be made to Snowflake")
    
    start_time = datetime.now()
    results = []
    
    try:
        # Establish connections
        with PostgresConnection(PG_URI) as pg_conn:
            with SnowflakeConnection(
                user=SF_USER,
                account=SF_ACCOUNT,
                warehouse=SF_WAREHOUSE,
                database=SF_DATABASE,
                schema=SF_SCHEMA,
                password=SF_PASSWORD,
                role=SF_ROLE,
                authenticator=SF_AUTHENTICATOR,
                passcode=SF_PASSCODE,
                private_key_path=SF_PRIVATE_KEY_PATH,
                private_key_passphrase=SF_PRIVATE_KEY_PASSPHRASE
            ) as sf_conn:
                
                # Create migrator instance
                migrator = TableMigrator(
                    pg_conn=pg_conn.conn,
                    sf_conn=sf_conn.conn,
                    chunk_size=CHUNK_SIZE,
                    dry_run=DRY_RUN
                )
                
                # Migrate each table
                for table_name in table_names:
                    try:
                        logger.info(f"Migrating table: {table_name}")
                        result = migrator.migrate_table(
                            table_name=table_name,
                            pg_schema=PG_SCHEMA,
                            drop_existing=DROP_EXISTING
                        )
                        results.append(result)
                        logger.info(f"Successfully migrated '{table_name}'")
                        
                    except MigrationError as e:
                        logger.error(f"Failed to migrate '{table_name}': {e}")
                        results.append({
                            "table_name": table_name,
                            "status": "failed",
                            "error": str(e)
                        })
    
    except MigrationError as e:
        logger.error(f"Migration failed: {e}")
        return 1
    
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1
    
    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r.get("status") == "success"]
    failed = [r for r in results if r.get("status") == "failed"]
    
    print(f"Total tables: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Duration: {duration}")
    
    if successful:
        print("\nSuccessful migrations:")

    if failed:
        print("\nFailed migrations:")
        for r in failed:
            print(f"  - {r['table_name']}: {r.get('error', 'Unknown error')}")
    
    print("=" * 60)
    
    return 0 if not failed else 1


main()
