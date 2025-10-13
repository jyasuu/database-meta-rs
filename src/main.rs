use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;

#[derive(Parser)]
#[command(name = "database-meta")]
#[command(about = "Database metadata extraction and comparison tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract database metadata
    DatabaseMeta {
        /// Output format (json, yaml, csv, xml, html, insert)
        format: String,
        /// Configuration file path
        #[arg(short, long)]
        config: String,
        /// Output directory
        #[arg(short, long, default_value = "./out")]
        out: String,
    },
    /// Compare tables between databases and generate SQL
    CompareTables {
        /// Configuration file path
        #[arg(short, long)]
        config: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct Config {
    databases: HashMap<String, DatabaseConfig>,
    tables: Vec<TableConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DatabaseConfig {
    #[serde(rename = "jdbcUrl")]
    jdbc_url: String,
    username: String,
    password: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct TableConfig {
    name: String,
    primary_key: Option<Vec<String>>,
    order: Option<String>,
    columns: Vec<ColumnConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ColumnConfig {
    column_name: String,
    data_type: String,
    is_track: String,
    #[serde(rename = "type")]
    column_type: Option<String>,
    default: Option<String>,
}

struct DatabaseMetaProcessor {
    config: Config,
}

impl DatabaseMetaProcessor {
    fn new(config: Config) -> Self {
        Self { config }
    }

    async fn database_meta(&self, format: String, out: String) -> Result<String> {
        let source_config = self.config.databases.get("source")
            .context("Source database configuration not found")?;

        let pool = self.create_connection_pool(source_config).await?;

        for table_config in &self.config.tables {
            let table_name = &table_config.name;
            let order_columns = self.parse_order_columns(&table_config.order);
            
            let (fields, _field_map) = self.build_fields(&table_config.columns);
            let query = self.build_select_query(table_name, &fields);
            
            let rows = sqlx::query(&query)
                .fetch_all(&pool)
                .await
                .context("Failed to execute query")?;

            let mut records: Vec<HashMap<String, serde_json::Value>> = Vec::new();
            
            for row in rows {
                let mut record = HashMap::new();
                for (i, field) in fields.iter().enumerate() {
                    let column_config = table_config.columns.iter()
                        .find(|c| c.column_name == *field)
                        .unwrap_or(&table_config.columns[0]); // fallback, should not happen
                    let value = self.extract_typed_value(&row, i, column_config);
                    record.insert(field.clone(), value);
                }
                records.push(record);
            }

            // Sort records if order columns are specified
            if !order_columns.is_empty() {
                records.sort_by(|a, b| {
                    for col in &order_columns {
                        if let (Some(val_a), Some(val_b)) = (a.get(col), b.get(col)) {
                            // Handle different value types for comparison
                            match (val_a, val_b) {
                                (serde_json::Value::Number(n1), serde_json::Value::Number(n2)) => {
                                    match (n1.as_i64(), n2.as_i64()) {
                                        (Some(i1), Some(i2)) => match i1.cmp(&i2) {
                                            std::cmp::Ordering::Equal => continue,
                                            ordering => return ordering,
                                        },
                                        _ => match (n1.as_f64(), n2.as_f64()) {
                                            (Some(f1), Some(f2)) => match f1.partial_cmp(&f2) {
                                                Some(std::cmp::Ordering::Equal) => continue,
                                                Some(ordering) => return ordering,
                                                None => continue,
                                            },
                                            _ => continue,
                                        }
                                    }
                                }
                                _ => {
                                    let str_a = val_a.as_str().map(|s| s.to_string()).unwrap_or_else(|| val_a.to_string());
                                    let str_b = val_b.as_str().map(|s| s.to_string()).unwrap_or_else(|| val_b.to_string());
                                    match str_a.cmp(&str_b) {
                                        std::cmp::Ordering::Equal => continue,
                                        ordering => return ordering,
                                    }
                                }
                            }
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            let file_content = self.format_output(&format, &records, table_name)?;
            let ext = self.get_file_extension(&format);
            
            self.write_to_file(&out, table_name, &ext, &file_content)?;
        }

        Ok(String::new())
    }

    async fn compare_tables(&self) -> Result<String> {
        let source_config = self.config.databases.get("source")
            .context("Source database configuration not found")?;
        let target_config = self.config.databases.get("target")
            .context("Target database configuration not found")?;

        let source_pool = self.create_connection_pool(source_config).await?;
        let target_pool = self.create_connection_pool(target_config).await?;

        let mut sql_statements = Vec::new();

        for table_config in &self.config.tables {
            let table_name = &table_config.name;
            let primary_keys = table_config.primary_key.as_ref()
                .context("Primary key not specified for table")?;

            let source_data = self.fetch_table_data(&source_pool, table_name, &table_config.columns).await?;
            let target_data = self.fetch_table_data(&target_pool, table_name, &table_config.columns).await?;

            let comparison_result = self.compare_and_generate_sql(
                table_name,
                &source_data,
                &target_data,
                &table_config.columns,
                primary_keys,
            );

            sql_statements.extend(comparison_result);
        }

        // Write DML statements to files per table
        std::fs::create_dir_all("out")?;
        
        for table_config in &self.config.tables {
            let table_name = &table_config.name;
            let table_statements: Vec<&String> = sql_statements.iter()
                .filter(|stmt| stmt.contains(&format!("INTO {}", table_name)) || 
                              stmt.contains(&format!("UPDATE {}", table_name)) ||
                              stmt.contains(&format!("DELETE FROM {}", table_name)))
                .collect();
            
            if !table_statements.is_empty() {
                let file_path = format!("out/{}.sql", table_name);
                let mut file = File::create(file_path)?;
                for statement in table_statements {
                    writeln!(file, "{};", statement)?;
                }
            }
        }

        Ok("Comparison completed and SQL generated.".to_string())
    }

    async fn create_connection_pool(&self, config: &DatabaseConfig) -> Result<PgPool> {
        let database_url = self.convert_jdbc_to_postgres_url(&config.jdbc_url, &config.username, &config.password)?;
        PgPool::connect(&database_url)
            .await
            .context("Failed to connect to database")
    }

    fn convert_jdbc_to_postgres_url(&self, jdbc_url: &str, username: &str, password: &str) -> Result<String> {
        let url = jdbc_url.strip_prefix("jdbc:postgresql://")
            .context("Invalid JDBC URL format")?;
        Ok(format!("postgresql://{}:{}@{}", username, password, url))
    }

    fn parse_order_columns(&self, order: &Option<String>) -> Vec<String> {
        order.as_ref()
            .map(|o| o.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
            .unwrap_or_default()
    }

    fn build_fields(&self, columns: &[ColumnConfig]) -> (Vec<String>, HashMap<String, String>) {
        let mut fields = Vec::new();
        let mut field_map = HashMap::new();

        for column in columns {
            let field_name = if column.is_track == "true" {
                column.column_name.clone()
            } else {
                // Handle default values for non-tracked columns
                match column.column_type.as_deref() {
                    Some("numeric") => {
                        column.default.as_ref().unwrap_or(&"0".to_string()).clone()
                    }
                    _ => column.default.as_ref().unwrap_or(&"".to_string()).clone()
                }
            };
            
            fields.push(column.column_name.clone());
            field_map.insert(column.column_name.clone(), field_name);
        }

        (fields, field_map)
    }

    fn build_select_query(&self, table_name: &str, fields: &[String]) -> String {
        let field_list = fields.join(", ");
        format!("SELECT {} FROM {}", field_list, table_name)
    }

    fn format_output(&self, format: &str, records: &[HashMap<String, serde_json::Value>], table_name: &str) -> Result<String> {
        match format {
            "json" => Ok(serde_json::to_string_pretty(records)?),
            "yaml" => Ok(serde_yaml::to_string(records)?),
            "csv" => self.format_as_csv(records),
            "xml" => self.format_as_xml(records),
            "html" => self.format_as_html(records, table_name),
            "insert" => self.format_as_insert(records, table_name),
            _ => Ok(serde_json::to_string_pretty(records)?),
        }
    }

    fn format_as_csv(&self, records: &[HashMap<String, serde_json::Value>]) -> Result<String> {
        if records.is_empty() {
            return Ok(String::new());
        }

        let mut wtr = csv::Writer::from_writer(vec![]);
        
        // Write headers
        let headers: Vec<String> = records[0].keys().cloned().collect();
        wtr.write_record(&headers)?;

        // Write data
        for record in records {
            let row: Vec<String> = headers.iter()
                .map(|h| record.get(h).unwrap_or(&serde_json::Value::Null).to_string())
                .collect();
            wtr.write_record(&row)?;
        }

        Ok(String::from_utf8(wtr.into_inner()?)?)
    }

    fn format_as_xml(&self, records: &[HashMap<String, serde_json::Value>]) -> Result<String> {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<records>\n");
        
        for record in records {
            xml.push_str("  <record>\n");
            for (key, value) in record {
                xml.push_str(&format!("    <{}>{}</{}>\n", key, value, key));
            }
            xml.push_str("  </record>\n");
        }
        
        xml.push_str("</records>");
        Ok(xml)
    }

    fn format_as_html(&self, records: &[HashMap<String, serde_json::Value>], table_name: &str) -> Result<String> {
        if records.is_empty() {
            return Ok(format!("<html><body><h1>{}</h1><p>No data</p></body></html>", table_name));
        }

        let mut html = format!("<html><body><h1>{}</h1><table border=\"1\">\n", table_name);
        
        // Headers
        html.push_str("<tr>");
        for key in records[0].keys() {
            html.push_str(&format!("<th>{}</th>", key));
        }
        html.push_str("</tr>\n");

        // Data
        for record in records {
            html.push_str("<tr>");
            for key in records[0].keys() {
                let value = record.get(key).unwrap_or(&serde_json::Value::Null);
                html.push_str(&format!("<td>{}</td>", value));
            }
            html.push_str("</tr>\n");
        }

        html.push_str("</table></body></html>");
        Ok(html)
    }

    fn format_as_insert(&self, records: &[HashMap<String, serde_json::Value>], table_name: &str) -> Result<String> {
        if records.is_empty() {
            return Ok(String::new());
        }

        let mut sql = String::new();
        let columns: Vec<String> = records[0].keys().cloned().collect();
        let column_list = columns.join(", ");

        for record in records {
            let values: Vec<String> = columns.iter()
                .map(|col| {
                    let value = record.get(col).unwrap_or(&serde_json::Value::Null);
                    match value {
                        serde_json::Value::String(s) => format!("'{}'", s.replace("'", "''")),
                        serde_json::Value::Null => "NULL".to_string(),
                        _ => value.to_string(),
                    }
                })
                .collect();
            
            sql.push_str(&format!(
                "INSERT INTO {} ({}) VALUES ({});\n",
                table_name,
                column_list,
                values.join(", ")
            ));
        }

        Ok(sql)
    }

    fn get_file_extension(&self, format: &str) -> String {
        match format {
            "csv" => ".csv",
            "xml" => ".xml",
            "html" => ".html",
            "insert" => ".sql",
            "yaml" => ".yaml",
            _ => ".json",
        }.to_string()
    }

    fn write_to_file(&self, out_dir: &str, table_name: &str, ext: &str, content: &str) -> Result<()> {
        std::fs::create_dir_all(out_dir)?;
        let file_path = Path::new(out_dir).join(format!("{}{}", table_name, ext));
        let mut file = File::create(file_path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    async fn fetch_table_data(
        &self,
        pool: &PgPool,
        table_name: &str,
        columns: &[ColumnConfig],
    ) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        let (fields, _) = self.build_fields(columns);
        let query = self.build_select_query(table_name, &fields);
        
        let rows = sqlx::query(&query)
            .fetch_all(pool)
            .await
            .context("Failed to fetch table data")?;

        let mut records = Vec::new();
        for row in rows {
            let mut record = HashMap::new();
            for (i, field) in fields.iter().enumerate() {
                let column_config = columns.iter().find(|c| c.column_name == *field)
                    .context("Column configuration not found")?;
                let value = self.extract_typed_value(&row, i, column_config);
                record.insert(field.clone(), value);
            }
            records.push(record);
        }

        Ok(records)
    }

    fn compare_and_generate_sql(
        &self,
        table_name: &str,
        source_data: &[HashMap<String, serde_json::Value>],
        target_data: &[HashMap<String, serde_json::Value>],
        columns: &[ColumnConfig],
        primary_keys: &[String],
    ) -> Vec<String> {
        let mut sql_statements = Vec::new();

        // Create maps for efficient lookup
        let mut source_map: HashMap<String, &HashMap<String, serde_json::Value>> = HashMap::new();
        let mut target_map: HashMap<String, &HashMap<String, serde_json::Value>> = HashMap::new();

        for record in source_data {
            let key = self.generate_key(record, primary_keys);
            source_map.insert(key, record);
        }

        for record in target_data {
            let key = self.generate_key(record, primary_keys);
            target_map.insert(key, record);
        }

        // Generate INSERT and UPDATE queries
        for target_record in target_data {
            let key = self.generate_key(target_record, primary_keys);
            
            if let Some(source_record) = source_map.get(&key) {
                // Record exists in both - check for updates
                if let Some(update_sql) = self.generate_update_sql(table_name, source_record, target_record, columns, primary_keys) {
                    sql_statements.push(update_sql);
                }
            } else {
                // Record exists in target but not source - INSERT
                let insert_sql = self.generate_insert_sql(table_name, target_record);
                sql_statements.push(insert_sql);
            }
        }

        // Generate DELETE queries
        for source_record in source_data {
            let key = self.generate_key(source_record, primary_keys);
            if !target_map.contains_key(&key) {
                let delete_sql = self.generate_delete_sql(table_name, source_record, primary_keys);
                sql_statements.push(delete_sql);
            }
        }

        sql_statements
    }

    fn generate_key(&self, record: &HashMap<String, serde_json::Value>, primary_keys: &[String]) -> String {
        primary_keys.iter()
            .map(|key| record.get(key).unwrap_or(&serde_json::Value::Null).to_string())
            .collect::<Vec<_>>()
            .join("|")
    }

    fn generate_insert_sql(&self, table_name: &str, record: &HashMap<String, serde_json::Value>) -> String {
        let columns: Vec<String> = record.keys().cloned().collect();
        let values: Vec<String> = columns.iter()
            .map(|col| {
                let value = record.get(col).unwrap();
                match value {
                    serde_json::Value::String(s) => format!("'{}'", s.replace("'", "''")),
                    serde_json::Value::Null => "NULL".to_string(),
                    _ => value.to_string(),
                }
            })
            .collect();

        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            columns.join(", "),
            values.join(", ")
        )
    }

    fn generate_update_sql(
        &self,
        table_name: &str,
        source_record: &HashMap<String, serde_json::Value>,
        target_record: &HashMap<String, serde_json::Value>,
        columns: &[ColumnConfig],
        primary_keys: &[String],
    ) -> Option<String> {
        let mut updates = Vec::new();

        for column in columns {
            if column.is_track == "true" {
                let column_name = &column.column_name;
                let source_value = source_record.get(column_name);
                let target_value = target_record.get(column_name);

                if source_value != target_value {
                    let formatted_value = match target_value {
                        Some(serde_json::Value::String(s)) => format!("'{}'", s.replace("'", "''")),
                        Some(serde_json::Value::Null) | None => "NULL".to_string(),
                        Some(serde_json::Value::Number(n)) => n.to_string(),
                        Some(serde_json::Value::Bool(b)) => if *b { "'Y'".to_string() } else { "'N'".to_string() },
                        Some(v) => v.to_string(),
                    };
                    updates.push(format!("{} = {}", column_name, formatted_value));
                }
            }
        }

        if updates.is_empty() {
            return None;
        }

        let where_clause = primary_keys.iter()
            .map(|key| {
                let value = source_record.get(key).unwrap();
                match value {
                    serde_json::Value::String(s) => format!("{} = '{}'", key, s.replace("'", "''")),
                    serde_json::Value::Null => format!("{} IS NULL", key),
                    _ => format!("{} = {}", key, value),
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        Some(format!(
            "UPDATE {} SET {} WHERE {}",
            table_name,
            updates.join(", "),
            where_clause
        ))
    }

    fn generate_delete_sql(&self, table_name: &str, record: &HashMap<String, serde_json::Value>, primary_keys: &[String]) -> String {
        let where_clause = primary_keys.iter()
            .map(|key| {
                let value = record.get(key).unwrap();
                match value {
                    serde_json::Value::String(s) => format!("{} = '{}'", key, s.replace("'", "''")),
                    serde_json::Value::Null => format!("{} IS NULL", key),
                    _ => format!("{} = {}", key, value),
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        format!("DELETE FROM {} WHERE {}", table_name, where_clause)
    }

    fn extract_typed_value(&self, row: &sqlx::postgres::PgRow, index: usize, column_config: &ColumnConfig) -> serde_json::Value {
        use sqlx::Row;
        
        // First check if the column should be tracked or use default
        if column_config.is_track != "true" {
            // Use default value for non-tracked columns
            return match column_config.column_type.as_deref() {
                Some("numeric") | Some("integer") | Some("bigint") | Some("decimal") | Some("real") | Some("double") => {
                    let default_str = column_config.default.as_ref().cloned().unwrap_or_else(|| "0".to_string());
                    if default_str == "CURRENT_TIMESTAMP" {
                        // Generate current timestamp in milliseconds for timestamp columns
                        let current_timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;
                        serde_json::Value::Number(serde_json::Number::from(current_timestamp))
                    } else if let Ok(decimal) = BigDecimal::from_str(&default_str) {
                        // Use BigDecimal for precise parsing
                        let decimal_str = decimal.to_string();
                        if !decimal_str.contains('.') || decimal_str.ends_with(".0") {
                            // Integer value
                            if let Ok(int_val) = decimal_str.parse::<i64>() {
                                serde_json::Value::Number(serde_json::Number::from(int_val))
                            } else {
                                serde_json::Value::String(decimal_str)
                            }
                        } else {
                            // Has decimal places
                            if let Ok(float_val) = decimal_str.parse::<f64>() {
                                serde_json::Number::from_f64(float_val)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::String(decimal_str))
                            } else {
                                serde_json::Value::String(decimal_str)
                            }
                        }
                    } else {
                        serde_json::Value::String(default_str)
                    }
                }
                _ => {
                    let default_str = column_config.default.as_ref().cloned().unwrap_or_else(|| "".to_string());
                    serde_json::Value::String(default_str)
                }
            };
        }

        // For tracked columns, extract the actual value with proper typing
        match column_config.data_type.as_str() {
            "integer" | "int4" | "serial" => {
                match row.try_get::<Option<i32>, _>(index) {
                    Ok(Some(val)) => serde_json::Value::Number(serde_json::Number::from(val)),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
            "bigint" | "int8" | "bigserial" => {
                match row.try_get::<Option<i64>, _>(index) {
                    Ok(Some(val)) => serde_json::Value::Number(serde_json::Number::from(val)),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
            "smallint" | "int2" | "smallserial" => {
                match row.try_get::<Option<i16>, _>(index) {
                    Ok(Some(val)) => serde_json::Value::Number(serde_json::Number::from(val)),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
            "real" | "float4" => {
                match row.try_get::<Option<f32>, _>(index) {
                    Ok(Some(val)) => {
                        serde_json::Number::from_f64(val as f64)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    }
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
            "double precision" | "float8" => {
                match row.try_get::<Option<f64>, _>(index) {
                    Ok(Some(val)) => {
                        serde_json::Number::from_f64(val)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    }
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
            "numeric" | "decimal" => {
                // Handle NUMERIC/DECIMAL with BigDecimal for precision
                match row.try_get::<Option<BigDecimal>, _>(index) {
                    Ok(Some(decimal)) => {
                        // Convert BigDecimal to appropriate JSON representation
                        let decimal_str = decimal.to_string();
                        
                        // Check if it's an integer by looking for decimal point
                        if !decimal_str.contains('.') || decimal_str.ends_with(".0") {
                            // Integer value
                            if let Ok(int_val) = decimal_str.parse::<i64>() {
                                serde_json::Value::Number(serde_json::Number::from(int_val))
                            } else {
                                // Large integer that doesn't fit in i64
                                serde_json::Value::String(decimal_str)
                            }
                        } else {
                            // Has decimal places - try as f64, fallback to string for precision
                            if let Ok(float_val) = decimal_str.parse::<f64>() {
                                serde_json::Number::from_f64(float_val)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::String(decimal_str))
                            } else {
                                serde_json::Value::String(decimal_str)
                            }
                        }
                    }
                    Ok(None) => {
                        // Use configured default value for non-tracked columns
                        if column_config.is_track != "true" {
                            if let Some(default_val) = &column_config.default {
                                if let Ok(num) = default_val.parse::<i64>() {
                                    serde_json::Value::Number(serde_json::Number::from(num))
                                } else {
                                    serde_json::Value::String(default_val.clone())
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    Err(_) => {
                        // Use configured default value for non-tracked columns
                        if column_config.is_track != "true" {
                            if let Some(default_val) = &column_config.default {
                                if let Ok(num) = default_val.parse::<i64>() {
                                    serde_json::Value::Number(serde_json::Number::from(num))
                                } else {
                                    serde_json::Value::String(default_val.clone())
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        } else {
                            serde_json::Value::Null
                        }
                    }
                }
            }
            "boolean" | "bool" => {
                match row.try_get::<Option<bool>, _>(index) {
                    Ok(Some(val)) => serde_json::Value::Bool(val),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
            "timestamp" | "timestamptz" | "date" | "time" | "timetz" => {
                // Handle timestamps as strings for consistency
                match row.try_get::<Option<String>, _>(index) {
                    Ok(Some(val)) => serde_json::Value::String(val),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
            _ => {
                // Default to string for text, varchar, char, etc.
                match row.try_get::<Option<String>, _>(index) {
                    Ok(Some(val)) => serde_json::Value::String(val),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::DatabaseMeta { format, config, out } => {
            let config_content = std::fs::read_to_string(&config)
                .context("Failed to read config file")?;
            let config: Config = serde_yaml::from_str(&config_content)
                .context("Failed to parse config file")?;

            let processor = DatabaseMetaProcessor::new(config);
            let result = processor.database_meta(format, out).await?;
            println!("{}", result);
        }
        Commands::CompareTables { config } => {
            let config_content = std::fs::read_to_string(&config)
                .context("Failed to read config file")?;
            let config: Config = serde_yaml::from_str(&config_content)
                .context("Failed to parse config file")?;

            let processor = DatabaseMetaProcessor::new(config);
            let result = processor.compare_tables().await?;
            println!("{}", result);
        }
    }

    Ok(())
}
