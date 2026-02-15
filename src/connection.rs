use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use tabby::connection::Config;
use tabby::row_writer::RowWriter;
use tabby::{Client as TdsClient, Column, ColumnType};

// ── RowWriter that collects values ─────────────────────────────────
#[derive(Default)]
struct JsRowCollector {
    columns: Vec<Column>,
    /// flat buffer: row-major
    values: Vec<JsValueWrapper>,
    cols_per_row: usize,
    rows_affected: i64,
}

impl RowWriter for JsRowCollector {
    fn on_metadata(&mut self, columns: &[Column]) {
        self.columns = columns.to_vec();
        self.cols_per_row = columns.len();
    }

    fn write_null(&mut self, _col: usize) {
        self.values.push(JsValueWrapper::Null);
    }
    fn write_bool(&mut self, _col: usize, v: bool) {
        self.values.push(JsValueWrapper::Bool(v));
    }
    fn write_u8(&mut self, _col: usize, v: u8) {
        self.values.push(JsValueWrapper::I64(v as i64));
    }
    fn write_i16(&mut self, _col: usize, v: i16) {
        self.values.push(JsValueWrapper::I64(v as i64));
    }
    fn write_i32(&mut self, _col: usize, v: i32) {
        self.values.push(JsValueWrapper::I64(v as i64));
    }
    fn write_i64(&mut self, _col: usize, v: i64) {
        self.values.push(JsValueWrapper::I64(v));
    }
    fn write_f32(&mut self, _col: usize, v: f32) {
        self.values.push(JsValueWrapper::F64(v as f64));
    }
    fn write_f64(&mut self, _col: usize, v: f64) {
        self.values.push(JsValueWrapper::F64(v));
    }
    fn write_str(&mut self, _col: usize, v: &str) {
        self.values.push(JsValueWrapper::Str(v.to_owned()));
    }
    fn write_bytes(&mut self, _col: usize, v: &[u8]) {
        self.values.push(JsValueWrapper::Bytes(v.to_owned()));
    }
    fn write_guid(&mut self, _col: usize, v: &[u8; 16]) {
        let u = uuid::Uuid::from_bytes(*v);
        self.values.push(JsValueWrapper::Str(u.to_string()));
    }
    fn write_decimal(&mut self, _col: usize, value: i128, _precision: u8, scale: u8) {
        self.values
            .push(JsValueWrapper::Str(crate::types::decimal_to_string(
                value, scale,
            )));
    }
    fn write_date(&mut self, _col: usize, unix_days: i32) {
        self.values
            .push(JsValueWrapper::Str(crate::types::unix_days_to_iso(
                unix_days,
            )));
    }
    fn write_time(&mut self, _col: usize, nanos: i64) {
        self.values
            .push(JsValueWrapper::Str(crate::types::nanos_to_time_str(
                nanos as u64,
            )));
    }
    fn write_datetime(&mut self, _col: usize, micros: i64) {
        self.values
            .push(JsValueWrapper::Str(crate::types::micros_to_iso(micros)));
    }
    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, offset_minutes: i16) {
        self.values
            .push(JsValueWrapper::Str(crate::types::micros_offset_to_iso(
                micros,
                offset_minutes,
            )));
    }
    fn on_done(&mut self, rows: u64) {
        self.rows_affected = rows as i64;
    }
}

// ── Fast binary-encoded collector ───────────────────────────────────
// Tags: 0=null, 1=false, 2=true, 3=f64, 4=i64(bigint), 5=string_ref, 6=bytes
const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_F64: u8 = 3;
const TAG_BIGINT: u8 = 4;
const TAG_STRING_REF: u8 = 5;
const TAG_BYTES: u8 = 6;

struct FastRowCollector {
    columns: Vec<Column>,
    cols_per_row: usize,
    rows_affected: i64,
    row_count: usize,
    // Cell data written directly to buffer
    cell_buf: Vec<u8>,
    // String interning
    string_table: Vec<String>,
    string_map: HashMap<String, u32>,
}

impl Default for FastRowCollector {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            cols_per_row: 0,
            rows_affected: 0,
            row_count: 0,
            cell_buf: Vec::with_capacity(1024 * 1024),
            string_table: Vec::with_capacity(4096),
            string_map: HashMap::with_capacity(4096),
        }
    }
}

impl FastRowCollector {
    #[inline(always)]
    fn intern_string(&mut self, s: &str) -> u32 {
        if let Some(&idx) = self.string_map.get(s) {
            return idx;
        }
        let idx = self.string_table.len() as u32;
        self.string_map.insert(s.to_owned(), idx);
        self.string_table.push(s.to_owned());
        idx
    }

    fn encode(&self) -> Vec<u8> {
        // Estimate size
        let mut buf = Vec::with_capacity(
            20 + self.columns.len() * 40
                + self.string_table.iter().map(|s| s.len() + 4).sum::<usize>()
                + self.cell_buf.len(),
        );

        // Header: col_count(u32) + row_count(u32) + string_table_len(u32) + rows_affected(i64)
        buf.extend_from_slice(&(self.cols_per_row as u32).to_le_bytes());
        buf.extend_from_slice(&(self.row_count as u32).to_le_bytes());
        buf.extend_from_slice(&(self.string_table.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.rows_affected.to_le_bytes());

        // Column definitions: type_tag(u8) + name_len(u16) + name_bytes
        for col in &self.columns {
            buf.push(col_type_id(col.column_type()));
            let name = col.name();
            buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
        }

        // String table: len(u32) + bytes for each
        for s in &self.string_table {
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }

        // Cell data (already encoded)
        buf.extend_from_slice(&self.cell_buf);

        buf
    }
}

impl RowWriter for FastRowCollector {
    fn on_metadata(&mut self, columns: &[Column]) {
        self.columns = columns.to_vec();
        self.cols_per_row = columns.len();
    }

    fn write_null(&mut self, _col: usize) {
        self.cell_buf.push(TAG_NULL);
    }
    fn write_bool(&mut self, _col: usize, v: bool) {
        self.cell_buf.push(if v { TAG_TRUE } else { TAG_FALSE });
    }
    fn write_u8(&mut self, _col: usize, v: u8) {
        self.cell_buf.push(TAG_F64);
        self.cell_buf.extend_from_slice(&(v as f64).to_le_bytes());
    }
    fn write_i16(&mut self, _col: usize, v: i16) {
        self.cell_buf.push(TAG_F64);
        self.cell_buf.extend_from_slice(&(v as f64).to_le_bytes());
    }
    fn write_i32(&mut self, _col: usize, v: i32) {
        self.cell_buf.push(TAG_F64);
        self.cell_buf.extend_from_slice(&(v as f64).to_le_bytes());
    }
    fn write_i64(&mut self, _col: usize, v: i64) {
        if v.unsigned_abs() <= (1u64 << 53) {
            self.cell_buf.push(TAG_F64);
            self.cell_buf.extend_from_slice(&(v as f64).to_le_bytes());
        } else {
            self.cell_buf.push(TAG_BIGINT);
            self.cell_buf.extend_from_slice(&v.to_le_bytes());
        }
    }
    fn write_f32(&mut self, _col: usize, v: f32) {
        self.cell_buf.push(TAG_F64);
        self.cell_buf.extend_from_slice(&(v as f64).to_le_bytes());
    }
    fn write_f64(&mut self, _col: usize, v: f64) {
        self.cell_buf.push(TAG_F64);
        self.cell_buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_str(&mut self, _col: usize, v: &str) {
        let idx = self.intern_string(v);
        self.cell_buf.push(TAG_STRING_REF);
        self.cell_buf.extend_from_slice(&idx.to_le_bytes());
    }
    fn write_bytes(&mut self, _col: usize, v: &[u8]) {
        self.cell_buf.push(TAG_BYTES);
        self.cell_buf
            .extend_from_slice(&(v.len() as u32).to_le_bytes());
        self.cell_buf.extend_from_slice(v);
    }
    fn write_guid(&mut self, _col: usize, v: &[u8; 16]) {
        let u = uuid::Uuid::from_bytes(*v);
        let s = u.to_string();
        let idx = self.intern_string(&s);
        self.cell_buf.push(TAG_STRING_REF);
        self.cell_buf.extend_from_slice(&idx.to_le_bytes());
    }
    fn write_decimal(&mut self, _col: usize, value: i128, _precision: u8, scale: u8) {
        let s = crate::types::decimal_to_string(value, scale);
        let idx = self.intern_string(&s);
        self.cell_buf.push(TAG_STRING_REF);
        self.cell_buf.extend_from_slice(&idx.to_le_bytes());
    }
    fn write_date(&mut self, _col: usize, unix_days: i32) {
        let s = crate::types::unix_days_to_iso(unix_days);
        let idx = self.intern_string(&s);
        self.cell_buf.push(TAG_STRING_REF);
        self.cell_buf.extend_from_slice(&idx.to_le_bytes());
    }
    fn write_time(&mut self, _col: usize, nanos: i64) {
        let s = crate::types::nanos_to_time_str(nanos as u64);
        let idx = self.intern_string(&s);
        self.cell_buf.push(TAG_STRING_REF);
        self.cell_buf.extend_from_slice(&idx.to_le_bytes());
    }
    fn write_datetime(&mut self, _col: usize, micros: i64) {
        let s = crate::types::micros_to_iso(micros);
        let idx = self.intern_string(&s);
        self.cell_buf.push(TAG_STRING_REF);
        self.cell_buf.extend_from_slice(&idx.to_le_bytes());
    }
    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, offset_minutes: i16) {
        let s = crate::types::micros_offset_to_iso(micros, offset_minutes);
        let idx = self.intern_string(&s);
        self.cell_buf.push(TAG_STRING_REF);
        self.cell_buf.extend_from_slice(&idx.to_le_bytes());
    }
    fn on_done(&mut self, rows: u64) {
        self.rows_affected = rows as i64;
        self.row_count = rows as usize;
    }
}

fn col_type_id(ct: ColumnType) -> u8 {
    match ct {
        ColumnType::Null => 0,
        ColumnType::Bit | ColumnType::Bitn => 1,
        ColumnType::Int1 => 2,
        ColumnType::Int2 => 3,
        ColumnType::Int4 => 4,
        ColumnType::Int8 => 5,
        ColumnType::Intn => 6,
        ColumnType::Float4 => 7,
        ColumnType::Float8 => 8,
        ColumnType::Floatn => 9,
        ColumnType::Datetime
        | ColumnType::Datetime2
        | ColumnType::Datetime4
        | ColumnType::Datetimen => 10,
        ColumnType::DatetimeOffsetn => 11,
        ColumnType::Daten => 12,
        ColumnType::Timen => 13,
        ColumnType::Decimaln | ColumnType::Numericn => 14,
        ColumnType::Guid => 15,
        ColumnType::NVarchar | ColumnType::NChar | ColumnType::NText => 16,
        ColumnType::BigVarChar | ColumnType::BigChar | ColumnType::Text => 17,
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => 18,
        ColumnType::Xml => 19,
        ColumnType::Money | ColumnType::Money4 => 20,
        ColumnType::Udt => 21,
        ColumnType::SSVariant => 22,
    }
}

#[allow(dead_code)]
static COL_TYPE_NAMES: &[&str] = &[
    "null",
    "bit",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "int",
    "real",
    "float",
    "float",
    "datetime",
    "datetimeoffset",
    "date",
    "time",
    "decimal",
    "uniqueidentifier",
    "nvarchar",
    "varchar",
    "varbinary",
    "xml",
    "money",
    "udt",
    "sql_variant",
];

// ── QueryResult: returned to JS ────────────────────────────────────
#[napi(object)]
pub struct QueryResult {
    pub rows: Vec<Vec<JsValueWrapper>>,
    pub columns: Vec<ColumnInfo>,
    pub row_count: i64,
}

#[napi(object)]
pub struct ColumnInfo {
    pub name: String,
    pub r#type: String,
}

// Wrapper to pass values through napi
pub enum JsValueWrapper {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
}

impl ToNapiValue for JsValueWrapper {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        match val {
            JsValueWrapper::Null => {
                // SAFETY: napi call
                unsafe { <()>::to_napi_value(env, ()) }
            }
            JsValueWrapper::Bool(v) => unsafe { bool::to_napi_value(env, v) },
            JsValueWrapper::I64(v) => {
                // Use f64 for safe integer range
                if v.unsigned_abs() <= (1u64 << 53) {
                    unsafe { f64::to_napi_value(env, v as f64) }
                } else {
                    unsafe { i64::to_napi_value(env, v) }
                }
            }
            JsValueWrapper::F64(v) => unsafe { f64::to_napi_value(env, v) },
            JsValueWrapper::Str(v) => unsafe { String::to_napi_value(env, v) },
            JsValueWrapper::Bytes(v) => unsafe { Buffer::to_napi_value(env, v.into()) },
        }
    }
}

impl FromNapiValue for JsValueWrapper {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        napi_val: napi::sys::napi_value,
    ) -> Result<Self> {
        // Determine JS type
        let mut value_type = 0;
        unsafe {
            napi::sys::napi_typeof(env, napi_val, &mut value_type);
        }
        match value_type {
            napi::sys::ValueType::napi_null | napi::sys::ValueType::napi_undefined => {
                Ok(JsValueWrapper::Null)
            }
            napi::sys::ValueType::napi_boolean => {
                let v = unsafe { bool::from_napi_value(env, napi_val)? };
                Ok(JsValueWrapper::Bool(v))
            }
            napi::sys::ValueType::napi_number => {
                let v = unsafe { f64::from_napi_value(env, napi_val)? };
                // If it's an integer, store as i64
                if v.fract() == 0.0 && v.abs() < (i64::MAX as f64) {
                    Ok(JsValueWrapper::I64(v as i64))
                } else {
                    Ok(JsValueWrapper::F64(v))
                }
            }
            napi::sys::ValueType::napi_string => {
                let v = unsafe { String::from_napi_value(env, napi_val)? };
                Ok(JsValueWrapper::Str(v))
            }
            _ => {
                // Try as buffer
                let mut is_buffer = false;
                unsafe {
                    napi::sys::napi_is_buffer(env, napi_val, &mut is_buffer);
                }
                if is_buffer {
                    let v = unsafe { Buffer::from_napi_value(env, napi_val)? };
                    Ok(JsValueWrapper::Bytes(v.to_vec()))
                } else {
                    // Fallback: coerce to string
                    let v = unsafe { String::from_napi_value(env, napi_val)? };
                    Ok(JsValueWrapper::Str(v))
                }
            }
        }
    }
}

impl ValidateNapiValue for JsValueWrapper {}

impl napi::bindgen_prelude::TypeName for JsValueWrapper {
    fn type_name() -> &'static str {
        "JsValueWrapper"
    }
    fn value_type() -> napi::ValueType {
        napi::ValueType::Unknown
    }
}

// Parse connection string into tabby Config
fn parse_conn_str(s: &str) -> Result<Config> {
    let mut server = "localhost".to_string();
    let mut port: u16 = 1433;
    let mut database = "master".to_string();
    let mut user = String::new();
    let mut password = String::new();
    let mut trust_cert = false;

    for part in s.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((key, val)) = part.split_once('=') {
            let key = key.trim().to_lowercase();
            let val = val.trim();
            match key.as_str() {
                "server" | "data source" => {
                    if let Some((h, p)) = val.rsplit_once(',') {
                        server = h.to_string();
                        port = p
                            .parse()
                            .map_err(|_| Error::from_reason("Invalid port in connection string"))?;
                    } else {
                        server = val.to_string();
                    }
                }
                "database" | "initial catalog" => database = val.to_string(),
                "uid" | "user id" | "user" => user = val.to_string(),
                "pwd" | "password" => password = val.to_string(),
                "trustservercertificate" => {
                    trust_cert = val.eq_ignore_ascii_case("yes") || val.eq_ignore_ascii_case("true")
                }
                _ => {} // ignore unknown keys
            }
        }
    }

    let mut config = Config::new();
    config.host(&server);
    config.port(port);
    config.database(&database);
    config.authentication(tabby::AuthMethod::sql_server(user, password));
    if trust_cert {
        config.trust_cert();
    }

    Ok(config)
}

fn col_type_name(ct: ColumnType) -> &'static str {
    match ct {
        ColumnType::Null => "null",
        ColumnType::Bit | ColumnType::Bitn => "bit",
        ColumnType::Int1 => "tinyint",
        ColumnType::Int2 => "smallint",
        ColumnType::Int4 => "int",
        ColumnType::Int8 => "bigint",
        ColumnType::Intn => "int",
        ColumnType::Float4 => "real",
        ColumnType::Float8 => "float",
        ColumnType::Floatn => "float",
        ColumnType::Datetime
        | ColumnType::Datetime2
        | ColumnType::Datetime4
        | ColumnType::Datetimen => "datetime",
        ColumnType::DatetimeOffsetn => "datetimeoffset",
        ColumnType::Daten => "date",
        ColumnType::Timen => "time",
        ColumnType::Decimaln | ColumnType::Numericn => "decimal",
        ColumnType::Guid => "uniqueidentifier",
        ColumnType::NVarchar | ColumnType::NChar | ColumnType::NText => "nvarchar",
        ColumnType::BigVarChar | ColumnType::BigChar | ColumnType::Text => "varchar",
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => "varbinary",
        ColumnType::Xml => "xml",
        ColumnType::Money | ColumnType::Money4 => "money",
        ColumnType::Udt => "udt",
        ColumnType::SSVariant => "sql_variant",
    }
}

// ── Client ─────────────────────────────────────────────────────────
type InnerClient = TdsClient<tokio_util::compat::Compat<TcpStream>>;

#[napi]
pub struct Client {
    config: Config,
    inner: Arc<Mutex<Option<InnerClient>>>,
}

#[napi]
impl Client {
    #[napi(constructor)]
    pub fn new(connection_string: String) -> Result<Self> {
        let config = parse_conn_str(&connection_string)?;
        Ok(Client {
            config,
            inner: Arc::new(Mutex::new(None)),
        })
    }

    #[napi]
    pub async fn connect(&self) -> Result<()> {
        let config = self.config.clone();

        let client = TdsClient::connect_with_redirect(config, |host, port| async move {
            let addr = format!("{}:{}", host, port);
            let tcp = TcpStream::connect(&addr)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            tcp.set_nodelay(true)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            Ok(tcp.compat_write())
        })
        .await
        .map_err(|e| Error::from_reason(format!("Connection failed: {e}")))?;

        *self.inner.lock().await = Some(client);
        Ok(())
    }

    #[napi]
    pub async fn query(
        &self,
        sql: String,
        params: Option<Vec<JsValueWrapper>>,
    ) -> Result<QueryResult> {
        let inner = self.inner.clone();
        let mut guard = inner.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("Not connected. Call connect() first."))?;

        let mut writer = JsRowCollector::default();

        // Inline params into SQL
        let final_sql = if let Some(ref p) = params {
            if p.is_empty() {
                sql.clone()
            } else {
                substitute_params(&sql, p)?
            }
        } else {
            sql.clone()
        };

        client
            .batch_into(&final_sql, &mut writer)
            .await
            .map_err(|e| Error::from_reason(format!("Query failed: {e}")))?;

        // Convert results
        let cols_per_row = writer.cols_per_row;
        let num_rows = if cols_per_row > 0 {
            writer.values.len() / cols_per_row
        } else {
            0
        };

        let columns: Vec<ColumnInfo> = writer
            .columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name().to_string(),
                r#type: col_type_name(c.column_type()).to_string(),
            })
            .collect();

        let mut rows = Vec::with_capacity(num_rows);
        for r in 0..num_rows {
            let base = r * cols_per_row;
            let mut row = Vec::with_capacity(cols_per_row);
            for c in 0..cols_per_row {
                let idx = base + c;
                // Take ownership to avoid clone
                row.push(std::mem::replace(
                    &mut writer.values[idx],
                    JsValueWrapper::Null,
                ));
            }
            rows.push(row);
        }

        Ok(QueryResult {
            rows,
            columns,
            row_count: num_rows as i64,
        })
    }

    #[napi]
    pub async fn execute(&self, sql: String, params: Option<Vec<JsValueWrapper>>) -> Result<i64> {
        let inner = self.inner.clone();
        let mut guard = inner.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("Not connected. Call connect() first."))?;

        let mut writer = JsRowCollector::default();

        let final_sql = if let Some(ref p) = params {
            if p.is_empty() {
                sql.clone()
            } else {
                substitute_params(&sql, p)?
            }
        } else {
            sql.clone()
        };

        client
            .batch_into(&final_sql, &mut writer)
            .await
            .map_err(|e| Error::from_reason(format!("Execute failed: {e}")))?;

        Ok(writer.rows_affected)
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        *self.inner.lock().await = None;
        Ok(())
    }

    /// Alias for close()
    #[napi]
    pub async fn end(&self) -> Result<()> {
        self.close().await
    }

    /// Fast query returning binary-encoded buffer for JS-side decoding
    #[napi]
    pub async fn query_raw(
        &self,
        sql: String,
        params: Option<Vec<JsValueWrapper>>,
    ) -> Result<Buffer> {
        let inner = self.inner.clone();
        let mut guard = inner.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("Not connected. Call connect() first."))?;

        let mut writer = FastRowCollector::default();

        let final_sql = if let Some(ref p) = params {
            if p.is_empty() {
                sql.clone()
            } else {
                substitute_params(&sql, p)?
            }
        } else {
            sql.clone()
        };

        client
            .batch_into(&final_sql, &mut writer)
            .await
            .map_err(|e| Error::from_reason(format!("Query failed: {e}")))?;

        Ok(writer.encode().into())
    }
}

/// Substitute $1, $2 or @p1, @p2 placeholders with inline SQL literals
fn substitute_params(sql: &str, params: &[JsValueWrapper]) -> Result<String> {
    let mut result = String::with_capacity(sql.len() + params.len() * 20);
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '@' && i + 1 < chars.len() && chars[i + 1].eq_ignore_ascii_case(&'p') {
            // Parse @p1, @p2, etc.
            let start = i;
            i += 2;
            let mut num_str = String::new();
            while i < chars.len() && chars[i].is_ascii_digit() {
                num_str.push(chars[i]);
                i += 1;
            }
            if let Ok(idx) = num_str.parse::<usize>()
                && idx >= 1
                && idx <= params.len()
            {
                result.push_str(&param_to_sql(&params[idx - 1]));
                continue;
            }
            // Not a valid param ref, emit as-is
            for c in &chars[start..i] {
                result.push(*c);
            }
        } else if chars[i] == '\'' {
            // Skip string literals
            result.push(chars[i]);
            i += 1;
            while i < chars.len() {
                result.push(chars[i]);
                if chars[i] == '\'' {
                    i += 1;
                    break;
                }
                i += 1;
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    Ok(result)
}

fn param_to_sql(p: &JsValueWrapper) -> String {
    match p {
        JsValueWrapper::Null => "NULL".to_string(),
        JsValueWrapper::Bool(v) => {
            if *v {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        JsValueWrapper::I64(v) => v.to_string(),
        JsValueWrapper::F64(v) => format!("{}", v),
        JsValueWrapper::Str(v) => {
            let escaped = v.replace('\'', "''");
            format!("N'{}'", escaped)
        }
        JsValueWrapper::Bytes(v) => {
            let hex: String = v.iter().map(|b| format!("{:02X}", b)).collect();
            format!("0x{}", hex)
        }
    }
}
