use std::fs;
use std::path::Path;

use color_eyre::eyre::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetSourceKind {
    ExternalParquet,
    SyntheticManagedJson,
    SyntheticNestedJson,
    TpchGenerated,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadMode {
    ExternalParquet,
    ManagedJson,
    ManagedIngest,
    FlightInsert,
}

/// Entry for one table inside a multi-table dataset.
#[derive(Debug, Clone, Deserialize)]
pub struct TableEntry {
    pub name: String,
    pub file: String,
    /// DDL statement used when load_mode is `managed_ingest`.
    pub create_sql: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetManifest {
    pub name: String,
    /// Single-table name (legacy datasets). Ignored when `tables` is present.
    pub table_name: Option<String>,
    pub source_kind: DatasetSourceKind,
    pub load_mode: LoadMode,
    pub create_sql: Option<String>,
    pub source_path: Option<String>,
    pub download_url: Option<String>,
    pub output_name: Option<String>,
    pub rows: Option<usize>,
    pub ingest_batch_size: Option<usize>,
    /// TPC-H scale factor (only for `tpch_generated`).
    pub scale_factor: Option<f64>,
    /// Multi-table dataset entries.
    pub tables: Option<Vec<TableEntry>>,
}

impl DatasetManifest {
    /// Effective single-table name for legacy datasets.
    pub fn single_table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioManifest {
    pub name: String,
    #[allow(dead_code)]
    pub dataset_manifest: String,
    pub sql_dir: String,
    pub transport: String,
    #[serde(default = "default_warmup")]
    pub warmup_runs: u32,
    #[serde(default = "default_measured")]
    pub measured_runs: u32,
}

fn default_warmup() -> u32 {
    2
}

fn default_measured() -> u32 {
    5
}

pub fn load_dataset_manifest(path: &Path) -> Result<DatasetManifest> {
    let raw = fs::read_to_string(path)?;
    Ok(toml::from_str(&raw)?)
}

pub fn load_scenario_manifest(path: &Path) -> Result<ScenarioManifest> {
    let raw = fs::read_to_string(path)?;
    Ok(toml::from_str(&raw)?)
}
