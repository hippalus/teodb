use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Result, eyre};
use serde_json::Value;

use crate::nested_gen;
use crate::suite_config::{DatasetManifest, DatasetSourceKind};
use crate::tpch_gen;

#[derive(Debug, Clone, serde::Serialize)]
pub struct PreparedDataset {
    pub dataset: String,
    pub primary_path: PathBuf,
    pub prepared_at: DateTime<Utc>,
}

pub async fn prepare_dataset(manifest: &DatasetManifest, work_dir: &Path) -> Result<PreparedDataset> {
    let data_dir = work_dir.join("datasets").join(&manifest.name);
    fs::create_dir_all(&data_dir)?;

    let primary_path = match manifest.source_kind {
        DatasetSourceKind::ExternalParquet => prepare_external_dataset(manifest, &data_dir).await?,
        DatasetSourceKind::SyntheticManagedJson => prepare_synthetic_json_dataset(manifest, &data_dir)?,
        DatasetSourceKind::SyntheticNestedJson => prepare_nested_json_dataset(manifest, &data_dir)?,
        DatasetSourceKind::TpchGenerated => prepare_tpch_dataset(manifest, &data_dir)?,
    };

    Ok(PreparedDataset {
        dataset: manifest.name.clone(),
        primary_path,
        prepared_at: Utc::now(),
    })
}

async fn prepare_external_dataset(manifest: &DatasetManifest, data_dir: &Path) -> Result<PathBuf> {
    if let Some(source_path) = &manifest.source_path {
        let path = PathBuf::from(source_path);
        if path.exists() {
            return Ok(path);
        }
    }

    let url = manifest
        .download_url
        .as_deref()
        .ok_or_else(|| eyre!("dataset '{}' requires source_path or download_url", manifest.name))?;
    let output_name = manifest.output_name.clone().unwrap_or_else(|| {
        url.split('/')
            .next_back()
            .unwrap_or("dataset.parquet")
            .to_string()
    });
    let target = data_dir.join(output_name);
    if target.exists() && target.metadata()?.len() > 0 {
        return Ok(target);
    }

    let bytes = reqwest::get(url)
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    fs::write(&target, &bytes)?;
    Ok(target)
}

fn prepare_synthetic_json_dataset(manifest: &DatasetManifest, data_dir: &Path) -> Result<PathBuf> {
    let target = data_dir.join(
        manifest
            .output_name
            .clone()
            .unwrap_or_else(|| "synthetic_managed.json".to_string()),
    );
    if target.exists() && target.metadata()?.len() > 0 {
        return Ok(target);
    }

    let rows = manifest.rows.unwrap_or(10_000);
    let mut payload = Vec::with_capacity(rows);
    for i in 0..rows {
        payload.push(serde_json::json!({
            "timestamp": 1_700_000_000_000_000i64 + i as i64 * 60_000_000,
            "sensor_id": format!("sensor-{:04}", i % 128),
            "temperature": 18.0 + ((i % 100) as f64 / 10.0),
        }));
    }
    fs::write(&target, serde_json::to_vec_pretty(&payload)?)?;
    Ok(target)
}

fn prepare_nested_json_dataset(manifest: &DatasetManifest, data_dir: &Path) -> Result<PathBuf> {
    let marker = data_dir.join(".generated");
    if marker.exists() {
        println!("nested JSON data already generated at {}", data_dir.display());
        return Ok(data_dir.to_path_buf());
    }

    let rows = manifest.rows.unwrap_or(1_000_000);
    println!("generating nested JSON events (rows={rows}) → {}", data_dir.display());
    nested_gen::generate_nested_events(data_dir, rows)?;
    fs::write(&marker, format!("rows={rows}"))?;
    Ok(data_dir.to_path_buf())
}

fn prepare_tpch_dataset(manifest: &DatasetManifest, data_dir: &Path) -> Result<PathBuf> {
    let sf = manifest.scale_factor.unwrap_or(0.01);
    let marker = data_dir.join(".generated");
    if marker.exists() {
        println!("tpch data already generated at {}", data_dir.display());
        return Ok(data_dir.to_path_buf());
    }

    println!("generating TPC-H data (SF={sf}) → {}", data_dir.display());
    let tables = tpch_gen::generate(data_dir, sf)?;
    println!("generated {} tables: {}", tables.len(), tables.join(", "));
    fs::write(&marker, format!("sf={sf}"))?;
    Ok(data_dir.to_path_buf())
}

pub fn read_json_rows(path: &Path) -> Result<Vec<Value>> {
    let raw = fs::read_to_string(path)?;
    let rows: Vec<Value> = serde_json::from_str(&raw)?;
    Ok(rows)
}
