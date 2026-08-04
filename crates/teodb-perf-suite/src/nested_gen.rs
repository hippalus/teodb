//! Generator for nested JSON e-commerce event data.
//!
//! Produces deterministic, partition-friendly event streams for benchmarking
//! nested JSON ingest and partition-pruning queries. Output is written as
//! numbered batch files (`events_000.json`, `events_001.json`, …) each
//! containing a JSON array of row objects.

use std::fs;
use std::path::{Path, PathBuf};

use color_eyre::eyre::Result;
use rand::{Rng, RngExt, SeedableRng};

const BATCH_SIZE: usize = 10_000;

const REGIONS: [&str; 6] = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"];

const EVENT_TYPES: [&str; 5] = [
    "order_placed",
    "order_shipped",
    "order_delivered",
    "order_cancelled",
    "order_returned",
];

const CUSTOMER_TIERS: [&str; 4] = ["basic", "standard", "premium", "enterprise"];

const PAYMENT_METHODS: [&str; 5] = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"];

const SHIPPING_METHODS: [&str; 4] = ["standard", "express", "overnight", "pickup"];

const SOURCES: [&str; 4] = ["web", "mobile", "api", "pos"];

const FIRST_NAMES: [&str; 10] = [
    "Alice", "Bob", "Carlos", "Diana", "Erik", "Fatima", "George", "Hannah", "Ivan", "Julia",
];

const LAST_NAMES: [&str; 10] = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Wilson",
];

const CITIES: [(&str, &str, &str); 12] = [
    ("New York", "NY", "US"),
    ("Los Angeles", "CA", "US"),
    ("London", "LDN", "GB"),
    ("Berlin", "BE", "DE"),
    ("Frankfurt", "HE", "DE"),
    ("Paris", "IDF", "FR"),
    ("Mumbai", "MH", "IN"),
    ("Tokyo", "TK", "JP"),
    ("Singapore", "SG", "SG"),
    ("Sydney", "NSW", "AU"),
    ("Toronto", "ON", "CA"),
    ("Sao Paulo", "SP", "BR"),
];

const CURRENCIES: [&str; 4] = ["USD", "EUR", "GBP", "JPY"];

/// 2024-01-01T00:00:00Z as Unix timestamp (seconds).
const TS_START: i64 = 1_704_067_200;
/// 2024-12-31T23:59:59Z as Unix timestamp (seconds).
const TS_END: i64 = 1_735_689_599;

/// Generate nested JSON event data as numbered batch files.
///
/// Returns the directory containing the generated files.
pub fn generate_nested_events(output_path: &Path, total_rows: usize) -> Result<PathBuf> {
    fs::create_dir_all(output_path)?;

    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let num_batches = total_rows.div_ceil(BATCH_SIZE);
    let mut rows_remaining = total_rows;
    let mut global_idx: usize = 0;

    for batch_idx in 0..num_batches {
        let batch_rows = rows_remaining.min(BATCH_SIZE);
        rows_remaining -= batch_rows;

        let mut batch = Vec::with_capacity(batch_rows);
        for _ in 0..batch_rows {
            batch.push(generate_event(&mut rng, global_idx));
            global_idx += 1;
        }

        let filename = format!("events_{batch_idx:03}.json");
        let path = output_path.join(&filename);
        let json = serde_json::to_vec(&batch)?;
        fs::write(&path, json)?;

        if (batch_idx + 1) % 10 == 0 || batch_idx + 1 == num_batches {
            println!(
                "  generated batch {}/{num_batches} ({global_idx}/{total_rows} rows)",
                batch_idx + 1
            );
        }
    }

    Ok(output_path.to_path_buf())
}

fn generate_event(rng: &mut impl Rng, idx: usize) -> serde_json::Value {
    let region = REGIONS[rng.random_range(0..REGIONS.len())];
    let event_type = EVENT_TYPES[rng.random_range(0..EVENT_TYPES.len())];
    let ts = rng.random_range(TS_START..=TS_END);
    let timestamp = format_timestamp(ts);

    let customer_id = rng.random_range(1..=500_000_i64);
    let first = FIRST_NAMES[rng.random_range(0..FIRST_NAMES.len())];
    let last = LAST_NAMES[rng.random_range(0..LAST_NAMES.len())];
    let tier = CUSTOMER_TIERS[rng.random_range(0..CUSTOMER_TIERS.len())];
    let (city, state, country) = CITIES[rng.random_range(0..CITIES.len())];

    let total_amount = (rng.random_range(500..500_000_i64) as f64) / 100.0;
    let currency = CURRENCIES[rng.random_range(0..CURRENCIES.len())];
    let items_count = rng.random_range(1..=20_i32);
    let payment_method = PAYMENT_METHODS[rng.random_range(0..PAYMENT_METHODS.len())];
    let shipping_method = SHIPPING_METHODS[rng.random_range(0..SHIPPING_METHODS.len())];
    let shipping_cost = (rng.random_range(0..5000_i32) as f64) / 100.0;
    let estimated_days = match shipping_method {
        "overnight" => 1,
        "express" => rng.random_range(2..=3),
        "standard" => rng.random_range(5..=10),
        "pickup" => 0,
        _ => 7,
    };

    let source = SOURCES[rng.random_range(0..SOURCES.len())];
    let session_id = format!("sess-{:08x}", rng.random_range(0..0xFFFF_FFFFu32));

    // Nested JSON — the ingest handler flattens to dot-notation columns.
    serde_json::json!({
        "event_id": format!("evt-{:08}", idx + 1),
        "event_type": event_type,
        "timestamp": timestamp,
        "region": region,
        "customer": {
            "id": customer_id,
            "name": format!("{first} {last}"),
            "tier": tier,
            "address": {
                "city": city,
                "state": state,
                "country": country
            }
        },
        "order": {
            "order_id": format!("ORD-{:08}", idx + 1),
            "total_amount": total_amount,
            "currency": currency,
            "items_count": items_count,
            "payment_method": payment_method,
            "shipping": {
                "method": shipping_method,
                "cost": shipping_cost,
                "estimated_days": estimated_days
            }
        },
        "metadata": {
            "source": source,
            "user_agent": "Mozilla/5.0",
            "session_id": session_id
        }
    })
}

fn format_timestamp(epoch_secs: i64) -> String {
    let secs_per_day: i64 = 86400;
    let days_since_epoch = epoch_secs / secs_per_day;
    let time_of_day = epoch_secs % secs_per_day;

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Convert days since Unix epoch to Y-M-D using a simple calendar algorithm.
    let (year, month, day) = days_to_ymd(days_since_epoch);

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Convert days since Unix epoch (1970-01-01) to (year, month, day).
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}
