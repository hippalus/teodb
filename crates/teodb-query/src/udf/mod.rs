//! Custom scalar UDFs registered on every TeoDB session.

pub mod url_path_hash;

pub use url_path_hash::url_path_hash_udf;
