use serde::{Deserialize, Serialize};

/// Catalog backend type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogType {
    #[default]
    Rest,
}

impl std::fmt::Display for CatalogType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rest => f.write_str("rest"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CatalogConfig {
    #[serde(rename = "type")]
    pub catalog_type: CatalogType,
    pub uri: String,
    pub warehouse: Option<String>,
    pub oauth2_credential: Option<String>,
    pub oauth2_scope: Option<String>,
    pub oauth2_server_uri: Option<String>,
    pub token: Option<String>,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            catalog_type: CatalogType::Rest,
            uri: "http://localhost:8181".into(),
            warehouse: None,
            oauth2_credential: None,
            oauth2_scope: None,
            oauth2_server_uri: None,
            token: None,
        }
    }
}
