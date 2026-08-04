//! HATEOAS response envelope (Richardson Maturity Model Level 3).

use std::collections::HashMap;

use serde::Serialize;
use teodb_core::problem::Link;

/// Response envelope that includes HATEOAS links.
#[derive(Debug, Serialize)]
pub struct HateoasResponse<T: Serialize> {
    #[serde(flatten)]
    pub data: T,

    #[serde(rename = "_links")]
    pub links: HashMap<String, Link>,
}

impl<T: Serialize> HateoasResponse<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            links: HashMap::new(),
        }
    }

    pub fn with_link(mut self, rel: impl Into<String>, link: Link) -> Self {
        self.links.insert(rel.into(), link);
        self
    }
}
