//! Encoding and decoding helpers for the Flight service.

use arrow_flight::FlightDescriptor;
use tonic::Status;

/// Encode an Arrow schema to IPC format bytes.
pub fn encode_schema(schema: &arrow::datatypes::Schema) -> Result<Vec<u8>, Status> {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let ipc: arrow_flight::IpcMessage = arrow_flight::SchemaAsIpc::new(schema, &options)
        .try_into()
        .map_err(|e: arrow::error::ArrowError| Status::internal(format!("schema IPC encoding: {e}")))?;
    Ok(ipc.0.to_vec())
}

/// Parse a flight descriptor's path into a `TableIdent`.
/// Expected format: [namespace, table_name]
pub fn parse_descriptor(desc: &FlightDescriptor) -> Result<teodb_core::ident::TableIdent, Status> {
    if desc.path.len() >= 2 {
        Ok(teodb_core::ident::TableIdent::new(&desc.path[0], &desc.path[1]))
    } else if desc.path.len() == 1 {
        // Try "namespace.table" format
        let full = &desc.path[0];
        let (ns, tbl) = full
            .split_once('.')
            .ok_or_else(|| Status::invalid_argument(format!("invalid table path: {full}")))?;
        Ok(teodb_core::ident::TableIdent::new(ns, tbl))
    } else {
        Err(Status::invalid_argument(
            "descriptor path must have at least 2 elements: [namespace, table]",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_two_element_path() {
        let desc = FlightDescriptor {
            r#type: 0,
            cmd: Default::default(),
            path: vec!["analytics".into(), "events".into()],
        };
        let ident = parse_descriptor(&desc).unwrap();
        assert_eq!(ident.namespace, "analytics");
        assert_eq!(ident.name, "events");
    }

    #[test]
    fn parse_dotted_path() {
        let desc = FlightDescriptor {
            r#type: 0,
            cmd: Default::default(),
            path: vec!["analytics.events".into()],
        };
        let ident = parse_descriptor(&desc).unwrap();
        assert_eq!(ident.namespace, "analytics");
        assert_eq!(ident.name, "events");
    }

    #[test]
    fn parse_empty_path_fails() {
        let desc = FlightDescriptor {
            r#type: 0,
            cmd: Default::default(),
            path: vec![],
        };
        assert!(parse_descriptor(&desc).is_err());
    }
}
