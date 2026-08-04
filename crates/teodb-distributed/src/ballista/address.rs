use teodb_core::error::{TeoDBError, TeoDBResult};

/// Parsed host/port endpoint for scheduler and executor configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl HostPort {
    /// Parse either `host:port` or `http(s)://host:port` without silent defaults.
    pub fn parse(input: &str, field: &str) -> TeoDBResult<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(TeoDBError::Config(format!("{field} must not be empty")));
        }

        if trimmed.contains("://") {
            let url = url::Url::parse(trimmed)
                .map_err(|error| TeoDBError::Config(format!("invalid {field} endpoint '{trimmed}': {error}")))?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err(TeoDBError::Config(format!(
                    "{field} endpoint '{trimmed}' must use http or https when a URL scheme is provided"
                )));
            }
            if url.username() != "" || url.password().is_some() || url.query().is_some() || url.fragment().is_some() {
                return Err(TeoDBError::Config(format!(
                    "{field} endpoint '{trimmed}' must not include credentials, query, or fragment"
                )));
            }
            if url.path() != "/" {
                return Err(TeoDBError::Config(format!(
                    "{field} endpoint '{trimmed}' must not include a path"
                )));
            }
            let host = url
                .host_str()
                .filter(|host| !host.is_empty())
                .ok_or_else(|| TeoDBError::Config(format!("{field} endpoint '{trimmed}' is missing a host")))?;
            let port = url.port().ok_or_else(|| {
                TeoDBError::Config(format!("{field} endpoint '{trimmed}' must include an explicit port"))
            })?;
            return Ok(Self {
                host: host.to_owned(),
                port,
            });
        }

        if trimmed.starts_with('[') {
            return parse_bracketed_host_port(trimmed, field);
        }

        let (host, port) = trimmed
            .rsplit_once(':')
            .ok_or_else(|| TeoDBError::Config(format!("{field} endpoint '{trimmed}' must be in host:port form")))?;
        if host.is_empty() || host.contains(':') {
            return Err(TeoDBError::Config(format!(
                "{field} endpoint '{trimmed}' has an invalid host; bracket IPv6 addresses as [::1]:50050"
            )));
        }
        parse_host_and_port(host, port, trimmed, field)
    }

    /// Socket-authority form, with IPv6 hosts bracketed.
    pub fn authority(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// HTTP URL used by Ballista scheduler clients.
    pub fn http_url(&self) -> String {
        format!("http://{}", self.authority())
    }
}

fn parse_bracketed_host_port(input: &str, field: &str) -> TeoDBResult<HostPort> {
    let close = input
        .find(']')
        .ok_or_else(|| TeoDBError::Config(format!("{field} endpoint '{input}' has an unterminated IPv6 host")))?;
    let host = &input[1..close];
    let rest = &input[close + 1..];
    let port = rest
        .strip_prefix(':')
        .ok_or_else(|| TeoDBError::Config(format!("{field} endpoint '{input}' must be in [host]:port form")))?;
    parse_host_and_port(host, port, input, field)
}

fn parse_host_and_port(host: &str, port: &str, original: &str, field: &str) -> TeoDBResult<HostPort> {
    if host.trim().is_empty() {
        return Err(TeoDBError::Config(format!(
            "{field} endpoint '{original}' is missing a host"
        )));
    }
    let port = port
        .parse::<u16>()
        .map_err(|e| TeoDBError::Config(format!("{field} endpoint '{original}' has invalid port '{port}': {e}")))?;
    if port == 0 {
        return Err(TeoDBError::Config(format!(
            "{field} endpoint '{original}' must use a non-zero port"
        )));
    }
    Ok(HostPort {
        host: host.to_owned(),
        port,
    })
}
