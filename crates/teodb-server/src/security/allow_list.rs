//! Allow-list authorizer: rule-based authorization loaded from configuration.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::traits::authz::{Action, Authorizer, Principal, Resource};

/// A single authorization rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// Role pattern: `"*"` matches all, `"admin"` matches exact, `"ops_*"` matches prefix.
    pub role: String,
    /// Action to match: `"*"` matches all, or exact like `"Ingest"`, `"Query"`.
    pub action: String,
    /// Resource pattern: `"*"` matches all, `"Table(ns.tbl)"`, `"Namespace(ns)"`,
    /// `"Table(*.events)"` for wildcard namespace.
    pub resource: String,
    /// Whether to allow or deny.
    #[serde(default = "default_allow")]
    pub allow: bool,
}

fn default_allow() -> bool {
    true
}

/// Authorizer that checks against a static list of rules loaded at startup.
/// Rules are evaluated most-specific-first; first match wins.
/// If no rule matches, access is denied by default.
pub struct AllowListAuthorizer {
    rules: Vec<Rule>,
}

impl AllowListAuthorizer {
    pub fn new(rules: Vec<Rule>) -> Self {
        Self { rules }
    }

    /// Load rules from a TOML file. The file should contain:
    /// ```toml
    /// [[rules]]
    /// role = "*"
    /// action = "Query"
    /// resource = "*"
    /// allow = true
    /// ```
    pub fn from_toml_file(path: &std::path::Path) -> TeoDBResult<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| TeoDBError::Config(format!("failed to read allow-list file {}: {e}", path.display())))?;
        Self::from_toml_str(&content)
    }

    pub fn from_toml_str(content: &str) -> TeoDBResult<Self> {
        #[derive(Deserialize)]
        struct RuleFile {
            rules: Vec<Rule>,
        }
        let file: RuleFile =
            toml::from_str(content).map_err(|e| TeoDBError::Config(format!("invalid allow-list TOML: {e}")))?;
        Ok(Self::new(file.rules))
    }

    fn matches_role(pattern: &str, principal: &Principal) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return principal
                .roles
                .iter()
                .any(|r| r.starts_with(prefix));
        }
        principal.roles.iter().any(|r| r == pattern)
    }

    fn matches_action(pattern: &str, action: &Action) -> bool {
        if pattern == "*" {
            return true;
        }
        let action_str = format!("{action:?}");
        pattern == action_str
    }

    fn matches_resource(pattern: &str, resource: &Resource) -> bool {
        if pattern == "*" {
            return true;
        }
        match resource {
            Resource::Cluster => pattern == "Cluster",
            Resource::Namespace(ns) => {
                if let Some(pat) = pattern
                    .strip_prefix("Namespace(")
                    .and_then(|s| s.strip_suffix(')'))
                {
                    if pat == "*" {
                        return true;
                    }
                    if let Some(prefix) = pat.strip_suffix('*') {
                        return ns.starts_with(prefix);
                    }
                    pat == ns
                } else {
                    false
                }
            }
            Resource::Table(ident) => {
                if let Some(pat) = pattern
                    .strip_prefix("Table(")
                    .and_then(|s| s.strip_suffix(')'))
                {
                    if pat == "*" {
                        return true;
                    }
                    let full = format!("{}.{}", ident.namespace, ident.name);
                    if let Some((ns_pat, tbl_pat)) = pat.split_once('.') {
                        let ns_match = ns_pat == "*"
                            || ns_pat == ident.namespace
                            || ns_pat
                                .strip_suffix('*')
                                .is_some_and(|p| ident.namespace.starts_with(p));
                        let tbl_match = tbl_pat == "*"
                            || tbl_pat == ident.name
                            || tbl_pat
                                .strip_suffix('*')
                                .is_some_and(|p| ident.name.starts_with(p));
                        ns_match && tbl_match
                    } else {
                        pat == full
                    }
                } else {
                    false
                }
            }
        }
    }
}

#[async_trait]
impl Authorizer for AllowListAuthorizer {
    async fn authorize(&self, principal: &Principal, action: &Action, resource: &Resource) -> TeoDBResult<()> {
        for rule in &self.rules {
            if Self::matches_role(&rule.role, principal)
                && Self::matches_action(&rule.action, action)
                && Self::matches_resource(&rule.resource, resource)
            {
                if rule.allow {
                    info!(
                        target: "teodb::audit",
                        subject = %principal.subject,
                        action = ?action,
                        resource = ?resource,
                        decision = "allow",
                        rule_role = %rule.role,
                        "authorization granted"
                    );
                    return Ok(());
                } else {
                    info!(
                        target: "teodb::audit",
                        subject = %principal.subject,
                        action = ?action,
                        resource = ?resource,
                        decision = "deny",
                        rule_role = %rule.role,
                        "authorization denied"
                    );
                    return Err(TeoDBError::Forbidden(format!(
                        "{} is not authorized to {action:?} on {resource:?}",
                        principal.subject
                    )));
                }
            }
        }

        // No matching rule → deny by default.
        info!(
            target: "teodb::audit",
            subject = %principal.subject,
            action = ?action,
            resource = ?resource,
            decision = "deny",
            "no matching rule, denied by default"
        );
        Err(TeoDBError::Forbidden(format!(
            "{} is not authorized to {action:?} on {resource:?} (no matching rule)",
            principal.subject
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use teodb_core::ident::TableIdent;

    fn principal(subject: &str, roles: &[&str]) -> Principal {
        Principal {
            subject: subject.into(),
            roles: roles.iter().map(|&s| s.into()).collect(),
            claims: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn wildcard_allows_all() {
        let authz = AllowListAuthorizer::new(vec![Rule {
            role: "*".into(),
            action: "*".into(),
            resource: "*".into(),
            allow: true,
        }]);
        let p = principal("alice", &["user"]);
        assert!(
            authz
                .authorize(&p, &Action::Query, &Resource::Cluster)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn no_rules_denies() {
        let authz = AllowListAuthorizer::new(vec![]);
        let p = principal("alice", &["user"]);
        assert!(
            authz
                .authorize(&p, &Action::Query, &Resource::Cluster)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn specific_role_match() {
        let authz = AllowListAuthorizer::new(vec![Rule {
            role: "admin".into(),
            action: "*".into(),
            resource: "*".into(),
            allow: true,
        }]);
        let admin = principal("alice", &["admin"]);
        let user = principal("bob", &["user"]);
        assert!(
            authz
                .authorize(&admin, &Action::Query, &Resource::Cluster)
                .await
                .is_ok()
        );
        assert!(
            authz
                .authorize(&user, &Action::Query, &Resource::Cluster)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn role_prefix_wildcard() {
        let authz = AllowListAuthorizer::new(vec![Rule {
            role: "ops_*".into(),
            action: "*".into(),
            resource: "*".into(),
            allow: true,
        }]);
        let p = principal("carol", &["ops_admin"]);
        assert!(
            authz
                .authorize(&p, &Action::Query, &Resource::Cluster)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn table_wildcard_namespace() {
        let authz = AllowListAuthorizer::new(vec![Rule {
            role: "*".into(),
            action: "Query".into(),
            resource: "Table(*.events)".into(),
            allow: true,
        }]);
        let p = principal("alice", &["user"]);
        let r = Resource::Table(TableIdent::new("analytics", "events"));
        assert!(
            authz
                .authorize(&p, &Action::Query, &r)
                .await
                .is_ok()
        );
        let r2 = Resource::Table(TableIdent::new("analytics", "users"));
        assert!(
            authz
                .authorize(&p, &Action::Query, &r2)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn deny_rule() {
        let authz = AllowListAuthorizer::new(vec![
            Rule {
                role: "*".into(),
                action: "Admin".into(),
                resource: "*".into(),
                allow: false,
            },
            Rule {
                role: "*".into(),
                action: "*".into(),
                resource: "*".into(),
                allow: true,
            },
        ]);
        let p = principal("alice", &["user"]);
        assert!(
            authz
                .authorize(&p, &Action::Admin, &Resource::Cluster)
                .await
                .is_err()
        );
        assert!(
            authz
                .authorize(&p, &Action::Query, &Resource::Cluster)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn first_match_wins() {
        let authz = AllowListAuthorizer::new(vec![
            Rule {
                role: "admin".into(),
                action: "*".into(),
                resource: "*".into(),
                allow: true,
            },
            Rule {
                role: "*".into(),
                action: "*".into(),
                resource: "*".into(),
                allow: false,
            },
        ]);
        let admin = principal("alice", &["admin"]);
        let user = principal("bob", &["user"]);
        assert!(
            authz
                .authorize(&admin, &Action::Query, &Resource::Cluster)
                .await
                .is_ok()
        );
        assert!(
            authz
                .authorize(&user, &Action::Query, &Resource::Cluster)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn from_toml_str() {
        let toml = r#"
[[rules]]
role = "*"
action = "Query"
resource = "*"
allow = true

[[rules]]
role = "admin"
action = "*"
resource = "*"
allow = true
"#;
        let authz = AllowListAuthorizer::from_toml_str(toml).unwrap();
        let p = principal("alice", &["user"]);
        assert!(
            authz
                .authorize(&p, &Action::Query, &Resource::Cluster)
                .await
                .is_ok()
        );
        assert!(
            authz
                .authorize(&p, &Action::Ingest, &Resource::Cluster)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn namespace_pattern() {
        let authz = AllowListAuthorizer::new(vec![Rule {
            role: "*".into(),
            action: "*".into(),
            resource: "Namespace(prod_*)".into(),
            allow: true,
        }]);
        let p = principal("alice", &["user"]);
        assert!(
            authz
                .authorize(&p, &Action::Query, &Resource::Namespace("prod_analytics".into()))
                .await
                .is_ok()
        );
        assert!(
            authz
                .authorize(&p, &Action::Query, &Resource::Namespace("dev_analytics".into()))
                .await
                .is_err()
        );
    }
}
