//! `PARTITION BY` clause parsing — Iceberg partition transform expressions.
//!
//! Supported syntax:
//! - `PARTITION BY (col)` → identity transform
//! - `PARTITION BY (year(col), month(col), day(col), hour(col))` → temporal transforms
//! - `PARTITION BY (bucket(16, col))` → bucket transform
//! - `PARTITION BY (truncate(100, col))` → truncate transform
//! - Hive-style `PARTITIONED BY (col type, ...)` → identity transforms

use sqlparser::ast::{Expr, Function, HiveDistributionStyle};

use teodb_core::error::{TeoDBError, TeoDBResult};

use super::plan::{PartitionFieldDef, PartitionTransformDef};

/// Parse the optional `PARTITION BY (...)` clause into a list of partition
/// field definitions.
pub(super) fn parse_partition_by(expr: &Option<Box<Expr>>) -> TeoDBResult<Vec<PartitionFieldDef>> {
    let Some(expr) = expr else {
        return Ok(Vec::new());
    };

    match expr.as_ref() {
        // Single expression: PARTITION BY year(ts) or PARTITION BY col
        Expr::Function(_) | Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(vec![parse_partition_expr(expr)?]),
        // Tuple: PARTITION BY (year(ts), bucket(16, id))
        Expr::Tuple(exprs) => exprs.iter().map(parse_partition_expr).collect(),
        // Nested: PARTITION BY (col) — parsed as Nested
        Expr::Nested(inner) => Ok(vec![parse_partition_expr(inner)?]),
        other => Err(TeoDBError::InvalidArgument {
            field: "partition_by".into(),
            message: format!("unsupported partition expression: {other}"),
        }),
    }
}

/// Extract partition fields from Hive-style `PARTITIONED BY (col type, ...)`.
///
/// In this syntax sqlparser parses the columns as `ColumnDef` values.
/// We only use the column names (identity transform) since the Hive syntax
/// doesn't support Iceberg transforms like `year()` or `bucket()`.
pub(super) fn parse_hive_partition(dist: &HiveDistributionStyle) -> TeoDBResult<Vec<PartitionFieldDef>> {
    match dist {
        HiveDistributionStyle::PARTITIONED { columns } => Ok(columns
            .iter()
            .map(|col| PartitionFieldDef {
                column_name: col.name.value.clone(),
                transform: PartitionTransformDef::Identity,
            })
            .collect()),
        _ => Ok(Vec::new()),
    }
}

/// Parse a single partition expression (e.g., `year(col)` or bare `col`).
fn parse_partition_expr(expr: &Expr) -> TeoDBResult<PartitionFieldDef> {
    match expr {
        // Bare column name → identity transform
        Expr::Identifier(ident) => Ok(PartitionFieldDef {
            column_name: ident.value.clone(),
            transform: PartitionTransformDef::Identity,
        }),
        Expr::CompoundIdentifier(parts) => {
            let col = parts
                .last()
                .map(|id| id.value.clone())
                .unwrap_or_default();
            Ok(PartitionFieldDef {
                column_name: col,
                transform: PartitionTransformDef::Identity,
            })
        }
        // Function call → transform(args)
        Expr::Function(func) => parse_partition_function(func),
        other => Err(TeoDBError::InvalidArgument {
            field: "partition_by".into(),
            message: format!("unsupported partition expression: {other}"),
        }),
    }
}

/// Parse a function-call partition expression like `year(col)` or `bucket(16, col)`.
fn parse_partition_function(func: &Function) -> TeoDBResult<PartitionFieldDef> {
    let func_name = func.name.to_string().to_lowercase();
    let args = extract_function_args(func)?;

    match func_name.as_str() {
        "year" | "month" | "day" | "hour" | "identity" => {
            if args.len() != 1 {
                return Err(TeoDBError::InvalidArgument {
                    field: "partition_by".into(),
                    message: format!("{func_name}() expects exactly 1 argument, got {}", args.len()),
                });
            }
            let transform = match func_name.as_str() {
                "year" => PartitionTransformDef::Year,
                "month" => PartitionTransformDef::Month,
                "day" => PartitionTransformDef::Day,
                "hour" => PartitionTransformDef::Hour,
                "identity" => PartitionTransformDef::Identity,
                _ => unreachable!(),
            };
            Ok(PartitionFieldDef {
                column_name: expr_to_column_name(&args[0])?,
                transform,
            })
        }
        "bucket" => {
            if args.len() != 2 {
                return Err(TeoDBError::InvalidArgument {
                    field: "partition_by".into(),
                    message: format!("bucket() expects 2 arguments (n, col), got {}", args.len()),
                });
            }
            let n = expr_to_u32(&args[0], "bucket")?;
            let col = expr_to_column_name(&args[1])?;
            Ok(PartitionFieldDef {
                column_name: col,
                transform: PartitionTransformDef::Bucket(n),
            })
        }
        "truncate" => {
            if args.len() != 2 {
                return Err(TeoDBError::InvalidArgument {
                    field: "partition_by".into(),
                    message: format!("truncate() expects 2 arguments (width, col), got {}", args.len()),
                });
            }
            let w = expr_to_u32(&args[0], "truncate")?;
            let col = expr_to_column_name(&args[1])?;
            Ok(PartitionFieldDef {
                column_name: col,
                transform: PartitionTransformDef::Truncate(w),
            })
        }
        _ => Err(TeoDBError::InvalidArgument {
            field: "partition_by".into(),
            message: format!(
                "unsupported partition transform: {func_name}. \
                 Supported: identity, year, month, day, hour, bucket(n, col), truncate(w, col)"
            ),
        }),
    }
}

/// Extract function arguments as a Vec<Expr>.
fn extract_function_args(func: &Function) -> TeoDBResult<Vec<Expr>> {
    use sqlparser::ast::FunctionArguments;
    match &func.args {
        FunctionArguments::List(arg_list) => {
            let mut exprs = Vec::new();
            for arg in &arg_list.args {
                use sqlparser::ast::FunctionArg;
                match arg {
                    FunctionArg::Unnamed(expr_arg) => {
                        use sqlparser::ast::FunctionArgExpr;
                        match expr_arg {
                            FunctionArgExpr::Expr(e) => exprs.push(e.clone()),
                            other => {
                                return Err(TeoDBError::InvalidArgument {
                                    field: "partition_by".into(),
                                    message: format!("unsupported function argument: {other}"),
                                });
                            }
                        }
                    }
                    FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                        use sqlparser::ast::FunctionArgExpr;
                        match arg {
                            FunctionArgExpr::Expr(e) => exprs.push(e.clone()),
                            other => {
                                return Err(TeoDBError::InvalidArgument {
                                    field: "partition_by".into(),
                                    message: format!("unsupported function argument: {other}"),
                                });
                            }
                        }
                    }
                }
            }
            Ok(exprs)
        }
        FunctionArguments::None => Ok(Vec::new()),
        other => Err(TeoDBError::InvalidArgument {
            field: "partition_by".into(),
            message: format!("unsupported function arguments syntax: {other}"),
        }),
    }
}

/// Extract a column name from an expression.
fn expr_to_column_name(expr: &Expr) -> TeoDBResult<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Ok(parts
            .last()
            .map(|id| id.value.clone())
            .unwrap_or_default()),
        other => Err(TeoDBError::InvalidArgument {
            field: "partition_by".into(),
            message: format!("expected column name, got: {other}"),
        }),
    }
}

/// Extract a u32 literal from an expression.
fn expr_to_u32(expr: &Expr, context: &str) -> TeoDBResult<u32> {
    match expr {
        Expr::Value(v) => {
            let s = v.to_string();
            s.parse::<u32>()
                .map_err(|_| TeoDBError::InvalidArgument {
                    field: "partition_by".into(),
                    message: format!("{context}() expects a positive integer, got: {s}"),
                })
        }
        other => Err(TeoDBError::InvalidArgument {
            field: "partition_by".into(),
            message: format!("{context}() expects a numeric literal, got: {other}"),
        }),
    }
}
