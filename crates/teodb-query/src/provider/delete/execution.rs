//! Position-delete filter execution plan.
//!
//! Wraps a child `ExecutionPlan` and filters out rows whose positions
//! appear in a pre-loaded delete set. Row positions are tracked as a
//! running counter per partition, so the child **must** scan exactly one
//! data file in physical row order (the provider builds one single-file
//! scan per delete-bearing file). The node rejects input repartitioning
//! and preserves order to keep the counter aligned with file positions.

use std::collections::HashSet;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

/// A set of row positions (0-based) to exclude from output.
#[derive(Debug, Clone)]
pub struct DeletePositions {
    /// Positions to delete within a data file (0-based row index).
    pub positions: HashSet<i64>,
}

/// Execution plan that filters rows by position-delete sets.
///
/// For each partition, maintains a running row counter. When the
/// counter's value is in the delete set, that row is excluded from output.
#[derive(Debug)]
pub struct PositionDeleteFilterExec {
    child: Arc<dyn ExecutionPlan>,
    /// Per-file delete positions (keyed by file path/partition).
    /// For single-file scans, this is a flat set of positions.
    delete_positions: Arc<DeletePositions>,
    props: Arc<PlanProperties>,
}

impl PositionDeleteFilterExec {
    /// Create a new position-delete filter wrapping the given child plan.
    pub fn new(child: Arc<dyn ExecutionPlan>, delete_positions: DeletePositions) -> Self {
        // Inherit all plan properties from the child — we only filter rows,
        // we don't change schema, partitioning, or boundedness.
        let props = Arc::clone(child.properties());
        Self {
            child,
            delete_positions: Arc::new(delete_positions),
            props,
        }
    }
}

impl DisplayAs for PositionDeleteFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PositionDeleteFilterExec: {} positions",
                    self.delete_positions.positions.len()
                )
            }
        }
    }
}

impl ExecutionPlan for PositionDeleteFilterExec {
    fn name(&self) -> &str {
        "PositionDeleteFilterExec"
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    /// The running row counter is only meaningful against the child's
    /// physical row order — repartitioning the input would misalign
    /// positions and delete the wrong rows.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "PositionDeleteFilterExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            (*self.delete_positions).clone(),
        )))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let schema = self.schema();
        let delete_positions = Arc::clone(&self.delete_positions);

        let stream = PositionDeleteStream {
            inner: child_stream,
            positions: delete_positions,
            current_row_offset: 0,
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Stream adapter that filters batches based on position deletes.
struct PositionDeleteStream {
    inner: SendableRecordBatchStream,
    positions: Arc<DeletePositions>,
    current_row_offset: i64,
}

impl Stream for PositionDeleteStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let num_rows = batch.num_rows();
                    let start_offset = self.current_row_offset;
                    self.current_row_offset += num_rows as i64;

                    // Fast path: no deletes in this batch's range.
                    let end_offset = start_offset + num_rows as i64;
                    let has_deletes = self
                        .positions
                        .positions
                        .iter()
                        .any(|p| *p >= start_offset && *p < end_offset);

                    if !has_deletes {
                        return Poll::Ready(Some(Ok(batch)));
                    }

                    // Build a boolean mask: true = keep, false = delete.
                    let mut keep = vec![true; num_rows];
                    let mut kept_count = num_rows;
                    for (i, keep_flag) in keep.iter_mut().enumerate() {
                        let pos = start_offset + i as i64;
                        if self.positions.positions.contains(&pos) {
                            *keep_flag = false;
                            kept_count -= 1;
                        }
                    }

                    if kept_count == 0 {
                        // Entire batch deleted, skip to next.
                        continue;
                    }

                    // Apply the filter mask using arrow's filter kernel.
                    let mask = arrow::array::BooleanArray::from(keep);
                    let filtered = arrow::compute::filter_record_batch(&batch, &mask)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

                    return Poll::Ready(Some(Ok(filtered)));
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    async fn exec_from_batches(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new();
        let mem = MemTable::try_new(test_schema(), vec![batches]).unwrap();
        let state = ctx.state();
        mem.scan(&state, None, &[], None).await.unwrap()
    }

    #[tokio::test]
    async fn no_deletes_passes_through() {
        let batch = test_batch(vec![1, 2, 3, 4, 5]);
        let child = exec_from_batches(vec![batch]).await;

        let deletes = DeletePositions {
            positions: HashSet::new(),
        };
        let exec = PositionDeleteFilterExec::new(child, deletes);

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();
        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 5);
    }

    #[tokio::test]
    async fn deletes_filter_rows() {
        let batch = test_batch(vec![10, 20, 30, 40, 50]);
        let child = exec_from_batches(vec![batch]).await;

        let deletes = DeletePositions {
            positions: [1, 3].into_iter().collect(),
        };
        let exec = PositionDeleteFilterExec::new(child, deletes);

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();
        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[10, 30, 50]);
    }

    #[tokio::test]
    async fn cross_batch_position_tracking() {
        let batch1 = test_batch(vec![1, 2, 3]);
        let batch2 = test_batch(vec![4, 5, 6]);
        let child = exec_from_batches(vec![batch1, batch2]).await;

        // Delete position 4 (row index 1 in second batch, value=5)
        let deletes = DeletePositions {
            positions: [4].into_iter().collect(),
        };
        let exec = PositionDeleteFilterExec::new(child, deletes);

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();

        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
        }
        // 6 total rows - 1 deleted = 5
        assert_eq!(total_rows, 5);
    }
}
