//! Scripted storage faults that can wrap a real backend.

use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use parking_lot::Mutex;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::location::ObjectPath;
use teodb_core::traits::storage::{ObjectMeta, Storage};

/// Storage operation targeted by a scripted fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageOperation {
    Get,
    GetRange,
    Head,
    Put,
    Delete,
    Copy,
    List,
}

/// Failure behavior for one matching operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageFaultKind {
    /// Return a retryable error without contacting the delegate.
    FailBefore { message: String },
    /// Complete the delegated call, then hide its successful response.
    LoseResponse { message: String },
}

/// One fault consumed after `skip` matching operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageFault {
    pub operation: StorageOperation,
    pub skip: usize,
    pub kind: StorageFaultKind,
}

impl StorageFault {
    pub fn fail_next(operation: StorageOperation, message: impl Into<String>) -> Self {
        Self {
            operation,
            skip: 0,
            kind: StorageFaultKind::FailBefore {
                message: message.into(),
            },
        }
    }

    pub fn fail_nth(operation: StorageOperation, nth: usize, message: impl Into<String>) -> Self {
        assert!(nth > 0, "fault occurrence is one-based");
        Self {
            operation,
            skip: nth - 1,
            kind: StorageFaultKind::FailBefore {
                message: message.into(),
            },
        }
    }

    pub fn lose_next_response(operation: StorageOperation, message: impl Into<String>) -> Self {
        Self {
            operation,
            skip: 0,
            kind: StorageFaultKind::LoseResponse {
                message: message.into(),
            },
        }
    }
}

/// Delegates to a real storage implementation while consuming deterministic
/// one-shot faults in FIFO order.
pub struct FaultInjectingStorage {
    delegate: Arc<dyn Storage>,
    faults: Mutex<VecDeque<StorageFault>>,
    reached_tx: tokio::sync::broadcast::Sender<StorageOperation>,
}

impl FaultInjectingStorage {
    pub fn new(delegate: Arc<dyn Storage>) -> Self {
        let (reached_tx, _) = tokio::sync::broadcast::channel(32);
        Self {
            delegate,
            faults: Mutex::new(VecDeque::new()),
            reached_tx,
        }
    }

    pub fn push(&self, fault: StorageFault) {
        self.faults.lock().push_back(fault);
    }

    pub fn clear(&self) {
        self.faults.lock().clear();
    }

    pub fn pending_faults(&self) -> usize {
        self.faults.lock().len()
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<StorageOperation> {
        self.reached_tx.subscribe()
    }

    fn take_fault(&self, operation: StorageOperation) -> Option<StorageFaultKind> {
        let mut faults = self.faults.lock();
        let front = faults.front_mut()?;
        if front.operation != operation {
            return None;
        }
        if front.skip > 0 {
            front.skip -= 1;
            return None;
        }
        let fault = faults.pop_front().expect("front existed");
        let _ = self.reached_tx.send(operation);
        Some(fault.kind)
    }

    fn before(&self, operation: StorageOperation) -> TeoDBResult<Option<StorageFaultKind>> {
        match self.take_fault(operation) {
            Some(StorageFaultKind::FailBefore { message }) => Err(TeoDBError::ExternalRetryable(message)),
            other => Ok(other),
        }
    }

    fn after<T>(&self, fault: Option<StorageFaultKind>, value: T) -> TeoDBResult<T> {
        match fault {
            Some(StorageFaultKind::LoseResponse { message }) => Err(TeoDBError::ExternalRetryable(message)),
            Some(StorageFaultKind::FailBefore { .. }) => {
                unreachable!("pre-delegation faults return before the delegate")
            }
            None => Ok(value),
        }
    }
}

#[async_trait]
impl Storage for FaultInjectingStorage {
    async fn get(&self, path: &ObjectPath) -> TeoDBResult<Bytes> {
        let fault = self.before(StorageOperation::Get)?;
        let value = self.delegate.get(path).await?;
        self.after(fault, value)
    }

    async fn get_range(&self, path: &ObjectPath, range: Range<u64>) -> TeoDBResult<Bytes> {
        let fault = self.before(StorageOperation::GetRange)?;
        let value = self.delegate.get_range(path, range).await?;
        self.after(fault, value)
    }

    async fn head(&self, path: &ObjectPath) -> TeoDBResult<ObjectMeta> {
        let fault = self.before(StorageOperation::Head)?;
        let value = self.delegate.head(path).await?;
        self.after(fault, value)
    }

    async fn put(&self, path: &ObjectPath, bytes: Bytes) -> TeoDBResult<ObjectMeta> {
        let fault = self.before(StorageOperation::Put)?;
        let value = self.delegate.put(path, bytes).await?;
        self.after(fault, value)
    }

    async fn delete(&self, path: &ObjectPath) -> TeoDBResult<()> {
        let fault = self.before(StorageOperation::Delete)?;
        self.delegate.delete(path).await?;
        self.after(fault, ())
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> TeoDBResult<()> {
        let fault = self.before(StorageOperation::Copy)?;
        self.delegate.copy(from, to).await?;
        self.after(fault, ())
    }

    async fn list(
        &self,
        prefix: &ObjectPath,
    ) -> TeoDBResult<Pin<Box<dyn Stream<Item = TeoDBResult<ObjectMeta>> + Send>>> {
        let fault = self.before(StorageOperation::List)?;
        let value = self.delegate.list(prefix).await?;
        self.after(fault, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory_backend;

    #[tokio::test]
    async fn fail_before_does_not_touch_delegate() {
        let inner: Arc<dyn Storage> = in_memory_backend();
        let storage = FaultInjectingStorage::new(inner.clone());
        let path = ObjectPath::new("fault.bin");
        storage.push(StorageFault::fail_next(StorageOperation::Put, "timeout"));

        let error = storage
            .put(&path, Bytes::from_static(b"data"))
            .await
            .expect_err("fault must fire");
        assert!(matches!(error, TeoDBError::ExternalRetryable(_)));
        assert!(inner.head(&path).await.is_err());
    }

    #[tokio::test]
    async fn lost_response_leaves_the_delegated_object() {
        let inner: Arc<dyn Storage> = in_memory_backend();
        let storage = FaultInjectingStorage::new(inner.clone());
        let path = ObjectPath::new("ambiguous.bin");
        storage.push(StorageFault::lose_next_response(StorageOperation::Put, "response lost"));

        storage
            .put(&path, Bytes::from_static(b"data"))
            .await
            .expect_err("client response is lost");
        assert_eq!(inner.get(&path).await.unwrap(), Bytes::from_static(b"data"));
    }

    #[tokio::test]
    async fn nth_fault_skips_matching_calls() {
        let inner: Arc<dyn Storage> = in_memory_backend();
        let storage = FaultInjectingStorage::new(inner);
        storage.push(StorageFault::fail_nth(StorageOperation::Put, 2, "second put failed"));

        storage
            .put(&ObjectPath::new("first"), Bytes::from_static(b"1"))
            .await
            .unwrap();
        storage
            .put(&ObjectPath::new("second"), Bytes::from_static(b"2"))
            .await
            .expect_err("second put must fail");
        assert_eq!(storage.pending_faults(), 0);
    }
}
