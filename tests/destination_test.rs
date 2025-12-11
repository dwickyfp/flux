//! Tests for destination/snowflake.rs buffering logic

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use serde_json::{json, Value};

/// Simulates the buffer logic from SnowflakeDestination
struct TestBuffer {
    buffer: Arc<Mutex<Vec<Value>>>,
    batch_size: usize,
    flush_interval_ms: u64,
    last_flush: Arc<Mutex<Instant>>,
    flush_count: Arc<Mutex<usize>>,
}

impl TestBuffer {
    fn new(batch_size: usize, flush_interval_ms: u64) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            batch_size,
            flush_interval_ms,
            last_flush: Arc::new(Mutex::new(Instant::now())),
            flush_count: Arc::new(Mutex::new(0)),
        }
    }

    async fn add_rows(&self, rows: Vec<Value>) -> bool {
        let mut buffer = self.buffer.lock().await;
        buffer.extend(rows);
        
        let should_flush = buffer.len() >= self.batch_size;
        drop(buffer);

        if should_flush {
            self.flush().await;
            return true;
        }
        
        let last = self.last_flush.lock().await;
        if last.elapsed() >= Duration::from_millis(self.flush_interval_ms) {
            drop(last);
            self.flush().await;
            return true;
        }
        
        false
    }

    async fn flush(&self) {
        let mut buffer = self.buffer.lock().await;
        let _batch = std::mem::take(&mut *buffer);
        drop(buffer);
        
        let mut count = self.flush_count.lock().await;
        *count += 1;
        
        let mut last = self.last_flush.lock().await;
        *last = Instant::now();
    }

    async fn get_buffer_size(&self) -> usize {
        self.buffer.lock().await.len()
    }

    async fn get_flush_count(&self) -> usize {
        *self.flush_count.lock().await
    }
}

#[tokio::test]
async fn test_buffer_accumulation() {
    let buffer = TestBuffer::new(10, 5000);
    
    let rows = vec![json!({"content": "test1"}), json!({"content": "test2"})];
    let flushed = buffer.add_rows(rows).await;
    
    assert!(!flushed, "Should not flush yet");
    assert_eq!(buffer.get_buffer_size().await, 2, "Buffer should have 2 rows");
}

#[tokio::test]
async fn test_flush_on_batch_size() {
    let buffer = TestBuffer::new(5, 60000); // Large interval to avoid time-based flush
    
    // Add rows below threshold
    for i in 0..4 {
        let rows = vec![json!({"content": format!("row{}", i)})];
        buffer.add_rows(rows).await;
    }
    assert_eq!(buffer.get_flush_count().await, 0, "Should not have flushed yet");
    
    // Add one more to trigger flush
    let rows = vec![json!({"content": "trigger"})];
    let flushed = buffer.add_rows(rows).await;
    
    assert!(flushed, "Should have flushed");
    assert_eq!(buffer.get_flush_count().await, 1, "Should have flushed once");
    assert_eq!(buffer.get_buffer_size().await, 0, "Buffer should be empty");
}

#[tokio::test]
async fn test_flush_on_time_interval() {
    let buffer = TestBuffer::new(1000, 50); // Small interval, large batch size
    
    let rows = vec![json!({"content": "test"})];
    buffer.add_rows(rows).await;
    
    // Wait for interval to pass
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Add another row to trigger time-based check
    let rows = vec![json!({"content": "test2"})];
    let flushed = buffer.add_rows(rows).await;
    
    assert!(flushed, "Should have flushed due to time interval");
}

#[tokio::test]
async fn test_concurrent_access() {
    let buffer = Arc::new(TestBuffer::new(100, 60000));
    let mut handles = vec![];
    
    // Spawn multiple tasks adding rows concurrently
    for i in 0..10 {
        let buf = Arc::clone(&buffer);
        let handle = tokio::spawn(async move {
            for j in 0..5 {
                let rows = vec![json!({"content": format!("task{}row{}", i, j)})];
                buf.add_rows(rows).await;
            }
        });
        handles.push(handle);
    }
    
    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Should have 50 rows total (10 tasks * 5 rows each)
    assert_eq!(buffer.get_buffer_size().await, 50, "Should have all 50 rows");
}

#[tokio::test]
async fn test_empty_batch_noop() {
    let buffer = TestBuffer::new(5, 1000);
    
    let rows: Vec<Value> = vec![];
    buffer.add_rows(rows).await;
    
    assert_eq!(buffer.get_buffer_size().await, 0, "Buffer should remain empty");
    assert_eq!(buffer.get_flush_count().await, 0, "Should not flush for empty batch");
}
