//! Fixed-capacity byte ring buffer for capping process output in memory.
//!
//! Once the buffer is full, old bytes are silently overwritten.

/// Default capacity: 1 MiB.
pub const DEFAULT_CAPACITY: usize = 1_048_576;

/// Circular byte buffer that retains at most `capacity` bytes.
pub struct RingBuffer {
    data: Vec<u8>,
    /// Write position (index into `data`).
    head: usize,
    /// Number of valid bytes currently stored (≤ capacity).
    len: usize,
    capacity: usize,
}

impl RingBuffer {
    /// Create a ring buffer with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            head: 0,
            len: 0,
            capacity,
        }
    }

    /// Number of bytes currently stored.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append `bytes` to the buffer.  If the incoming slice exceeds capacity,
    /// only the last `capacity` bytes are kept.
    pub fn push(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }

        // If bytes is larger than our entire capacity, skip to the tail.
        let src = if bytes.len() > self.capacity {
            &bytes[bytes.len() - self.capacity..]
        } else {
            bytes
        };

        for &b in src {
            self.data[self.head] = b;
            self.head = (self.head + 1) % self.capacity;
            if self.len < self.capacity {
                self.len += 1;
            }
        }
    }

    /// Return all stored bytes in chronological order.
    pub fn as_bytes(&self) -> Vec<u8> {
        if self.len == 0 {
            return Vec::new();
        }
        let start = if self.len < self.capacity {
            0
        } else {
            self.head // head is where the oldest byte lives after wrap
        };

        let mut out = Vec::with_capacity(self.len);
        for i in 0..self.len {
            out.push(self.data[(start + i) % self.capacity]);
        }
        out
    }

    /// Return the last `n` bytes (or fewer if fewer are stored).
    pub fn tail(&self, n: usize) -> Vec<u8> {
        let n = n.min(self.len);
        if n == 0 {
            return Vec::new();
        }
        let all = self.as_bytes();
        all[all.len() - n..].to_vec()
    }
}

impl Default for RingBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_push_and_read() {
        let mut rb = RingBuffer::new(16);
        rb.push(b"hello");
        assert_eq!(rb.len(), 5);
        assert_eq!(rb.as_bytes(), b"hello");
    }

    #[test]
    fn tail_returns_last_n() {
        let mut rb = RingBuffer::new(16);
        rb.push(b"hello world");
        assert_eq!(rb.tail(5), b"world");
        // Requesting more than stored returns everything.
        assert_eq!(rb.tail(100), b"hello world");
    }

    #[test]
    fn overflow_wraps_around() {
        let mut rb = RingBuffer::new(8);
        rb.push(b"ABCDEFGH"); // fills exactly
        assert_eq!(rb.len(), 8);
        assert_eq!(rb.as_bytes(), b"ABCDEFGH");

        rb.push(b"XY"); // overwrites A, B
        assert_eq!(rb.len(), 8);
        assert_eq!(rb.as_bytes(), b"CDEFGHXY");
    }

    #[test]
    fn push_larger_than_capacity() {
        let mut rb = RingBuffer::new(4);
        rb.push(b"ABCDEFGH"); // only last 4 kept
        assert_eq!(rb.as_bytes(), b"EFGH");
    }

    #[test]
    fn empty_buffer() {
        let rb = RingBuffer::new(8);
        assert!(rb.is_empty());
        assert_eq!(rb.as_bytes(), b"");
        assert_eq!(rb.tail(5), b"");
    }

    #[test]
    fn multiple_small_pushes_with_wrap() {
        let mut rb = RingBuffer::new(6);
        rb.push(b"ABC");
        rb.push(b"DEF");
        assert_eq!(rb.as_bytes(), b"ABCDEF");
        rb.push(b"GH");
        assert_eq!(rb.as_bytes(), b"CDEFGH");
        assert_eq!(rb.tail(3), b"FGH");
    }

    #[test]
    fn default_capacity() {
        let rb = RingBuffer::default();
        assert_eq!(rb.capacity, DEFAULT_CAPACITY);
        assert!(rb.is_empty());
    }
}
