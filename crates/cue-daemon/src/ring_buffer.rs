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
    /// Whether bytes have been dropped because writes exceeded capacity.
    overflowed: bool,
}

impl RingBuffer {
    /// Create a ring buffer with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            head: 0,
            len: 0,
            capacity,
            overflowed: false,
        }
    }

    /// Number of bytes currently stored.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Append `bytes` to the buffer.  If the incoming slice exceeds capacity,
    /// only the last `capacity` bytes are kept.
    pub fn push(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }

        if self.capacity == 0 {
            self.overflowed = true;
            return;
        }

        if self.len.saturating_add(bytes.len()) > self.capacity {
            self.overflowed = true;
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
        self.tail_with_truncation(n).0
    }

    /// Return the last `n` bytes and whether older output was omitted.
    pub fn tail_with_truncation(&self, n: usize) -> (Vec<u8>, bool) {
        let n = n.min(self.len);
        if n == 0 {
            return (Vec::new(), self.overflowed || self.len > 0);
        }
        let all = self.as_bytes();
        let truncated_by_limit = all.len() > n;
        (
            all[all.len() - n..].to_vec(),
            self.overflowed || truncated_by_limit,
        )
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
    fn tail_with_truncation_reports_limit_and_overflow() {
        let mut rb = RingBuffer::new(6);
        rb.push(b"abcd");
        assert_eq!(rb.tail_with_truncation(4), (b"abcd".to_vec(), false));
        assert_eq!(rb.tail_with_truncation(2), (b"cd".to_vec(), true));

        rb.push(b"efgh");
        assert_eq!(rb.as_bytes(), b"cdefgh");
        assert_eq!(rb.tail_with_truncation(6), (b"cdefgh".to_vec(), true));
    }

    #[test]
    fn empty_buffer() {
        let rb = RingBuffer::new(8);
        assert_eq!(rb.len(), 0);
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
        assert_eq!(rb.len(), 0);
    }
}
