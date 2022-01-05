// Copyright 2021 The Meerkat Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module provides specialized buffer implementations that are used
//! to store column values in memory until they are flushed to disk.
//!
//! Column values are stored in a sparse representation, only non-null values
//! are stored and validity data is encoded using an internal bitmap.
//!

use std::convert::TryInto;
use std::ops::Range;

use itertools::Itertools;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const INITIAL_BITMAP_CAP: usize = 1024;

pub type Int32Buffer = PrimitiveBuffer<i32>;
pub type Int64Buffer = PrimitiveBuffer<i64>;
pub type Uint64Buffer = PrimitiveBuffer<u64>;
pub type Float64Buffer = PrimitiveBuffer<f64>;

/// A reusable growable bitmap.
pub struct Bitmap {
    values: Vec<u8>,
    len: usize,
}

impl Bitmap {
    pub fn new() -> Self {
        Self {
            values: vec![0; INITIAL_BITMAP_CAP],
            len: 0,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            values: vec![0; cap >> 3],
            len: 0,
        }
    }

    pub fn append(&mut self, set: bool) {
        let idx = self.len >> 3;

        if idx == self.values.len() {
            self.values.resize(self.values.len() * 2, 0);
        }

        if set {
            self.values[idx] |= BIT_MASK[self.len & 7];
        }

        self.len += 1;
    }

    pub fn append_from(&mut self, src: &Bitmap, start_pos: usize, num_of_items: usize) -> usize {
        let mut items = 0;
        let mut bit_pos = start_pos;
        while items < num_of_items {
            let mut bit_value = src.is_set(bit_pos);
            self.append(bit_value);
            items += bit_value as usize;
            bit_pos += 1;
        }
        bit_pos
    }

    pub fn is_set(&self, idx: usize) -> bool {
        assert!(idx < self.len);
        (self.values[idx >> 3] & BIT_MASK[idx & 7]) != 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        self.len = 0;
        self.values.resize(INITIAL_BITMAP_CAP, 0);
    }
}

/// A growable sparse encoded buffer for primitive values.
/// Non-null values are stores sequentially, Nulls are encoded using a bitmap.
pub struct PrimitiveBuffer<T> {
    values: Vec<T>,
    validity: Option<Bitmap>,
}

impl<T: Clone> PrimitiveBuffer<T> {
    pub fn new(nullable: bool) -> Self {
        let validity = match nullable {
            true => Some(Bitmap::new()),
            false => None,
        };

        Self {
            values: Vec::new(),
            validity: validity,
        }
    }

    /// Append a non-null value to the buffer
    pub fn append(&mut self, value: T) {
        self.values.push(value);
        if let Some(ref mut validity) = self.validity {
            validity.append(true);
        }
    }

    /// Append a null value to the buffer.
    /// This methos will panic if the buffer is not nullable.
    pub fn append_null(&mut self) {
        match self.validity {
            Some(ref mut validity) => validity.append(false),
            None => panic!("null append on non-nullable buffer"),
        }
    }

    pub fn append_from(
        &mut self,
        buf: &PrimitiveBuffer<T>,
        start: usize,
        end: usize,
        validity_pos: usize,
    ) -> usize {
        self.values.extend_from_slice(&buf.values[start..end]);

        assert!(u32::MAX as usize > self.values.len());

        let num_values = end - start;

        if let Some(ref validity_src) = buf.validity {
            let mut validity_dst = self
                .validity
                .as_mut()
                .expect("trying to append validity on a non-nullable buffer");

            validity_dst.append_from(validity_src, validity_pos, num_values)
        } else {
            assert!(self.validity.is_none(), "missing validity bitmap");
            num_values
        }
    }

    /// Returns the values stored in this buffer.
    /// This is a sparse representation so only non-null values are returned.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// Returns a ref to the validity bitmap.
    /// Panic if the buffer is not nullable.
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the number of items stored in the buffer including null values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns the number of items stored in this buffer. ( including nulls )
    pub fn num_values(&self) -> usize {
        match self.validity {
            Some(ref validity) => validity.len(),
            None => self.values.len(),
        }
    }

    /// Returns true if the buffer accepts non-null values.
    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    /// Returns true if the buffer has null items.
    pub fn has_nulls(&self) -> bool {
        match self.validity {
            Some(ref validity) => validity.len() - self.values.len() != 0,
            None => false,
        }
    }

    /// Clear all existing data from this buffer.
    pub fn clear(&mut self) {
        self.values.clear();
        if let Some(ref mut validity) = self.validity {
            validity.clear();
        }
    }
}

#[derive(Debug)]
pub struct BinaryChunk {
    pub start_pos: usize,
    pub end_pos: usize,
    pub start_offset: u32,
    pub end_offset: u32,
    pub size: u32,
    pub len: usize,
}

/// A growable sparse encoded buffer for binary types.
/// Buffer items are stored sequentially and offsets to each item are stored
/// in a separated offset buffer.
pub struct BinaryBuffer {
    data: Vec<u8>,
    offsets: Vec<u32>,
    validity: Option<Bitmap>,
}

impl BinaryBuffer {
    pub fn new(nullable: bool) -> Self {
        let validity = match nullable {
            true => Some(Bitmap::new()),
            false => None,
        };

        Self {
            data: Vec::new(),
            offsets: vec![0; 1],
            validity: validity,
        }
    }

    /// Append a non-null value.
    pub fn append(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
        let value_offset: u32 = self.data.len().try_into().expect("BinaryBuffer overflow");
        self.offsets.push(value_offset);
        if let Some(ref mut validity) = self.validity {
            validity.append(true);
        }
    }

    /// Append a null value to the buffer.
    pub fn append_null(&mut self) {
        match self.validity {
            Some(ref mut validity) => validity.append(false),
            None => panic!("null append on non-nullable buffer"),
        }
    }

    pub fn item_len(&self, idx: usize) -> u32 {
        self.offsets[idx + 1] - self.offsets[idx]
    }

    pub fn append_from(
        &mut self,
        buf: &BinaryBuffer,
        chunk: &BinaryChunk,
        validity_pos: usize,
    ) -> usize {
        let mut last_item_offset = *self.offsets.last().unwrap_or(&0u32);

        for i in chunk.start_pos..chunk.end_pos {
            last_item_offset += buf.item_len(i);
            self.offsets.push(last_item_offset);
        }

        self.data
            .extend_from_slice(&buf.data()[chunk.start_offset as usize..chunk.end_offset as usize]);

        assert!(u32::MAX as usize > self.data.len());

        if let Some(ref validity_src) = buf.validity {
            let mut validity_dst = self
                .validity
                .as_mut()
                .expect("trying to append validity on a non-nullable buffer");
            validity_dst.append_from(validity_src, validity_pos, chunk.len)
        } else {
            assert!(self.validity.is_none(), "missing validity bitmap");
            chunk.len
        }
    }

    /// Returns a reference to the stored values.
    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    /// Returns a slice containing the values offsets
    pub fn offsets(&self) -> &[u32] {
        self.offsets.as_slice()
    }

    /// Returns a ref to the validity bitmap.
    /// Panic if the buffer is not nullable.
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the number of not nulls items stored in this buffer.
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Returns the number of items stored in this buffer. ( including nulls )
    pub fn num_values(&self) -> usize {
        match self.validity {
            Some(ref validity) => validity.len(),
            None => self.len(),
        }
    }

    /// Returns true if the buffer accepts non-null values.
    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    /// Returns true if the buffer has null items.
    pub fn has_nulls(&self) -> bool {
        match self.validity {
            Some(ref validity) => validity.len() - self.len() != 0,
            None => false,
        }
    }

    /// Clear all existing data from this buffer.
    pub fn clear(&mut self) {
        self.data.clear();
        self.offsets.truncate(1);
        if let Some(ref mut validity) = self.validity {
            validity.clear();
        };
    }

    pub fn chunk(&self, start_pos: usize, min_size: u64) -> BinaryChunk {
        let start_offset = self.offsets()[start_pos];

        let maybe_end_pos = self.offsets()[start_pos..]
            .iter()
            .find_position(|offset| (*offset - start_offset) as u64 >= min_size);

        let (end_pos, end_offset) = match maybe_end_pos {
            Some(pos) => (pos.0, *pos.1),
            None => (self.offsets.len() - 1, self.offsets[self.offsets.len() - 1]),
        };

        BinaryChunk {
            start_pos,
            end_pos,
            start_offset,
            end_offset,
            size: end_offset - start_offset,
            len: end_pos - start_pos,
        }
    }
}

/// A growable sparse encoded buffer for bool values.
pub struct BoolBuffer {
    values: Bitmap,
    validity: Option<Bitmap>,
    len: usize,
}

impl BoolBuffer {
    pub fn new(nullable: bool) -> Self {
        let validity = match nullable {
            true => Some(Bitmap::new()),
            false => None,
        };

        Self {
            values: Bitmap::new(),
            validity: validity,
            len: 0,
        }
    }

    /// Append a new non-null value to the buffer.
    pub fn append(&mut self, value: bool) {
        self.values.append(value);
        if let Some(ref mut validity) = self.validity {
            validity.append(true);
        };
        self.len += 1;
    }

    /// Append a new value to the buffer.
    pub fn append_null(&mut self) {
        match self.validity {
            Some(ref mut validity) => validity.append(false),
            None => panic!("null append on non-nullable buffer"),
        };
        self.len += 1;
    }

    /// Returns a reference to the internal value bitmap.
    pub fn values(&self) -> &Bitmap {
        &self.values
    }

    /// Returns a ref to the validity bitmap.
    /// Panic if the buffer is not nullable.
    pub fn validity(&self) -> &Bitmap {
        &self.validity.as_ref().unwrap()
    }

    /// The number of items stored in this buffer. ( including non-null )
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer accepts non-null values.
    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    /// Returns true if the buffer has null items.
    pub fn has_nulls(&self) -> bool {
        !(self.values.len() == self.len)
    }

    /// Clear all existing data from this buffer.
    pub fn clear(&mut self) {
        self.values.clear();
        if let Some(ref mut validity) = self.validity {
            validity.clear();
        };
        self.len = 0;
    }
}

#[cfg(test)]
mod test {
    use itertools::Chunk;
    use rand::Rng;

    use super::*;

    #[test]
    fn bitmap_rnd_test() {
        let mut rng = rand::thread_rng();
        let len = rng.gen_range(0..2048);
        let mut values = vec![false; len];
        rng.fill(values.as_mut_slice());

        let mut bitmap = Bitmap::new();

        for value in &values {
            bitmap.append(*value);
        }

        for (i, value) in values.iter().enumerate() {
            assert_eq!(bitmap.is_set(i), *value);
        }

        assert_eq!(bitmap.len(), values.len());
    }

    #[test]
    fn bitmap_append_from_test() {
        let mut bitmap_dst = Bitmap::new();

        for _ in 0..6 {
            bitmap_dst.append(true);
        }

        let mut bitmap_src = Bitmap::new();
        bitmap_src.append(true);
        bitmap_src.append(false);
        bitmap_src.append(true);
        bitmap_src.append(false);
        bitmap_src.append(true);

        let num_items = bitmap_dst.append_from(&bitmap_src, 0, 2);

        assert_eq!(bitmap_dst.len(), 9);
        assert_eq!(num_items, 3);
        assert!(!bitmap_dst.is_set(7));
        assert!(bitmap_dst.is_set(8));
    }

    #[test]
    fn test_primitive_buffer() {
        let mut buffer: PrimitiveBuffer<i32> = PrimitiveBuffer::new(false);

        buffer.append(1);
        buffer.append(2);
        buffer.append(3);

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.values().len(), 3);
        assert_eq!(buffer.is_nullable(), false);
        assert_eq!(buffer.has_nulls(), false);

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.values().len(), 0);
        assert_eq!(buffer.is_nullable(), false);
        assert_eq!(buffer.has_nulls(), false);
    }

    #[test]
    fn test_primitive_nullable_buffer() {
        let mut buffer: PrimitiveBuffer<i32> = PrimitiveBuffer::new(true);

        buffer.append(1);
        buffer.append(2);
        buffer.append_null();
        buffer.append_null();
        buffer.append(3);

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.num_values(), 5);
        assert_eq!(buffer.is_nullable(), true);
        assert_eq!(buffer.has_nulls(), true);

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.values().len(), 0);
        assert_eq!(buffer.is_nullable(), true);
        assert_eq!(buffer.has_nulls(), false);

        buffer.append(10);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.values().len(), 1);
        assert_eq!(buffer.is_nullable(), true);
        assert_eq!(buffer.has_nulls(), false);
    }

    #[test]
    fn test_binary_buffer() {
        let mut buffer = BinaryBuffer::new(false);

        buffer.append("Liskov".as_bytes());
        buffer.append("Lamport".as_bytes());
        buffer.append("Gray".as_bytes());

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.data().len(), 17);
        assert_eq!(buffer.is_nullable(), false);
        assert_eq!(buffer.has_nulls(), false);

        let expected_offsets: Vec<u32> = vec![0, 6, 13, 17];
        for (actual, expected) in buffer.offsets.iter().zip(expected_offsets.iter()) {
            assert_eq!(actual, expected);
        }

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.data().len(), 0);
        assert_eq!(buffer.is_nullable(), false);
        assert_eq!(buffer.has_nulls(), false);

        buffer.append("Naur".as_bytes());
    }

    #[test]
    fn test_binary_nullable_buffer() {
        let mut buffer = BinaryBuffer::new(true);

        buffer.append("Liskov".as_bytes());
        buffer.append_null();
        buffer.append_null();
        buffer.append("Lamport".as_bytes());
        buffer.append_null();
        buffer.append("Gray".as_bytes());

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.data().len(), 17);
        assert_eq!(buffer.is_nullable(), true);
        assert_eq!(buffer.has_nulls(), true);

        let expected_offsets: Vec<u32> = vec![0, 6, 13, 17];
        for (actual, expected) in buffer.offsets.iter().zip(expected_offsets.iter()) {
            assert_eq!(actual, expected);
        }

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.data().len(), 0);
        assert_eq!(buffer.is_nullable(), true);
        assert_eq!(buffer.has_nulls(), false);

        buffer.append("Naur".as_bytes());
    }

    #[test]
    fn test_binary_append_from() {
        let mut src = BinaryBuffer::new(true);

        for _ in 0..10 {
            src.append_null();
        }

        src.append("Knuth".as_bytes());

        let mut dst = BinaryBuffer::new(true);

        dst.append("Liskov".as_bytes());
        dst.append_null();
        dst.append_null();
        dst.append("Lamport".as_bytes());
        dst.append_null();
        dst.append("Gray".as_bytes());
        dst.append_null();

        let chunk = BinaryChunk {
            start_pos: 0,
            end_pos: src.len(),
            start_offset: 0,
            end_offset: src.offsets[1],
            size: src.offsets[1],
            len: 1,
        };

        dst.append_from(&src, &chunk, 0);

        assert_eq!(dst.len(), 4);
        assert_eq!(dst.data().len(), 22);
        assert_eq!(dst.is_nullable(), true);
        assert_eq!(dst.has_nulls(), true);

        let expected_offsets: Vec<u32> = vec![0, 6, 13, 17, 22];
        for (actual, expected) in dst.offsets.iter().zip(expected_offsets.iter()) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_boolean_buffer() {
        let mut buffer = BoolBuffer::new(false);

        buffer.append(true);
        buffer.append(true);
        buffer.append(false);
        buffer.append(true);

        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.values().len(), 4);
        assert_eq!(buffer.is_nullable(), false);
        assert_eq!(buffer.has_nulls(), false);

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.values().len(), 0);
        assert_eq!(buffer.is_nullable(), false);
        assert_eq!(buffer.has_nulls(), false);

        buffer.append(true);
    }

    #[test]
    fn test_boolean_nullable_buffer() {
        let mut buffer = BoolBuffer::new(true);

        buffer.append(true);
        buffer.append(true);
        buffer.append_null();
        buffer.append_null();
        buffer.append(false);
        buffer.append_null();
        buffer.append(true);

        assert_eq!(buffer.len(), 7);
        assert_eq!(buffer.values().len(), 4);
        assert_eq!(buffer.is_nullable(), true);
        assert_eq!(buffer.has_nulls(), true);

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.values().len(), 0);
        assert_eq!(buffer.is_nullable(), true);
        assert_eq!(buffer.has_nulls(), false);

        buffer.append(true);
    }
}
