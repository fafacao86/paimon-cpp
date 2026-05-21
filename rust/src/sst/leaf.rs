// Copyright 2026-present Alibaba Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;

use arrow_array::Array;
use arrow_array::BinaryArray;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::LargeBinaryArray;
use arrow_array::LargeStringArray;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::UInt32Array;
use arrow_array::UInt64Array;
use arrow_schema::ArrowError;
use arrow_schema::DataType;

use crate::sst::block::BlockKind;
use crate::sst::block::HEADER_LEN;
use crate::sst::block::Header;
use crate::sst::block::PhysicalType;
use crate::sst::block::SECTION_ENTRY_LEN;
use crate::sst::block::SectionEntry;
use crate::sst::block::SectionKind;
use crate::sst::block::read_header;
use crate::sst::block::read_i32;
use crate::sst::block::read_i64;
use crate::sst::block::read_section_entry;
use crate::sst::block::read_u32;
use crate::sst::block::read_u64;
use crate::sst::block::write_header;
use crate::sst::block::write_i32;
use crate::sst::block::write_i64;
use crate::sst::block::write_section_entry;
use crate::sst::block::write_u32;
use crate::sst::block::write_u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyValue {
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryKey {
    pub columns: Vec<KeyValue>,
}

impl QueryKey {
    pub fn new(columns: Vec<KeyValue>) -> Self {
        Self { columns }
    }
}

pub fn encode_leaf_block(
    batch: &RecordBatch,
    key_column_count: usize,
) -> Result<Vec<u8>, ArrowError> {
    validate_leaf_batch(batch, key_column_count)?;

    let mut sections = Vec::new();
    for column_index in 0..key_column_count {
        sections.push(encode_key_column(
            batch.column(column_index).as_ref(),
            column_index,
        )?);
    }

    let value_payload = encode_value_payload(batch, key_column_count)?;
    sections.push(EncodedSection {
        entry: SectionEntry {
            kind: SectionKind::ValueOffset,
            column_index: u16::MAX,
            physical_type: PhysicalType::U32,
            flags: 0,
            offset: 0,
            len: 0,
        },
        bytes: value_payload.offsets,
    });
    sections.push(EncodedSection {
        entry: SectionEntry {
            kind: SectionKind::ValueLen,
            column_index: u16::MAX,
            physical_type: PhysicalType::U32,
            flags: 0,
            offset: 0,
            len: 0,
        },
        bytes: value_payload.lengths,
    });
    sections.push(EncodedSection {
        entry: SectionEntry {
            kind: SectionKind::ValueBytes,
            column_index: u16::MAX,
            physical_type: PhysicalType::Binary,
            flags: 0,
            offset: 0,
            len: 0,
        },
        bytes: value_payload.bytes,
    });

    let section_table_offset = HEADER_LEN;
    let data_offset = HEADER_LEN + sections.len() * SECTION_ENTRY_LEN;
    let mut cursor = data_offset;
    for section in &mut sections {
        section.entry.offset = cursor as u64;
        section.entry.len = section.bytes.len() as u64;
        cursor += section.bytes.len();
    }

    let mut out = Vec::with_capacity(cursor);
    write_header(
        &mut out,
        Header {
            kind: BlockKind::Leaf,
            row_count: batch.num_rows() as u32,
            key_column_count: key_column_count as u16,
            section_count: sections.len() as u16,
            section_table_offset: section_table_offset as u32,
        },
    );
    for section in &sections {
        write_section_entry(&mut out, section.entry);
    }
    for section in sections {
        out.extend_from_slice(&section.bytes);
    }
    Ok(out)
}

pub struct LeafBlockReader<'a> {
    bytes: &'a [u8],
    row_count: usize,
    key_column_count: usize,
    sections: Vec<SectionEntry>,
}

impl<'a> LeafBlockReader<'a> {
    pub fn try_new(bytes: &'a [u8]) -> Result<Self, ArrowError> {
        let header = read_header(bytes)?;
        if header.kind != BlockKind::Leaf {
            return Err(ArrowError::ParseError("expected a leaf SST block".into()));
        }

        let section_table_offset = header.section_table_offset as usize;
        let section_table_len = header.section_count as usize * SECTION_ENTRY_LEN;
        if section_table_offset + section_table_len > bytes.len() {
            return Err(ArrowError::ParseError(
                "SST section table is truncated".into(),
            ));
        }

        let mut sections = Vec::with_capacity(header.section_count as usize);
        for idx in 0..header.section_count as usize {
            sections.push(read_section_entry(
                bytes,
                section_table_offset + idx * SECTION_ENTRY_LEN,
            )?);
        }

        for section in &sections {
            let offset = section.offset as usize;
            let len = section.len as usize;
            if offset + len > bytes.len() {
                return Err(ArrowError::ParseError(
                    "SST section data is truncated".into(),
                ));
            }
        }

        Ok(Self {
            bytes,
            row_count: header.row_count as usize,
            key_column_count: header.key_column_count as usize,
            sections,
        })
    }

    pub fn binary_search_in_block(&self, query: &QueryKey) -> Result<Option<usize>, ArrowError> {
        if query.columns.len() != self.key_column_count {
            return Err(ArrowError::InvalidArgumentError(format!(
                "query key arity {} does not match block key arity {}",
                query.columns.len(),
                self.key_column_count
            )));
        }

        let mut left = 0;
        let mut right = self.row_count;
        while left < right {
            let mid = left + (right - left) / 2;
            match self.compare_at(mid, query)? {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => return Ok(Some(mid)),
            }
        }
        Ok(None)
    }

    pub fn lookup(&self, query: &QueryKey) -> Result<Option<&'a [u8]>, ArrowError> {
        let Some(row) = self.binary_search_in_block(query)? else {
            return Ok(None);
        };
        self.value_at(row).map(Some)
    }

    pub fn compare_at(&self, row: usize, query: &QueryKey) -> Result<Ordering, ArrowError> {
        if row >= self.row_count {
            return Err(ArrowError::InvalidArgumentError(format!(
                "row {row} is out of bounds for leaf block with {} rows",
                self.row_count
            )));
        }
        if query.columns.len() != self.key_column_count {
            return Err(ArrowError::InvalidArgumentError(format!(
                "query key arity {} does not match block key arity {}",
                query.columns.len(),
                self.key_column_count
            )));
        }

        for (column_index, query_value) in query.columns.iter().enumerate() {
            let section = self.key_section(column_index)?;
            let ordering = compare_section_value(
                self.section_bytes(section),
                section,
                self.row_count,
                row,
                query_value,
            )?;
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }
        Ok(Ordering::Equal)
    }

    pub fn value_at(&self, row: usize) -> Result<&'a [u8], ArrowError> {
        if row >= self.row_count {
            return Err(ArrowError::InvalidArgumentError(format!(
                "row {row} is out of bounds for leaf block with {} rows",
                self.row_count
            )));
        }
        let offsets = self.single_section(SectionKind::ValueOffset)?;
        let lengths = self.single_section(SectionKind::ValueLen)?;
        let bytes = self.single_section(SectionKind::ValueBytes)?;
        let value_offset = read_u32(self.section_bytes(offsets), row * 4)? as usize;
        let value_len = read_u32(self.section_bytes(lengths), row * 4)? as usize;
        let value_bytes = self.section_bytes(bytes);
        value_bytes
            .get(value_offset..value_offset + value_len)
            .ok_or_else(|| ArrowError::ParseError("value payload is out of bounds".into()))
    }

    fn key_section(&self, column_index: usize) -> Result<&SectionEntry, ArrowError> {
        self.sections
            .iter()
            .find(|entry| {
                entry.kind == SectionKind::KeyColumn && entry.column_index as usize == column_index
            })
            .ok_or_else(|| {
                ArrowError::ParseError(format!("missing key section for column {column_index}"))
            })
    }

    fn single_section(&self, kind: SectionKind) -> Result<&SectionEntry, ArrowError> {
        self.sections
            .iter()
            .find(|entry| entry.kind == kind)
            .ok_or_else(|| ArrowError::ParseError(format!("missing section {kind:?}")))
    }

    fn section_bytes(&self, entry: &SectionEntry) -> &'a [u8] {
        let start = entry.offset as usize;
        let end = start + entry.len as usize;
        &self.bytes[start..end]
    }
}

struct EncodedSection {
    entry: SectionEntry,
    bytes: Vec<u8>,
}

struct ValuePayload {
    offsets: Vec<u8>,
    lengths: Vec<u8>,
    bytes: Vec<u8>,
}

fn validate_leaf_batch(batch: &RecordBatch, key_column_count: usize) -> Result<(), ArrowError> {
    if key_column_count == 0 {
        return Err(ArrowError::InvalidArgumentError(
            "key_column_count must be greater than zero".into(),
        ));
    }
    if key_column_count >= batch.num_columns() {
        return Err(ArrowError::InvalidArgumentError(
            "leaf block v1 expects at least one value column after key columns".into(),
        ));
    }
    if key_column_count > u16::MAX as usize {
        return Err(ArrowError::InvalidArgumentError(
            "key_column_count exceeds SST block format limit".into(),
        ));
    }
    if batch.num_rows() > u32::MAX as usize {
        return Err(ArrowError::InvalidArgumentError(
            "row count exceeds SST block format limit".into(),
        ));
    }
    for column_index in 0..key_column_count {
        if batch.column(column_index).null_count() > 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "key column {column_index} contains nulls"
            )));
        }
    }
    Ok(())
}

fn encode_key_column(array: &dyn Array, column_index: usize) -> Result<EncodedSection, ArrowError> {
    let physical_type = physical_type(array.data_type())?;
    let mut bytes = Vec::new();
    match physical_type {
        PhysicalType::I32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for row in 0..array.len() {
                write_i32(&mut bytes, array.value(row));
            }
        }
        PhysicalType::I64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..array.len() {
                write_i64(&mut bytes, array.value(row));
            }
        }
        PhysicalType::U32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            for row in 0..array.len() {
                write_u32(&mut bytes, array.value(row));
            }
        }
        PhysicalType::U64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            for row in 0..array.len() {
                write_u64(&mut bytes, array.value(row));
            }
        }
        PhysicalType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            encode_variable_values(array.len(), &mut bytes, |row| array.value(row).as_bytes())?;
        }
        PhysicalType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            encode_variable_values(array.len(), &mut bytes, |row| array.value(row).as_bytes())?;
        }
        PhysicalType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            encode_variable_values(array.len(), &mut bytes, |row| array.value(row))?;
        }
        PhysicalType::LargeBinary => {
            let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            encode_variable_values(array.len(), &mut bytes, |row| array.value(row))?;
        }
    }

    Ok(EncodedSection {
        entry: SectionEntry {
            kind: SectionKind::KeyColumn,
            column_index: column_index as u16,
            physical_type,
            flags: 0,
            offset: 0,
            len: 0,
        },
        bytes,
    })
}

fn encode_value_payload(
    batch: &RecordBatch,
    key_column_count: usize,
) -> Result<ValuePayload, ArrowError> {
    if batch.num_columns() != key_column_count + 1 {
        return Err(ArrowError::InvalidArgumentError(
            "leaf block v1 expects exactly one value payload column".into(),
        ));
    }
    let value_column = batch.column(key_column_count);
    if value_column.null_count() > 0 {
        return Err(ArrowError::InvalidArgumentError(
            "value payload column contains nulls".into(),
        ));
    }

    let mut offsets = Vec::with_capacity(batch.num_rows() * 4);
    let mut lengths = Vec::with_capacity(batch.num_rows() * 4);
    let mut bytes = Vec::new();
    match physical_type(value_column.data_type())? {
        PhysicalType::Utf8 => {
            let array = value_column.as_any().downcast_ref::<StringArray>().unwrap();
            encode_value_values(array.len(), &mut offsets, &mut lengths, &mut bytes, |row| {
                array.value(row).as_bytes()
            })?;
        }
        PhysicalType::LargeUtf8 => {
            let array = value_column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            encode_value_values(array.len(), &mut offsets, &mut lengths, &mut bytes, |row| {
                array.value(row).as_bytes()
            })?;
        }
        PhysicalType::Binary => {
            let array = value_column.as_any().downcast_ref::<BinaryArray>().unwrap();
            encode_value_values(array.len(), &mut offsets, &mut lengths, &mut bytes, |row| {
                array.value(row)
            })?;
        }
        PhysicalType::LargeBinary => {
            let array = value_column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            encode_value_values(array.len(), &mut offsets, &mut lengths, &mut bytes, |row| {
                array.value(row)
            })?;
        }
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "leaf block v1 expects value payload to be utf8/binary, got {other:?}"
            )));
        }
    }

    Ok(ValuePayload {
        offsets,
        lengths,
        bytes,
    })
}

fn encode_variable_values<'a>(
    len: usize,
    out: &mut Vec<u8>,
    value_at: impl Fn(usize) -> &'a [u8],
) -> Result<(), ArrowError> {
    let offsets_len = (len + 1) * 4;
    out.resize(offsets_len, 0);
    let mut data = Vec::new();
    for row in 0..len {
        let offset = data.len();
        if offset > u32::MAX as usize {
            return Err(ArrowError::InvalidArgumentError(
                "variable key column exceeds SST block format limit".into(),
            ));
        }
        out[row * 4..row * 4 + 4].copy_from_slice(&(offset as u32).to_le_bytes());
        data.extend_from_slice(value_at(row));
    }
    if data.len() > u32::MAX as usize {
        return Err(ArrowError::InvalidArgumentError(
            "variable key column exceeds SST block format limit".into(),
        ));
    }
    out[len * 4..len * 4 + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(&data);
    Ok(())
}

fn encode_value_values<'a>(
    len: usize,
    offsets: &mut Vec<u8>,
    lengths: &mut Vec<u8>,
    bytes: &mut Vec<u8>,
    value_at: impl Fn(usize) -> &'a [u8],
) -> Result<(), ArrowError> {
    for row in 0..len {
        let value = value_at(row);
        if bytes.len() > u32::MAX as usize || value.len() > u32::MAX as usize {
            return Err(ArrowError::InvalidArgumentError(
                "value payload exceeds SST block format limit".into(),
            ));
        }
        write_u32(offsets, bytes.len() as u32);
        write_u32(lengths, value.len() as u32);
        bytes.extend_from_slice(value);
    }
    Ok(())
}

fn compare_section_value(
    bytes: &[u8],
    section: &SectionEntry,
    row_count: usize,
    row: usize,
    query_value: &KeyValue,
) -> Result<Ordering, ArrowError> {
    match (section.physical_type, query_value) {
        (PhysicalType::I32, KeyValue::I32(query)) => Ok(read_i32(bytes, row * 4)?.cmp(query)),
        (PhysicalType::I64, KeyValue::I64(query)) => Ok(read_i64(bytes, row * 8)?.cmp(query)),
        (PhysicalType::U32, KeyValue::U32(query)) => Ok(read_u32(bytes, row * 4)?.cmp(query)),
        (PhysicalType::U64, KeyValue::U64(query)) => Ok(read_u64(bytes, row * 8)?.cmp(query)),
        (physical_type, KeyValue::Bytes(query)) if physical_type.is_variable_width() => {
            Ok(variable_value_at(bytes, row_count, row)?.cmp(query.as_slice()))
        }
        (physical_type, query) => Err(ArrowError::InvalidArgumentError(format!(
            "query value {query:?} does not match physical type {physical_type:?}"
        ))),
    }
}

fn variable_value_at(bytes: &[u8], row_count: usize, row: usize) -> Result<&[u8], ArrowError> {
    let data_start = (row_count + 1) * 4;
    if data_start > bytes.len() {
        return Err(ArrowError::ParseError(
            "variable-width offsets are truncated".into(),
        ));
    }
    let start = read_u32(bytes, row * 4)? as usize;
    let end = read_u32(bytes, (row + 1) * 4)? as usize;
    bytes
        .get(data_start + start..data_start + end)
        .ok_or_else(|| ArrowError::ParseError("variable-width value is out of bounds".into()))
}

fn physical_type(data_type: &DataType) -> Result<PhysicalType, ArrowError> {
    match data_type {
        DataType::Int32 => Ok(PhysicalType::I32),
        DataType::Int64 => Ok(PhysicalType::I64),
        DataType::UInt32 => Ok(PhysicalType::U32),
        DataType::UInt64 => Ok(PhysicalType::U64),
        DataType::Utf8 => Ok(PhysicalType::Utf8),
        DataType::LargeUtf8 => Ok(PhysicalType::LargeUtf8),
        DataType::Binary => Ok(PhysicalType::Binary),
        DataType::LargeBinary => Ok(PhysicalType::LargeBinary),
        other => Err(ArrowError::InvalidArgumentError(format!(
            "unsupported SST leaf column type: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_array::RecordBatch;
    use arrow_array::StringArray;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;

    use super::KeyValue;
    use super::LeafBlockReader;
    use super::QueryKey;
    use super::encode_leaf_block;

    #[test]
    fn encodes_and_looks_up_numeric_key() -> Result<(), arrow_schema::ArrowError> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k0", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let bytes = encode_leaf_block(&batch, 1)?;
        let reader = LeafBlockReader::try_new(&bytes)?;

        assert_eq!(
            reader.lookup(&QueryKey::new(vec![KeyValue::I32(20)]))?,
            Some("b".as_bytes())
        );
        assert_eq!(
            reader.lookup(&QueryKey::new(vec![KeyValue::I32(25)]))?,
            None
        );
        Ok(())
    }

    #[test]
    fn encodes_and_looks_up_multi_column_string_key() -> Result<(), arrow_schema::ArrowError> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k0", DataType::Int32, false),
                Field::new("k1", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["alpha", "beta", "alpha"])),
                Arc::new(StringArray::from(vec!["v1", "v2", "v3"])),
            ],
        )?;

        let bytes = encode_leaf_block(&batch, 2)?;
        let reader = LeafBlockReader::try_new(&bytes)?;

        assert_eq!(
            reader.lookup(&QueryKey::new(vec![
                KeyValue::I32(1),
                KeyValue::Bytes(b"beta".to_vec())
            ]))?,
            Some("v2".as_bytes())
        );
        assert_eq!(
            reader.lookup(&QueryKey::new(vec![
                KeyValue::I32(1),
                KeyValue::Bytes(b"delta".to_vec())
            ]))?,
            None
        );
        Ok(())
    }
}
