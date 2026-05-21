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

use arrow_schema::ArrowError;

pub(crate) const MAGIC: &[u8; 4] = b"VXST";
pub(crate) const VERSION: u16 = 1;
pub(crate) const HEADER_LEN: usize = 20;
pub(crate) const SECTION_ENTRY_LEN: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum BlockKind {
    Leaf = 1,
}

impl BlockKind {
    pub(crate) fn from_u8(value: u8) -> Result<Self, ArrowError> {
        match value {
            1 => Ok(Self::Leaf),
            _ => Err(ArrowError::ParseError(format!(
                "unsupported SST block kind: {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum SectionKind {
    KeyColumn = 1,
    ValueOffset = 4,
    ValueLen = 5,
    ValueBytes = 6,
}

impl SectionKind {
    pub(crate) fn from_u16(value: u16) -> Result<Self, ArrowError> {
        match value {
            1 => Ok(Self::KeyColumn),
            4 => Ok(Self::ValueOffset),
            5 => Ok(Self::ValueLen),
            6 => Ok(Self::ValueBytes),
            _ => Err(ArrowError::ParseError(format!(
                "unsupported SST section kind: {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum PhysicalType {
    I32 = 1,
    I64 = 2,
    U32 = 3,
    U64 = 4,
    Utf8 = 5,
    LargeUtf8 = 6,
    Binary = 7,
    LargeBinary = 8,
}

impl PhysicalType {
    pub(crate) fn from_u16(value: u16) -> Result<Self, ArrowError> {
        match value {
            1 => Ok(Self::I32),
            2 => Ok(Self::I64),
            3 => Ok(Self::U32),
            4 => Ok(Self::U64),
            5 => Ok(Self::Utf8),
            6 => Ok(Self::LargeUtf8),
            7 => Ok(Self::Binary),
            8 => Ok(Self::LargeBinary),
            _ => Err(ArrowError::ParseError(format!(
                "unsupported SST physical type: {value}"
            ))),
        }
    }

    pub(crate) fn is_variable_width(self) -> bool {
        matches!(
            self,
            Self::Utf8 | Self::LargeUtf8 | Self::Binary | Self::LargeBinary
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SectionEntry {
    pub(crate) kind: SectionKind,
    pub(crate) column_index: u16,
    pub(crate) physical_type: PhysicalType,
    pub(crate) flags: u16,
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Header {
    pub(crate) kind: BlockKind,
    pub(crate) row_count: u32,
    pub(crate) key_column_count: u16,
    pub(crate) section_count: u16,
    pub(crate) section_table_offset: u32,
}

pub(crate) fn write_header(out: &mut Vec<u8>, header: Header) {
    out.extend_from_slice(MAGIC);
    write_u16(out, VERSION);
    out.push(header.kind as u8);
    out.push(0);
    write_u32(out, header.row_count);
    write_u16(out, header.key_column_count);
    write_u16(out, header.section_count);
    write_u32(out, header.section_table_offset);
}

pub(crate) fn read_header(bytes: &[u8]) -> Result<Header, ArrowError> {
    if bytes.len() < HEADER_LEN {
        return Err(ArrowError::ParseError(
            "SST block header is truncated".into(),
        ));
    }
    if &bytes[0..4] != MAGIC {
        return Err(ArrowError::ParseError("invalid SST block magic".into()));
    }
    let version = read_u16(bytes, 4)?;
    if version != VERSION {
        return Err(ArrowError::ParseError(format!(
            "unsupported SST block version: {version}"
        )));
    }
    Ok(Header {
        kind: BlockKind::from_u8(bytes[6])?,
        row_count: read_u32(bytes, 8)?,
        key_column_count: read_u16(bytes, 12)?,
        section_count: read_u16(bytes, 14)?,
        section_table_offset: read_u32(bytes, 16)?,
    })
}

pub(crate) fn write_section_entry(out: &mut Vec<u8>, entry: SectionEntry) {
    write_u16(out, entry.kind as u16);
    write_u16(out, entry.column_index);
    write_u16(out, entry.physical_type as u16);
    write_u16(out, entry.flags);
    write_u64(out, entry.offset);
    write_u64(out, entry.len);
}

pub(crate) fn read_section_entry(bytes: &[u8], offset: usize) -> Result<SectionEntry, ArrowError> {
    if offset + SECTION_ENTRY_LEN > bytes.len() {
        return Err(ArrowError::ParseError(
            "SST section table entry is truncated".into(),
        ));
    }
    Ok(SectionEntry {
        kind: SectionKind::from_u16(read_u16(bytes, offset)?)?,
        column_index: read_u16(bytes, offset + 2)?,
        physical_type: PhysicalType::from_u16(read_u16(bytes, offset + 4)?)?,
        flags: read_u16(bytes, offset + 6)?,
        offset: read_u64(bytes, offset + 8)?,
        len: read_u64(bytes, offset + 16)?,
    })
}

pub(crate) fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, ArrowError> {
    let slice = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| ArrowError::ParseError("u16 read out of bounds".into()))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, ArrowError> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| ArrowError::ParseError("u32 read out of bounds".into()))?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

pub(crate) fn read_i32(bytes: &[u8], offset: usize) -> Result<i32, ArrowError> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| ArrowError::ParseError("i32 read out of bounds".into()))?;
    Ok(i32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, ArrowError> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| ArrowError::ParseError("u64 read out of bounds".into()))?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

pub(crate) fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, ArrowError> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| ArrowError::ParseError("i64 read out of bounds".into()))?;
    Ok(i64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}
