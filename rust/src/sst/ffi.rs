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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::ptr::null;
use std::ptr::read;
use std::slice;

use arrow_array::Array;
use arrow_array::BinaryArray;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::LargeBinaryArray;
use arrow_array::LargeStringArray;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::StructArray;
use arrow_array::UInt32Array;
use arrow_array::UInt64Array;
use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::ffi::from_ffi;
use arrow_array::make_array;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Schema;
use arrow_schema::ffi::FFI_ArrowSchema;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

use crate::sst::leaf::KeyValue;
use crate::sst::leaf::LeafBlockReader;
use crate::sst::leaf::QueryKey;
use crate::sst::leaf::encode_leaf_block;

/// Build options shared between C++ and Rust.
///
/// The caller is expected to pass one sorted batch representing exactly one leaf block's logical
/// row set. Rust then encodes that batch into one serialized block payload.
#[repr(C)]
pub struct VortexSstLeafBuildOptions {
    /// Number of leading columns in the input batch that belong to the primary key.
    pub key_column_count: u32,
    /// Prefix length used by string key columns on the hot compare path.
    pub string_prefix_len: u32,
}

/// Owned byte buffer returned across the FFI boundary.
///
/// C++ must return the pointer to `vortex_sst_free_owned_bytes` after use.
#[repr(C)]
pub struct VortexOwnedBytes {
    pub data: *const u8,
    pub len: usize,
}

impl VortexOwnedBytes {
    fn empty() -> Self {
        Self {
            data: null(),
            len: 0,
        }
    }
}

/// Lookup result returned across the FFI boundary.
#[repr(C)]
pub struct VortexSstLookupResult {
    pub found: u8,
    pub value: VortexOwnedBytes,
}

impl VortexSstLookupResult {
    fn not_found() -> Self {
        Self {
            found: 0,
            value: VortexOwnedBytes::empty(),
        }
    }
}

/// Status codes for the narrow C ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VortexSstStatusCode {
    Ok = 0,
    NullPointer = 1,
    InvalidInput = 2,
    InternalError = 3,
}

/// Import one Arrow C batch and encode one SST leaf block.
///
/// This function is intentionally batch-oriented:
/// - one FFI call per logical leaf block
/// - zero-copy Arrow import on the input boundary
/// - one owned encoded output buffer on the return boundary
///
/// The current implementation writes a v1 sectioned leaf block. Key columns are stored as typed
/// random-access sections, while the value payload is stored as offset/length plus a byte arena.
///
/// Ownership contract:
/// - the caller transfers ownership of `ArrowSchema` and `ArrowArray` into this call
/// - Rust consumes those FFI structs during Arrow import
///
/// # Safety
///
/// `schema`, `array`, `options`, and `out_bytes` must be valid pointers. `schema` and `array` must
/// point to live Arrow C Data Interface wrappers whose ownership is transferred to this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vortex_sst_build_leaf_block_from_ffi(
    schema: *const FFI_ArrowSchema,
    array: *const FFI_ArrowArray,
    options: *const VortexSstLeafBuildOptions,
    out_bytes: *mut VortexOwnedBytes,
) -> VortexSstStatusCode {
    match catch_unwind(AssertUnwindSafe(|| {
        if schema.is_null() || array.is_null() || options.is_null() || out_bytes.is_null() {
            return VortexSstStatusCode::NullPointer;
        }

        let schema = unsafe { read(schema) };
        let array = unsafe { read(array) };
        let options = unsafe { &*options };

        match build_leaf_block_from_ffi(&schema, array, options) {
            Ok(bytes) => {
                unsafe { *out_bytes = bytes };
                VortexSstStatusCode::Ok
            }
            Err(_) => {
                unsafe { *out_bytes = VortexOwnedBytes::empty() };
                VortexSstStatusCode::InvalidInput
            }
        }
    })) {
        Ok(status) => status,
        Err(_) => {
            if !out_bytes.is_null() {
                unsafe { *out_bytes = VortexOwnedBytes::empty() };
            }
            VortexSstStatusCode::InternalError
        }
    }
}

/// Lookup one key in one encoded SST leaf block.
///
/// `key_schema` and `key_array` must describe a one-row StructArray whose columns match the leaf
/// block key columns. Rust consumes the Arrow FFI structs during import.
///
/// # Safety
///
/// `block_data` must point to `block_len` readable bytes. `key_schema`, `key_array`, and `out`
/// must be valid pointers. `key_schema` and `key_array` ownership is transferred to this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vortex_sst_lookup_leaf_block(
    block_data: *const u8,
    block_len: usize,
    key_schema: *const FFI_ArrowSchema,
    key_array: *const FFI_ArrowArray,
    out: *mut VortexSstLookupResult,
) -> VortexSstStatusCode {
    match catch_unwind(AssertUnwindSafe(|| {
        if block_data.is_null() || key_schema.is_null() || key_array.is_null() || out.is_null() {
            return VortexSstStatusCode::NullPointer;
        }

        let block = unsafe { slice::from_raw_parts(block_data, block_len) };
        let key_schema = unsafe { read(key_schema) };
        let key_array = unsafe { read(key_array) };

        match lookup_leaf_block(block, &key_schema, key_array) {
            Ok(Some(value)) => {
                unsafe {
                    *out = VortexSstLookupResult {
                        found: 1,
                        value: owned_bytes(value.to_vec()),
                    }
                };
                VortexSstStatusCode::Ok
            }
            Ok(None) => {
                unsafe { *out = VortexSstLookupResult::not_found() };
                VortexSstStatusCode::Ok
            }
            Err(_) => {
                unsafe { *out = VortexSstLookupResult::not_found() };
                VortexSstStatusCode::InvalidInput
            }
        }
    })) {
        Ok(status) => status,
        Err(_) => {
            if !out.is_null() {
                unsafe { *out = VortexSstLookupResult::not_found() };
            }
            VortexSstStatusCode::InternalError
        }
    }
}

/// Release a buffer previously returned by `vortex_sst_build_leaf_block_from_ffi`.
///
/// # Safety
///
/// `bytes` must either be empty or must be a buffer previously returned by this Rust library and
/// not yet released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vortex_sst_free_owned_bytes(bytes: VortexOwnedBytes) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if bytes.data.is_null() || bytes.len == 0 {
            return;
        }

        let slice = unsafe { slice::from_raw_parts_mut(bytes.data as *mut u8, bytes.len) };
        let _ = unsafe { Box::<[u8]>::from_raw(slice) };
    }));
}

fn build_leaf_block_from_ffi(
    schema: &FFI_ArrowSchema,
    array: FFI_ArrowArray,
    options: &VortexSstLeafBuildOptions,
) -> Result<VortexOwnedBytes, ArrowError> {
    let array_data = unsafe { from_ffi(array, schema) }?;
    let array = make_array(array_data);
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("expected a StructArray batch".into()))?;
    let schema = Schema::try_from(schema)?;
    let batch = RecordBatch::from(struct_array.clone());

    validate_batch_contract(&schema, &batch, options)?;

    validate_batch_contract(&schema, &batch, options)?;

    // Establish the intended Vortex boundary early. The v1 block format is deliberately small and
    // sectioned; future patches can swap key sections to Vortex encodings behind this boundary.
    let _session = VortexSession::default();

    let bytes = encode_leaf_block(&batch, options.key_column_count as usize)?;
    Ok(owned_bytes(bytes))
}

fn lookup_leaf_block<'a>(
    block: &'a [u8],
    key_schema: &FFI_ArrowSchema,
    key_array: FFI_ArrowArray,
) -> Result<Option<&'a [u8]>, ArrowError> {
    let key_data = unsafe { from_ffi(key_array, key_schema) }?;
    let key_array = make_array(key_data);
    let struct_array = key_array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("expected a StructArray key".into()))?;
    let batch = RecordBatch::from(struct_array.clone());
    let query = query_key_from_batch(&batch)?;
    let reader = LeafBlockReader::try_new(block)?;
    reader.lookup(&query)
}

fn validate_batch_contract(
    schema: &Schema,
    batch: &RecordBatch,
    options: &VortexSstLeafBuildOptions,
) -> Result<(), ArrowError> {
    if options.key_column_count == 0 {
        return Err(ArrowError::InvalidArgumentError(
            "key_column_count must be greater than zero".into(),
        ));
    }

    if options.key_column_count as usize > schema.fields().len() {
        return Err(ArrowError::InvalidArgumentError(
            "key_column_count exceeds batch column count".into(),
        ));
    }

    if batch.num_columns() != schema.fields().len() {
        return Err(ArrowError::InvalidArgumentError(
            "record batch column count does not match schema".into(),
        ));
    }

    Ok(())
}

fn query_key_from_batch(batch: &RecordBatch) -> Result<QueryKey, ArrowError> {
    if batch.num_rows() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "lookup key batch must contain exactly one row".into(),
        ));
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        if column.null_count() > 0 {
            return Err(ArrowError::InvalidArgumentError(
                "lookup key must not contain nulls".into(),
            ));
        }
        columns.push(key_value_from_array(column.as_ref())?);
    }
    Ok(QueryKey::new(columns))
}

fn key_value_from_array(array: &dyn Array) -> Result<KeyValue, ArrowError> {
    match array.data_type() {
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(KeyValue::I32(array.value(0)))
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(KeyValue::I64(array.value(0)))
        }
        DataType::UInt32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(KeyValue::U32(array.value(0)))
        }
        DataType::UInt64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(KeyValue::U64(array.value(0)))
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(KeyValue::Bytes(array.value(0).as_bytes().to_vec()))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(KeyValue::Bytes(array.value(0).as_bytes().to_vec()))
        }
        DataType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(KeyValue::Bytes(array.value(0).to_vec()))
        }
        DataType::LargeBinary => {
            let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(KeyValue::Bytes(array.value(0).to_vec()))
        }
        other => Err(ArrowError::InvalidArgumentError(format!(
            "unsupported lookup key type: {other:?}"
        ))),
    }
}

fn owned_bytes(bytes: Vec<u8>) -> VortexOwnedBytes {
    let boxed = bytes.into_boxed_slice();
    let len = boxed.len();
    let data = Box::into_raw(boxed) as *const u8;
    VortexOwnedBytes { data, len }
}
