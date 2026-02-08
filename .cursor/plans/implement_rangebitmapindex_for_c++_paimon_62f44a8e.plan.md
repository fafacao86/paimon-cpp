---
name: Implement RangeBitmapIndex for C++ Paimon
overview: Implement RangeBitmapIndex file index for C++ Paimon, following the Java implementation. RangeBitmapIndex combines a dictionary (mapping values to codes) with a BitSliceIndex (storing codes) to enable efficient range queries on various data types.
todos:
  - id: key_factory
    content: Implement KeyFactory and type-specific factories (IntKeyFactory, BigIntKeyFactory, StringKeyFactory, etc.)
    status: pending
  - id: chunked_dictionary
    content: Implement ChunkedDictionary with Chunk, FixedLengthChunk, and VariableLengthChunk
    status: pending
  - id: bsi_bitmap
    content: Implement BitSliceIndexBitmap for code-based indexing (different from existing BitSliceIndexRoaringBitmap)
    status: pending
  - id: range_bitmap
    content: Implement RangeBitmap core class with Appender and query methods
    status: pending
  - id: file_index_writer
    content: Implement RangeBitmapFileIndexWriter (FileIndexWriter interface)
    status: pending
  - id: file_index_reader
    content: Implement RangeBitmapFileIndexReader (FileIndexReader interface)
    status: pending
  - id: file_index_factory
    content: Implement RangeBitmapFileIndexFactory and register it
    status: pending
  - id: build_integration
    content: Update CMakeLists.txt to include new source files
    status: pending
  - id: tests
    content: Create unit tests for RangeBitmap and integration tests for RangeBitmapFileIndex
    status: pending
isProject: false
---

# Implement RangeBitmapIndex for C++ Paimon

## Overview

RangeBitmapIndex is a file-level index that combines:

- **ChunkedDictionary**: Maps distinct values to integer codes, stored in chunks for memory efficiency
- **BitSliceIndexBitmap**: Stores codes using bit-sliced indexing for efficient range queries
- Supports equality (eq, neq, in, notIn) and range queries (lt, lte, gt, gte) on multiple data types

## Architecture

```
RangeBitmapFileIndex (FileIndexer)
├── RangeBitmapFileIndexFactory (FileIndexerFactory)
├── RangeBitmap (core data structure)
│   ├── ChunkedDictionary (value -> code mapping)
│   └── BitSliceIndexBitmap (code -> row bitmap)
└── KeyFactory (type-specific serialization/comparison)
```

## Implementation Plan

### Phase 1: Core Data Structures

#### 1.1 KeyFactory and Type Support (`src/paimon/common/file_index/rangebitmap/`)

**Files to create:**

- `key_factory.h` / `key_factory.cpp` - Factory for creating type-specific key handlers
- Support types: INT, BIGINT, TINYINT, SMALLINT, FLOAT, DOUBLE, BOOLEAN, STRING, DECIMAL, TIMESTAMP, DATE, TIME

**Key components:**

- `KeyFactory` interface with methods:
  - `CreateComparator()` - Returns comparator for values
  - `CreateSerializer()` - Serializes values to bytes
  - `CreateDeserializer()` - Deserializes bytes to values
  - `CreateConverter()` - Converts Literal to internal representation (e.g., Decimal to long, Timestamp to microseconds)
  - `DefaultChunkSize()` - Returns default chunk size for dictionary
- Type-specific factories: `IntKeyFactory`, `BigIntKeyFactory`, `StringKeyFactory`, `DecimalKeyFactory`, `TimestampKeyFactory`, etc.
- Reference: Java `KeyFactory.java` and its implementations

#### 1.2 ChunkedDictionary (`src/paimon/common/file_index/rangebitmap/dictionary/`)

**Files to create:**

- `dictionary.h` / `dictionary.cpp` - Dictionary interface
- `chunked_dictionary.h` / `chunked_dictionary.cpp` - Chunked implementation
- `chunk.h` / `chunk.cpp` - Individual chunk abstraction
- `fixed_length_chunk.h` / `fixed_length_chunk.cpp` - Fixed-length value chunks
- `variable_length_chunk.h` / `variable_length_chunk.cpp` - Variable-length value chunks (strings)

**Key components:**

- `Dictionary` interface:
  - `Find(Literal key) -> Result<int>` - Returns code for key, or negative insertion point if not found
  - `Find(int code) -> Result<Literal>` - Returns key for code
- `ChunkedDictionary::Appender` - Builder for creating dictionary
  - `SortedAppend(Literal key, int code)` - Append key-code pair (must be sorted)
  - `Serialize() -> Result<Bytes>` - Serialize dictionary
- `ChunkedDictionary` reader - Deserializes and provides lookup
- Reference: Java `ChunkedDictionary.java`, `Chunk.java`, `FixedLengthChunk.java`, `VariableLengthChunk.java`

#### 1.3 BitSliceIndexBitmap (`src/paimon/common/file_index/rangebitmap/`)

**Files to create:**

- `bit_slice_index_bitmap.h` / `bit_slice_index_bitmap.cpp` - BSI implementation for codes

**Key differences from existing `BitSliceIndexRoaringBitmap`:**

- Works with codes (0 to cardinality-1) instead of raw numeric values
- Supports `eq(int code)`, `gt(int code)`, `gte(int code)` operations
- `Appender` takes `(int rid, int code)` pairs
- Reference: Java `BitSliceIndexBitmap.java`

**Key components:**

- `BitSliceIndexBitmap` class:
  - Constructor from `InputStream` and offset
  - `Eq(int code) -> RoaringBitmap32`
  - `Gt(int code) -> RoaringBitmap32`
  - `Gte(int code) -> RoaringBitmap32`
  - `IsNotNull() -> RoaringBitmap32`
  - `Get(int position) -> std::optional<int>` - Get code at position
- `BitSliceIndexBitmap::Appender`:
  - Constructor: `Appender(int min_code, int max_code)` - typically (0, cardinality-1)
  - `Append(int rid, int code)`
  - `Serialize() -> Result<Bytes>`

### Phase 2: RangeBitmap Core

#### 2.1 RangeBitmap (`src/paimon/common/file_index/rangebitmap/`)

**Files to create:**

- `range_bitmap.h` / `range_bitmap.cpp` - Main RangeBitmap class

**Key components:**

- `RangeBitmap` class:
  - Constructor: `RangeBitmap(InputStream, int offset, KeyFactory)`
  - Query methods:
    - `Eq(Literal key) -> RoaringBitmap32`
    - `Neq(Literal key) -> RoaringBitmap32`
    - `Lt(Literal key) -> RoaringBitmap32`
    - `Lte(Literal key) -> RoaringBitmap32`
    - `Gt(Literal key) -> RoaringBitmap32`
    - `Gte(Literal key) -> RoaringBitmap32`
    - `In(std::vector<Literal>) -> RoaringBitmap32`
    - `NotIn(std::vector<Literal>) -> RoaringBitmap32`
    - `IsNull() -> RoaringBitmap32`
    - `IsNotNull() -> RoaringBitmap32`
  - Uses min/max bounds for early filtering
  - Converts literals using KeyFactory converter
  - Looks up codes in dictionary, then queries BSI

- `RangeBitmap::Appender`:
  - Constructor: `Appender(KeyFactory, int chunk_size_bytes)`
  - `Append(Literal key)` - Append value for current row (rid auto-increments)
  - `Serialize() -> Result<Bytes>` - Serialize complete RangeBitmap
  - Builds dictionary and BSI in one pass

**Serialization format** (matching Java):

```
[header_length: 4 bytes]
[header:
  version: 1 byte
  rid: 4 bytes (total rows)
  cardinality: 4 bytes (distinct values)
  min: variable (serialized key)
  max: variable (serialized key)
  dictionary_length: 4 bytes
]
[dictionary: variable bytes]
[bsi: variable bytes]
```

### Phase 3: FileIndex Integration

#### 3.1 RangeBitmapFileIndex (`src/paimon/common/file_index/rangebitmap/`)

**Files to create:**

- `range_bitmap_file_index.h` / `range_bitmap_file_index.cpp` - Main FileIndexer implementation
- `range_bitmap_file_index_factory.h` / `range_bitmap_file_index_factory.cpp` - Factory

**Key components:**

- `RangeBitmapFileIndex` (implements `FileIndexer`):
  - `CreateReader(ArrowSchema*, int32_t start, int32_t length, InputStream*, MemoryPool*) -> Result<FileIndexReader*>`
  - `CreateWriter(ArrowSchema*, MemoryPool*) -> Result<FileIndexWriter*>`
  - Options: `"chunk-size"` (default from KeyFactory)

- `RangeBitmapFileIndexWriter` (implements `FileIndexWriter`):
  - `AddBatch(ArrowArray*) -> Status` - Extract field values, append to RangeBitmap::Appender
  - `SerializedBytes() -> Result<Bytes>` - Serialize RangeBitmap
  - Uses KeyFactory to convert Arrow values to Literals

- `RangeBitmapFileIndexReader` (implements `FileIndexReader`):
  - Implements all `Visit*` methods from `FileIndexReader`
  - Delegates to `RangeBitmap` query methods
  - Returns `BitmapIndexResult` (reuse existing)

- `RangeBitmapFileIndexFactory` (implements `FileIndexerFactory`):
  - Identifier: `"range-bitmap"`
  - `Create(options) -> Result<FileIndexer*>`
  - Registered via `REGISTER_PAIMON_FACTORY`

### Phase 4: Integration and Testing

#### 4.1 Build System Updates

**Files to modify:**

- `src/paimon/common/file_index/CMakeLists.txt` - Add new source files:
  ```
  rangebitmap/range_bitmap_file_index.cpp
  rangebitmap/range_bitmap_file_index_factory.cpp
  rangebitmap/range_bitmap.cpp
  rangebitmap/bit_slice_index_bitmap.cpp
  rangebitmap/dictionary/chunked_dictionary.cpp
  rangebitmap/dictionary/chunk.cpp
  rangebitmap/dictionary/fixed_length_chunk.cpp
  rangebitmap/dictionary/variable_length_chunk.cpp
  rangebitmap/key_factory.cpp
  ```


#### 4.2 Header Installation

**Files to create/update:**

- `include/paimon/file_index/range_bitmap_file_index.h` - Public header (if needed)
- Ensure all headers follow existing patterns

#### 4.3 Testing

**Files to create:**

- `src/paimon/common/file_index/rangebitmap/range_bitmap_test.cpp` - Unit tests for RangeBitmap
- `src/paimon/common/file_index/rangebitmap/range_bitmap_file_index_test.cpp` - Integration tests
- Test cases:
  - Serialization/deserialization
  - All query types (eq, neq, lt, lte, gt, gte, in, notIn, isNull, isNotNull)
  - Multiple data types
  - Edge cases (empty, single value, all nulls, etc.)

## Implementation Notes

1. **Literal Conversion**: Use existing `Literal` class and `LiteralConverter` for type conversions. KeyFactory converters should work with Literals.

2. **Memory Management**: Use `MemoryPool` for allocations. Follow existing patterns from `BitmapFileIndex`.

3. **Error Handling**: Use `Result<T>` pattern consistently. Return appropriate errors for invalid inputs.

4. **Serialization Compatibility**: Match Java serialization format exactly for cross-language compatibility.

5. **Type Support**: Start with numeric types (INT, BIGINT, FLOAT, DOUBLE) and BOOLEAN, then add STRING, DECIMAL, TIMESTAMP.

6. **Reuse Existing Code**:

   - `RoaringBitmap32` for bitmap operations
   - `InputStream` / `OutputStream` interfaces
   - `Bytes` for serialized data
   - `BitmapIndexResult` for query results

## File Structure

```
src/paimon/common/file_index/rangebitmap/
├── range_bitmap_file_index.h
├── range_bitmap_file_index.cpp
├── range_bitmap_file_index_factory.h
├── range_bitmap_file_index_factory.cpp
├── range_bitmap.h
├── range_bitmap.cpp
├── bit_slice_index_bitmap.h
├── bit_slice_index_bitmap.cpp
├── key_factory.h
├── key_factory.cpp
└── dictionary/
    ├── dictionary.h
    ├── dictionary.cpp
    ├── chunked_dictionary.h
    ├── chunked_dictionary.cpp
    ├── chunk.h
    ├── chunk.cpp
    ├── fixed_length_chunk.h
    ├── fixed_length_chunk.cpp
    ├── variable_length_chunk.h
    └── variable_length_chunk.cpp
```

## References

- Java implementation: `/home/xiaoheng/IdeaProjects/paimon/paimon-common/src/main/java/org/apache/paimon/fileindex/rangebitmap/`
- Design doc: https://docs.google.com/document/d/14YXPtCUmvjwozdLhgWJdPgHrVYOTdv9uiC1N2GlNvG4/edit
- Existing C++ implementations: `BitmapFileIndex`, `BitSliceIndexBitmapFileIndex`