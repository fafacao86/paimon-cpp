/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"

#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/c/bridge.h>

#include "paimon/common/utils/field_type_utils.h"

#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/common/file_index/rangebitmap/range_bitmap.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

RangeBitmapFileIndex::RangeBitmapFileIndex(const std::map<std::string, std::string>& options)
    : options_(options) {
}

Result<std::shared_ptr<FileIndexReader>> RangeBitmapFileIndex::CreateReader(
    ::ArrowSchema* arrow_schema, int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
  // Extract arrow type from schema - simplified
  std::shared_ptr<arrow::DataType> arrow_type;  // TODO: Extract from arrow_schema

  return RangeBitmapFileIndexReader::Create(arrow_type, start, length, input_stream, pool);
}

Result<std::shared_ptr<FileIndexWriter>> RangeBitmapFileIndex::CreateWriter(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const {
  return Status::NotImplemented("RangeBitmapFileIndex::CreateWriter");
}

// RangeBitmapFileIndexWriter implementation
Result<std::shared_ptr<RangeBitmapFileIndexWriter>> RangeBitmapFileIndexWriter::Create(
    const std::shared_ptr<arrow::Schema>& arrow_schema, const std::string& field_name,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {

  // Find the field in the schema
  auto field = arrow_schema->GetFieldByName(field_name);
  if (!field) {
    return Status::Invalid("Field not found in schema: " + field_name);
  }

  // Convert arrow type to FieldType
  PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                         FieldTypeUtils::ConvertToFieldType(field->type()->id()));

  // Initialize the appender
  int32_t chunk_size = 16 * 1024;  // 16KB default
  auto chunk_size_it = options.find(RangeBitmapFileIndex::CHUNK_SIZE);
  if (chunk_size_it != options.end()) {
    chunk_size = std::stoi(chunk_size_it->second);
  }

  return std::shared_ptr<RangeBitmapFileIndexWriter>(
      new RangeBitmapFileIndexWriter(field->type(), field_type, options, pool, chunk_size));
}

RangeBitmapFileIndexWriter::RangeBitmapFileIndexWriter(
    std::shared_ptr<arrow::DataType> arrow_type,
    FieldType field_type,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool,
    int32_t chunk_size)
    : arrow_type_(std::move(arrow_type)),
      field_type_(field_type),
      options_(options),
      pool_(pool) {
  appender_ = std::make_unique<RangeBitmap::Appender>(field_type_, chunk_size);
}

Status RangeBitmapFileIndexWriter::AddBatch(::ArrowArray* batch) {
  if (!batch || !appender_) {
    return Status::Invalid("Invalid batch or uninitialized appender");
  }

  // TODO: Convert ArrowArray to Literals and append
  // This is simplified - in practice we'd iterate through the array
  // and convert each value to a Literal
  for (int64_t i = 0; i < batch->length; ++i) {
    // Placeholder - convert array element to Literal
    // Literal literal;  // This needs proper conversion
    // auto status = appender_->Append(literal);
    // if (!status.ok()) {
    //   return status;
    // }
  }

  return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> RangeBitmapFileIndexWriter::SerializedBytes() const {
  if (!appender_) {
    return Status::Invalid("Appender not initialized");
  }

  return appender_->Serialize(pool_.get());
}

// RangeBitmapFileIndexReader implementation
Result<std::shared_ptr<RangeBitmapFileIndexReader>> RangeBitmapFileIndexReader::Create(
    const std::shared_ptr<arrow::DataType>& arrow_type,
    int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) {

  if (!arrow_type || !input_stream || !pool) {
    return Status::Invalid("RangeBitmapFileIndexReader::Create: null argument");
  }

  PAIMON_ASSIGN_OR_RAISE(const FieldType field_type,
                         FieldTypeUtils::ConvertToFieldType(arrow_type->id()));

  PAIMON_ASSIGN_OR_RAISE(auto range_bitmap,
                         RangeBitmap::Create(input_stream, start,
                                                   length,
                                                   field_type, pool.get()));

  return std::shared_ptr<RangeBitmapFileIndexReader>(
      new RangeBitmapFileIndexReader(std::move(range_bitmap)));
}

RangeBitmapFileIndexReader::RangeBitmapFileIndexReader(
    std::unique_ptr<RangeBitmap> range_bitmap)
    : range_bitmap_(std::move(range_bitmap)) {
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitEqual(const Literal& literal) {
  if (!range_bitmap_) {
    // TODO: Implement RangeBitmap creation
    return std::make_shared<BitmapIndexResult>([]() -> Result<RoaringBitmap32> {
      return RoaringBitmap32();
    });
  }

  auto result = range_bitmap_->Eq(literal);
  if (!result.ok()) {
    return result.status();
  }

  return std::make_shared<BitmapIndexResult>(
      [result = std::move(result)]() -> Result<RoaringBitmap32> {
        return result;
      });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitNotEqual(const Literal& literal) {
  if (!range_bitmap_) {
    return std::make_shared<BitmapIndexResult>([]() { return RoaringBitmap32(); });
  }
  auto result = range_bitmap_->Neq(literal);
  if (!result.ok()) return result.status();
  return std::make_shared<BitmapIndexResult>(
      [r = std::move(result)]() -> Result<RoaringBitmap32> { return r; });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIn(const std::vector<Literal>& literals) {
  if (!range_bitmap_) {
    return std::make_shared<BitmapIndexResult>([]() { return RoaringBitmap32(); });
  }
  auto result = range_bitmap_->In(literals);
  if (!result.ok()) return result.status();
  return std::make_shared<BitmapIndexResult>(
      [r = std::move(result)]() -> Result<RoaringBitmap32> { return r; });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitNotIn(const std::vector<Literal>& literals) {
  if (!range_bitmap_) {
    return std::make_shared<BitmapIndexResult>([]() { return RoaringBitmap32(); });
  }
  auto result = range_bitmap_->NotIn(literals);
  if (!result.ok()) return result.status();
  return std::make_shared<BitmapIndexResult>(
      [r = std::move(result)]() -> Result<RoaringBitmap32> { return r; });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIsNull() {
  if (!range_bitmap_) {
    return std::make_shared<BitmapIndexResult>([]() { return RoaringBitmap32(); });
  }
  auto result = range_bitmap_->IsNull();
  if (!result.ok()) return result.status();
  return std::make_shared<BitmapIndexResult>(
      [r = std::move(result)]() -> Result<RoaringBitmap32> { return r; });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIsNotNull() {
  if (!range_bitmap_) {
    return std::make_shared<BitmapIndexResult>([]() { return RoaringBitmap32(); });
  }
  auto result = range_bitmap_->IsNotNull();
  if (!result.ok()) return result.status();
  return std::make_shared<BitmapIndexResult>(
      [r = std::move(result)]() -> Result<RoaringBitmap32> { return r; });
}

}  // namespace paimon