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

#include "paimon/common/file_index/rangebitmap/range_bitmap.h"

#include <algorithm>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/memory/bytes.h"

namespace paimon {

// --- Literal serialization: same pattern as bitmap_file_index_meta.cpp ---

static Result<std::function<Result<Literal>()>> GetValueReader(
    FieldType field_type, const std::shared_ptr<DataInputStream>& in, MemoryPool* pool) {
  switch (field_type) {
    case FieldType::BOOLEAN: {
      return std::function<Result<Literal>()>([in]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(bool value, in->ReadValue<bool>());
        return Literal(value);
      });
    }
    case FieldType::TINYINT: {
      return std::function<Result<Literal>()>([in]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(int8_t value, in->ReadValue<int8_t>());
        return Literal(value);
      });
    }
    case FieldType::SMALLINT: {
      return std::function<Result<Literal>()>([in]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(int16_t value, in->ReadValue<int16_t>());
        return Literal(value);
      });
    }
    case FieldType::DATE:
    case FieldType::INT: {
      return std::function<Result<Literal>()>([in, field_type]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(int32_t value, in->ReadValue<int32_t>());
        if (field_type == FieldType::DATE) {
          return Literal(FieldType::DATE, value);
        }
        return Literal(value);
      });
    }
    case FieldType::BIGINT: {
      return std::function<Result<Literal>()>([in]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(int64_t value, in->ReadValue<int64_t>());
        return Literal(value);
      });
    }
    case FieldType::FLOAT: {
      return std::function<Result<Literal>()>([in]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(float value, in->ReadValue<float>());
        return Literal(value);
      });
    }
    case FieldType::DOUBLE: {
      return std::function<Result<Literal>()>([in]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(double value, in->ReadValue<double>());
        return Literal(value);
      });
    }
    case FieldType::STRING: {
      return std::function<Result<Literal>()>([in, field_type, pool]() -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(int32_t length, in->ReadValue<int32_t>());
        auto bytes = Bytes::AllocateBytes(length, pool);
        PAIMON_RETURN_NOT_OK(in->ReadBytes(bytes.get()));
        return Literal(field_type, bytes->data(), bytes->size());
      });
    }
    default:
      return Status::Invalid(
          fmt::format("unsupported field type for RangeBitmap: {}",
                      FieldTypeUtils::FieldTypeToString(field_type)));
  }
}

static Result<std::function<Status(const Literal&)>> GetValueWriter(
    FieldType field_type,
    const std::shared_ptr<MemorySegmentOutputStream>& out) {
  switch (field_type) {
    case FieldType::BOOLEAN:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            out->WriteValue<bool>(literal.GetValue<bool>());
            return Status::OK();
          });
    case FieldType::TINYINT:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            out->WriteValue<int8_t>(literal.GetValue<int8_t>());
            return Status::OK();
          });
    case FieldType::SMALLINT:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            out->WriteValue<int16_t>(literal.GetValue<int16_t>());
            return Status::OK();
          });
    case FieldType::DATE:
    case FieldType::INT:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            out->WriteValue<int32_t>(literal.GetValue<int32_t>());
            return Status::OK();
          });
    case FieldType::BIGINT:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            out->WriteValue<int64_t>(literal.GetValue<int64_t>());
            return Status::OK();
          });
    case FieldType::FLOAT:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            out->WriteValue<float>(literal.GetValue<float>());
            return Status::OK();
          });
    case FieldType::DOUBLE:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            out->WriteValue<double>(literal.GetValue<double>());
            return Status::OK();
          });
    case FieldType::STRING:
      return std::function<Status(const Literal&)>(
          [out](const Literal& literal) -> Status {
            auto value = literal.GetValue<std::string>();
            out->WriteValue<uint32_t>(value.size());
            out->Write(value.data(), value.size());
            return Status::OK();
          });
    default:
      return Status::Invalid(
          fmt::format("unsupported field type for RangeBitmap: {}",
                      FieldTypeUtils::FieldTypeToString(field_type)));
  }
}

// --- RangeBitmap ---

RangeBitmap::RangeBitmap(FieldType field_type, std::vector<Literal> keys,
                         std::shared_ptr<BitSliceIndexBitmap> bsi,
                         RoaringBitmap32 null_bitmap, std::optional<Literal> min,
                         std::optional<Literal> max)
    : field_type_(field_type),
      keys_(std::move(keys)),
      bsi_(std::move(bsi)),
      null_bitmap_(std::move(null_bitmap)),
      min_(std::move(min)),
      max_(std::move(max)) {}

Result<std::unique_ptr<RangeBitmap>> RangeBitmap::Deserialize(
    const std::shared_ptr<InputStream>& input_stream, int64_t offset, int64_t length,
    FieldType field_type, MemoryPool* pool) {
  PAIMON_RETURN_NOT_OK(input_stream->Seek(offset, SeekOrigin::FS_SEEK_SET));
  auto data_in = std::make_shared<DataInputStream>(input_stream);

  PAIMON_ASSIGN_OR_RAISE(int8_t version, data_in->ReadValue<int8_t>());
  if (version != VERSION) {
    return Status::Invalid(
        fmt::format("RangeBitmap unsupported version {} (expected {})", version, VERSION));
  }

  PAIMON_ASSIGN_OR_RAISE(int32_t num_keys, data_in->ReadValue<int32_t>());
  PAIMON_ASSIGN_OR_RAISE(auto value_reader, GetValueReader(field_type, data_in, pool));

  std::vector<Literal> keys;
  keys.reserve(num_keys);
  for (int32_t i = 0; i < num_keys; ++i) {
    PAIMON_ASSIGN_OR_RAISE(Literal key, value_reader());
    keys.push_back(std::move(key));
  }

  PAIMON_ASSIGN_OR_RAISE(int32_t null_bitmap_size, data_in->ReadValue<int32_t>());
  std::string null_bitmap_buf(null_bitmap_size, '\0');
  PAIMON_RETURN_NOT_OK(data_in->Read(null_bitmap_buf.data(), null_bitmap_size));

  RoaringBitmap32 null_bitmap;
  PAIMON_RETURN_NOT_OK(
      null_bitmap.Deserialize(null_bitmap_buf.data(), null_bitmap_buf.size()));

  PAIMON_ASSIGN_OR_RAISE(int8_t has_min, data_in->ReadValue<int8_t>());
  std::optional<Literal> min_val;
  if (has_min != 0) {
    PAIMON_ASSIGN_OR_RAISE(Literal m, value_reader());
    min_val = std::move(m);
  }

  PAIMON_ASSIGN_OR_RAISE(int8_t has_max, data_in->ReadValue<int8_t>());
  std::optional<Literal> max_val;
  if (has_max != 0) {
    PAIMON_ASSIGN_OR_RAISE(Literal m, value_reader());
    max_val = std::move(m);
  }

  PAIMON_ASSIGN_OR_RAISE(int32_t bsi_size, data_in->ReadValue<int32_t>());
  std::string bsi_buf(bsi_size, '\0');
  PAIMON_RETURN_NOT_OK(data_in->Read(bsi_buf.data(), bsi_size));

  auto bsi_stream = std::make_shared<ByteArrayInputStream>(bsi_buf.data(), bsi_buf.size());
  PAIMON_ASSIGN_OR_RAISE(auto bsi, BitSliceIndexBitmap::Create(bsi_stream));

  return std::unique_ptr<RangeBitmap>(new RangeBitmap(
      field_type, std::move(keys), std::move(bsi), std::move(null_bitmap),
      std::move(min_val), std::move(max_val)));
}

Result<int32_t> RangeBitmap::FindCode(const Literal& key) const {
  auto it = std::lower_bound(keys_.begin(), keys_.end(), key,
                             [](const Literal& a, const Literal& b) {
                               auto r = a.CompareTo(b);
                               return r.ok() && r.value() < 0;
                             });
  if (it == keys_.end()) return -1;
  auto cmp = it->CompareTo(key);
  if (!cmp.ok() || cmp.value() != 0) return -1;
  return static_cast<int32_t>(std::distance(keys_.begin(), it));
}

Result<RoaringBitmap32> RangeBitmap::Eq(const Literal& key) const {
  if (key.IsNull()) return Result<RoaringBitmap32>(null_bitmap_);
  auto code = FindCode(key);
  if (code < 0) return RoaringBitmap32();
  return bsi_->Eq(code);
}

Result<RoaringBitmap32> RangeBitmap::Neq(const Literal& key) const {
  if (key.IsNull()) {
    RoaringBitmap32 all = bsi_->IsNotNull();
    all -= null_bitmap_;
    return all;
  }
  auto code = FindCode(key);
  if (code < 0) return bsi_->IsNotNull();
  RoaringBitmap32 eq_bitmap;
  auto eq = bsi_->Eq(code);
  if (eq.ok()) eq_bitmap = eq.value();
  RoaringBitmap32 result = bsi_->IsNotNull();
  result -= eq_bitmap;
  return result;
}

Result<RoaringBitmap32> RangeBitmap::Lt(const Literal& key) const {
  if (key.IsNull()) return RoaringBitmap32();
  // All rows with code < code_of(key). We need Gte then negate, or iterate.
  // BSI has Gt, Gte. Lt(key) = not Gte(key) & IsNotNull.
  auto gte = Gte(key);
  if (!gte.ok()) return gte.status();
  RoaringBitmap32 result = bsi_->IsNotNull();
  result -= gte.value();
  return result;
}

Result<RoaringBitmap32> RangeBitmap::Lte(const Literal& key) const {
  auto lt = Lt(key);
  if (!lt.ok()) return lt.status();
  auto eq = Eq(key);
  if (!eq.ok()) return eq.status();
  RoaringBitmap32 result = lt.value();
  result |= eq.value();
  return result;
}

Result<RoaringBitmap32> RangeBitmap::Gt(const Literal& key) const {
  if (key.IsNull()) return RoaringBitmap32();
  auto code = FindCode(key);
  if (code < 0) return bsi_->IsNotNull();
  return bsi_->Gt(code);
}

Result<RoaringBitmap32> RangeBitmap::Gte(const Literal& key) const {
  if (key.IsNull()) return RoaringBitmap32();
  auto code = FindCode(key);
  if (code < 0) return RoaringBitmap32();
  return bsi_->Gte(code);
}

Result<RoaringBitmap32> RangeBitmap::In(const std::vector<Literal>& keys) const {
  RoaringBitmap32 result;
  for (const auto& key : keys) {
    if (key.IsNull()) continue;
    auto eq = Eq(key);
    if (eq.ok()) result |= eq.value();
  }
  return result;
}

Result<RoaringBitmap32> RangeBitmap::NotIn(const std::vector<Literal>& keys) const {
  RoaringBitmap32 in_result;
  for (const auto& key : keys) {
    if (key.IsNull()) continue;
    auto eq = Eq(key);
    if (eq.ok()) in_result |= eq.value();
  }
  RoaringBitmap32 result = bsi_->IsNotNull();
  result -= in_result;
  return result;
}

Result<RoaringBitmap32> RangeBitmap::IsNull() const {
  return Result<RoaringBitmap32>(null_bitmap_);
}

Result<RoaringBitmap32> RangeBitmap::IsNotNull() const {
  return Result<RoaringBitmap32>(bsi_->IsNotNull());
}

// --- Appender ---

// Max distinct codes for BSI; extend if needed.
static constexpr int32_t kMaxDictionaryCodes = 65536;

RangeBitmap::Appender::Appender(FieldType field_type, int32_t chunk_size_bytes)
    : field_type_(field_type), chunk_size_bytes_(chunk_size_bytes) {
  bsi_appender_ = std::make_unique<BitSliceIndexBitmap::Appender>(0, kMaxDictionaryCodes - 1);
}

Status RangeBitmap::Appender::Append(const Literal& key) {
  if (key.IsNull()) {
    null_bitmap_.Add(rid_);
    ++rid_;
    return Status::OK();
  }

  auto it = std::lower_bound(keys_.begin(), keys_.end(), key,
                             [](const Literal& a, const Literal& b) {
                               auto r = a.CompareTo(b);
                               return r.ok() && r.value() < 0;
                             });
  int32_t code;
  if (it == keys_.end() || it->CompareTo(key).value() != 0) {
    if (keys_.size() >= static_cast<size_t>(kMaxDictionaryCodes)) {
      return Status::Invalid("RangeBitmap dictionary exceeds max codes");
    }
    code = static_cast<int32_t>(keys_.size());
    keys_.insert(it, key);
  } else {
    code = static_cast<int32_t>(std::distance(keys_.begin(), it));
  }

  if (!min_ || key.CompareTo(*min_).value() < 0) min_ = key;
  if (!max_ || key.CompareTo(*max_).value() > 0) max_ = key;

  codes_.push_back(code);
  PAIMON_RETURN_NOT_OK(bsi_appender_->Append(rid_, code));
  ++rid_;
  return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> RangeBitmap::Appender::Serialize(MemoryPool* pool) const {
  const int32_t segment_size = 64 * 1024;
  auto mem_pool = std::shared_ptr<MemoryPool>(pool, [](MemoryPool*) {});
  auto out = std::make_shared<MemorySegmentOutputStream>(segment_size, mem_pool);

  out->WriteValue<int8_t>(VERSION);
  out->WriteValue<int32_t>(static_cast<int32_t>(keys_.size()));

  PAIMON_ASSIGN_OR_RAISE(auto value_writer, GetValueWriter(field_type_, out));
  for (const auto& k : keys_) {
    PAIMON_RETURN_NOT_OK(value_writer(k));
  }

  auto null_bytes = null_bitmap_.Serialize(pool);
  out->WriteValue<int32_t>(static_cast<int32_t>(null_bytes->size()));
  out->Write(null_bytes->data(), null_bytes->size());

  out->WriteValue<int8_t>(min_.has_value() ? 1 : 0);
  if (min_) PAIMON_RETURN_NOT_OK(value_writer(*min_));
  out->WriteValue<int8_t>(max_.has_value() ? 1 : 0);
  if (max_) PAIMON_RETURN_NOT_OK(value_writer(*max_));

  auto bsi_bytes = bsi_appender_->Serialize(pool);
  if (!bsi_bytes.ok()) return bsi_bytes.status();
  out->WriteValue<int32_t>(static_cast<int32_t>(bsi_bytes.value()->size()));
  out->Write(bsi_bytes.value()->data(), bsi_bytes.value()->size());

  int64_t total = out->CurrentSize();
  auto result = Bytes::AllocateBytes(static_cast<int32_t>(total), pool);
  const auto& segs = out->Segments();
  int64_t pos = 0;
  for (size_t i = 0; i < segs.size(); ++i) {
    int32_t seg_len = (i + 1 == segs.size())
                          ? out->CurrentPositionInSegment()
                          : segs[i].Size();
    memcpy(reinterpret_cast<char*>(result->data()) + pos,
           segs[i].GetArray()->data(), seg_len);
    pos += seg_len;
  }
  return result;
}

}  // namespace paimon
