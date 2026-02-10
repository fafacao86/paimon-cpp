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

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "dictionary/dictionary.h"
#include "paimon/common/file_index/rangebitmap/bit_slice_index_bitmap.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {

class InputStream;
class MemoryPool;

/// RangeBitmap: dictionary (value -> code) + BitSliceIndex (code -> row bitmap).
/// Supports equality and range predicates. No IO in constructor; use static Deserialize().
class RangeBitmap {
 public:
  static constexpr int8_t VERSION = 1;

  /// Deserialize from stream. Performs all IO here; no IO in constructor.
  static Result<std::unique_ptr<RangeBitmap>> Create(
      const std::shared_ptr<InputStream>& input_stream, int64_t offset, int64_t length,
      FieldType field_type, MemoryPool* pool);

  /// Query: rows where column equals key
  Result<RoaringBitmap32> Eq(const Literal& key) const;
  /// Query: rows where column not equal to key
  Result<RoaringBitmap32> Neq(const Literal& key) const;
  Result<RoaringBitmap32> Lt(const Literal& key) const;
  Result<RoaringBitmap32> Lte(const Literal& key) const;
  Result<RoaringBitmap32> Gt(const Literal& key) const;
  Result<RoaringBitmap32> Gte(const Literal& key) const;
  Result<RoaringBitmap32> In(const std::vector<Literal>& keys) const;
  Result<RoaringBitmap32> NotIn(const std::vector<Literal>& keys) const;
  Result<RoaringBitmap32> IsNull() const;
  Result<RoaringBitmap32> IsNotNull() const;

  /// Builder for creating a RangeBitmap (write path).
  class Appender {
   public:
    Appender(FieldType field_type, int32_t chunk_size_bytes);

    Status Append(const Literal& key);
    Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize(MemoryPool* pool) const;

   private:
    [[maybe_unused]] FieldType field_type_;
    [[maybe_unused]] int32_t chunk_size_bytes_;
    int32_t rid_ = 0;
    std::vector<Literal> keys_;  // sorted unique keys (dictionary)
    std::vector<int32_t> codes_; // rid -> code for each appended row
    RoaringBitmap32 null_bitmap_;
    std::optional<Literal> min_;
    std::optional<Literal> max_;
    std::unique_ptr<BitSliceIndexBitmap::Appender> bsi_appender_;
    std::unique_ptr<KeyFactory> key_factory_;
  };

 private:
    RangeBitmap(MemoryPool* pool, int32_t rid, int32_t cardinality, int32_t dictionary_offset, int32_t dictionary_length,
                          int32_t bsi_offset, Literal&& min, Literal&& max,
                           std::unique_ptr<KeyFactory> key_factory, const std::shared_ptr<InputStream>& input_stream);
    Result<const BitSliceIndexBitmap*> GetBitSliceIndex();
    Result<const Dictionary*> GetDictionary();
    MemoryPool* pool_;
  [[maybe_unused]] int32_t rid_;
  [[maybe_unused]] int32_t cardinality_;
  [[maybe_unused]] int32_t bsi_offset_;
  [[maybe_unused]] int32_t dictionary_offset_;
  [[maybe_unused]] int32_t dictionary_length_;
  Literal min_;
  Literal max_;
  std::unique_ptr<KeyFactory> key_factory_;
  std::shared_ptr<InputStream> input_stream_;

  // For lazy loading
  std::optional<std::unique_ptr<BitSliceIndexBitmap>> bsi_;
  std::optional<std::unique_ptr<Dictionary>> dictionary_;

  Result<int32_t> FindCode(const Literal& key) const;
};

}  // namespace paimon
