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

#include <cstdint>
#include <memory>
#include <optional>

#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {

class BitSliceIndexBitmap {
 public:
  static Result<std::shared_ptr<BitSliceIndexBitmap>> Create(
      const std::shared_ptr<ByteArrayInputStream>& input_stream);

  /// Query for exact code match
  Result<RoaringBitmap32> Eq(int32_t code) const;

  /// Query for codes greater than the given code
  Result<RoaringBitmap32> Gt(int32_t code) const;

  /// Query for codes greater than or equal to the given code
  Result<RoaringBitmap32> Gte(int32_t code) const;

  /// Get bitmap for non-null values
  const RoaringBitmap32& IsNotNull() const;

  /// Get code at specific position
  Result<std::optional<int32_t>> Get(int32_t position) const;

  /// Builder for creating BitSliceIndexBitmap
  class Appender {
   public:
    Appender(int32_t min_code, int32_t max_code);

    /// Append a row ID and its corresponding code
    Status Append(int32_t rid, int32_t code);

    /// Serialize the bitmap
    Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize(MemoryPool* pool) const;

   private:
    int32_t min_code_;
    int32_t max_code_;
    int32_t bit_width_;
    std::vector<std::vector<uint8_t>> bit_slices_;
    RoaringBitmap32 not_null_bitmap_;
  };

 private:
  std::vector<RoaringBitmap32> bit_slices_;
  RoaringBitmap32 not_null_bitmap_;
  int32_t min_code_;
  int32_t max_code_;
};

}  // namespace paimon