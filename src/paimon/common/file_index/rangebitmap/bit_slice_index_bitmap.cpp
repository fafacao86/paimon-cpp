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

#include "paimon/common/file_index/rangebitmap/bit_slice_index_bitmap.h"

#include <algorithm>
#include <bit>
#include <cmath>

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<BitSliceIndexBitmap>> BitSliceIndexBitmap::Create(
    const std::shared_ptr<ByteArrayInputStream>& input_stream) {
  // Simplified implementation - in practice this would deserialize from input_stream
  auto bitmap = std::make_shared<BitSliceIndexBitmap>();
  return bitmap;
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Eq(int32_t code) const {
  if (code < min_code_ || code > max_code_) {
    return RoaringBitmap32();  // Empty bitmap
  }

  int32_t relative_code = code - min_code_;
  RoaringBitmap32 result = not_null_bitmap_;

  // Apply bit-wise equality check
  for (size_t i = 0; i < bit_slices_.size(); ++i) {
    bool bit_set = (relative_code & (1 << i)) != 0;
    if (bit_set) {
      result &= bit_slices_[i];
    } else {
      result -= bit_slices_[i];
    }
  }

  return result;
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Gt(int32_t code) const {
  if (code >= max_code_) {
    return RoaringBitmap32();  // Empty bitmap
  }
  if (code < min_code_) {
    return not_null_bitmap_;  // All values
  }

  // For greater than, we need to find all codes > code
  RoaringBitmap32 result;

  // Start from the highest bit that differs
  int32_t relative_code = code - min_code_;
  for (int32_t current_code = relative_code + 1; current_code <= max_code_ - min_code_; ++current_code) {
    auto eq_result = Eq(current_code + min_code_);
    if (eq_result.ok()) {
      result |= eq_result.value();
    }
  }

  return result;
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Gte(int32_t code) const {
  auto gt_result = Gt(code - 1);
  if (!gt_result.ok()) {
    return gt_result.status();
  }
  auto eq_result = Eq(code);
  if (!eq_result.ok()) {
    return eq_result.status();
  }
  return gt_result.value() | eq_result.value();
}

const RoaringBitmap32& BitSliceIndexBitmap::IsNotNull() const {
  return not_null_bitmap_;
}

Result<std::optional<int32_t>> BitSliceIndexBitmap::Get(int32_t position) const {
  // Simplified - in practice this would reconstruct the code from bit slices
  // For now, return nullopt to indicate not implemented
  return std::nullopt;
}

// Appender implementation
BitSliceIndexBitmap::Appender::Appender(int32_t min_code, int32_t max_code)
    : min_code_(min_code), max_code_(max_code) {
  if (max_code < min_code) {
    // Handle error
    return;
  }

  // Calculate bit width needed
  int32_t range = max_code - min_code;
  bit_width_ = (range == 0) ? 1 : (32 - std::countl_zero(static_cast<uint32_t>(range)));

  // Initialize bit slices
  bit_slices_.resize(bit_width_);
  for (auto& slice : bit_slices_) {
    slice.resize((range + 1 + 7) / 8, 0);  // Round up to bytes
  }
}

Status BitSliceIndexBitmap::Appender::Append(int32_t rid, int32_t code) {
  if (code < min_code_ || code > max_code_) {
    return Status::Invalid("Code out of range");
  }

  not_null_bitmap_.Add(rid);

  int32_t relative_code = code - min_code_;

  // Set bits in each slice
  for (int32_t bit = 0; bit < bit_width_; ++bit) {
    bool bit_set = (relative_code & (1 << bit)) != 0;
    if (bit_set) {
      int32_t byte_index = rid / 8;
      int32_t bit_index = rid % 8;
      if (byte_index < static_cast<int32_t>(bit_slices_[bit].size())) {
        bit_slices_[bit][byte_index] |= (1 << bit_index);
      }
    }
  }

  return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> BitSliceIndexBitmap::Appender::Serialize(MemoryPool* pool) const {
  // Calculate total size
  int32_t header_size = 3 * sizeof(int32_t);  // min_code, max_code, bit_width
  int32_t not_null_size = not_null_bitmap_.GetSizeInBytes();
  int32_t bit_slices_size = 0;
  for (const auto& slice : bit_slices_) {
    bit_slices_size += slice.size();
  }

  int32_t total_size = header_size + sizeof(int32_t) + not_null_size + sizeof(int32_t) + bit_slices_size;
  auto buffer = Bytes::AllocateBytes(total_size, pool);
  uint8_t* data = reinterpret_cast<uint8_t*>(buffer->data());

  // Write header
  *reinterpret_cast<int32_t*>(data) = min_code_;
  data += sizeof(int32_t);
  *reinterpret_cast<int32_t*>(data) = max_code_;
  data += sizeof(int32_t);
  *reinterpret_cast<int32_t*>(data) = bit_width_;
  data += sizeof(int32_t);

  // Write not-null bitmap
  *reinterpret_cast<int32_t*>(data) = not_null_size;
  data += sizeof(int32_t);
  auto serialized_bitmap = not_null_bitmap_.Serialize(pool);
  if (!serialized_bitmap.ok()) {
    return serialized_bitmap.status();
  }
  memcpy(data, serialized_bitmap.value()->data(), serialized_bitmap.value()->size());
  data += not_null_size;

  // Write bit slices
  *reinterpret_cast<int32_t*>(data) = static_cast<int32_t>(bit_slices_.size());
  data += sizeof(int32_t);
  for (const auto& slice : bit_slices_) {
    memcpy(data, slice.data(), slice.size());
    data += slice.size();
  }

  return buffer;
}

}  // namespace paimon