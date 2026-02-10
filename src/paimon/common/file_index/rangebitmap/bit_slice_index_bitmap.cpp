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
#include <cmath>

#include "paimon/io/data_input_stream.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<std::unique_ptr<BitSliceIndexBitmap>> BitSliceIndexBitmap::Create(
    MemoryPool* pool,
    const std::shared_ptr<InputStream>& input_stream, const int32_t offset) {
    const auto data_in = std::make_unique<DataInputStream>(input_stream);
    PAIMON_ASSIGN_OR_RAISE(const auto header_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto version, data_in->ReadValue<int8_t>());
    if (version != CURRENT_VERSION) {
        return Status::Invalid("Unknown BitSliceBitmap Version");
    }
    PAIMON_ASSIGN_OR_RAISE(const auto slices_size, data_in->ReadValue<int32_t>());
    auto slices = std::vector<std::unique_ptr<RoaringBitmap32>>();
    slices.resize(slices_size);
    PAIMON_ASSIGN_OR_RAISE(const auto ebm_size, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto indexes_length, data_in->ReadValue<int32_t>());
    auto indexes = Bytes::AllocateBytes(indexes_length, pool);
    auto body_offset = offset + header_length;
    return std::make_unique<BitSliceIndexBitmap>(pool, indexes_length, std::move(indexes),
        ebm_size, slices_size, input_stream, body_offset);
}

BitSliceIndexBitmap::BitSliceIndexBitmap(MemoryPool* pool, const int32_t indexes_length,PAIMON_UNIQUE_PTR<Bytes> indexes,
                                         const int32_t ebm_length,
                                         const int32_t slices_size,
                                         const std::shared_ptr<InputStream>& input_stream,
                                         const int32_t body_offset)
    : pool_(pool),
      initialized_(false),
      bit_slices_(std::vector<std::optional<RoaringBitmap32>>(slices_size, {std::nullopt})),
      ebm({std::nullopt}),
      input_stream_(input_stream),
      body_offset_(body_offset),
      indexes_(std::move(indexes)),
      ebm_length_(ebm_length),
      indexes_length_(indexes_length) {
}

Result<const RoaringBitmap32*> BitSliceIndexBitmap::GetEmtpyBitmap() {
    if (!ebm.has_value()) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_, FS_SEEK_SET));
        const auto bytes = Bytes::AllocateBytes(ebm_length_, pool_);
        PAIMON_RETURN_NOT_OK(input_stream_->Read(bytes->data(), ebm_length_));
        RoaringBitmap32 bitmap;
        PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bytes->data(), ebm_length_));
        ebm.value() = bitmap;
    }
    return &ebm.value();
}

Result<const RoaringBitmap32*> BitSliceIndexBitmap::GetSliceBitmap(const int32_t idx) {
    if (!bit_slices_[idx].has_value()) {
        const auto data_in = std::make_unique<DataInputStream>(
            std::make_shared<ByteArrayInputStream>(indexes_->data(), indexes_length_));
        const int position = static_cast<int32_t>(2 * sizeof(int32_t) * idx);
        PAIMON_RETURN_NOT_OK(data_in->Seek(position));
        PAIMON_ASSIGN_OR_RAISE(const auto offset, data_in->ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(const auto length, data_in->ReadValue<int32_t>());
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_+ebm_length_+offset, FS_SEEK_SET));
        RoaringBitmap32 bitmap;
        const auto bytes = Bytes::AllocateBytes(length, pool_);
        PAIMON_RETURN_NOT_OK(input_stream_->Read(bytes->data(), length));
        PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bytes->data(), length));
        bit_slices_[idx].value() = bitmap;
    }
    return &bit_slices_[idx].value();
}

Status BitSliceIndexBitmap::LoadSlices(const int32_t start, const int32_t end) {
    if (initialized_) {
        return Status::OK();
    }
    auto indexes_stream = std::make_shared<ByteArrayInputStream>(indexes_->data(), indexes_length_);
    const auto data_in = std::make_unique<DataInputStream>(indexes_stream);
    const auto position = static_cast<int32_t>(2 * sizeof(int32_t) * start);
    PAIMON_RETURN_NOT_OK(data_in->Seek(position));
    PAIMON_ASSIGN_OR_RAISE(const auto offset, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(auto length, data_in->ReadValue<int32_t>());
    std::vector<int32_t> lengths(end);
    lengths[start] = length;

    for (int32_t i = start + 1; i < end; ++i) {
        PAIMON_RETURN_NOT_OK(data_in->ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(const auto slice_length, data_in->ReadValue<int32_t>());
        lengths[i] = slice_length;
        length += slice_length;
    }
    PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_ + ebm_length_ + offset, FS_SEEK_SET));
    const auto bytes = Bytes::AllocateBytes(length, pool_);
    PAIMON_RETURN_NOT_OK(input_stream_->Read(bytes->data(), length));
    int32_t byte_position = 0;
    for (int32_t i = start; i < end; ++i) {
        const int32_t slice_length = lengths[i];
        RoaringBitmap32 bitmap;
        PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bytes->data() + byte_position, slice_length));
        bit_slices_[i].value() = bitmap;
        byte_position += slice_length;
    }
    initialized_ = true;
    return Status::OK();
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Eq(int32_t code) const {

}

Result<RoaringBitmap32> BitSliceIndexBitmap::Gt(int32_t code) const {
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Gte(int32_t code) const {

}

Result<RoaringBitmap32> BitSliceIndexBitmap::IsNotNull(const RoaringBitmap32& found_set) {
    if (!ebm.has_value()) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_, FS_SEEK_SET));
        const auto bytes = Bytes::AllocateBytes(ebm_length_, pool_);
        PAIMON_RETURN_NOT_OK(input_stream_->Read(bytes->data(), ebm_length_));
        RoaringBitmap32 bitmap;
        PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bytes->data(), ebm_length_));
        ebm.value() = bitmap;
    }
    return found_set.IsEmpty() ? ebm.value() : RoaringBitmap32::And(ebm.value(), found_set);
}

Result<std::optional<int32_t>> BitSliceIndexBitmap::Get(int32_t position) const {

}

// Appender implementation
BitSliceIndexBitmap::Appender::Appender(int32_t min_code, int32_t max_code){
}

Status BitSliceIndexBitmap::Appender::Append(int32_t rid, int32_t code) {
  return Status::NotImplemented("Appender::Append");
}

Result<PAIMON_UNIQUE_PTR<Bytes>> BitSliceIndexBitmap::Appender::Serialize(MemoryPool* pool) const {
  return Status::NotImplemented("Serialize");
}

}  // namespace paimon