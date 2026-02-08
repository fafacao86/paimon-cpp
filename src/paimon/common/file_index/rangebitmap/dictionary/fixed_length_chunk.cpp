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

#include "paimon/common/file_index/rangebitmap/dictionary/fixed_length_chunk.h"

#include <algorithm>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<bool> FixedLengthChunk::TryAdd(const Literal& key) {
    return true;
}
Result<Literal> FixedLengthChunk::GetKey(const int32_t index) {
    if (index < 0 || index >= size_) {
        return Status::Invalid("Index out of bounds");
    }
    if (!keys_.has_value()) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(keys_base_offset_, FS_SEEK_SET));
        keys_ = Bytes::AllocateBytes(keys_length_, nullptr);
        input_stream_->Read(keys_->get()->data(), keys_length_);
        PAIMON_ASSIGN_OR_RAISE(deserializer_, factory_->CreateDeserializer());
        keys_stream_ = std::make_shared<DataInputStream>(
            std::make_shared<ByteArrayInputStream>(keys_->get()->data(), keys_length_));
    }
    PAIMON_RETURN_NOT_OK(keys_stream_.value()->Seek(index * fixed_length_));
    return deserializer_.value()(keys_stream_.value(), nullptr);
}

Result<PAIMON_UNIQUE_PTR<Bytes>> FixedLengthChunk::SerializeChunk(MemoryPool* pool) const {
    constexpr int32_t segment_size = 4096;
    auto mem_pool = std::shared_ptr<MemoryPool>(pool, [](MemoryPool*) {});
    const auto out = std::make_shared<MemorySegmentOutputStream>(segment_size, mem_pool);
    PAIMON_ASSIGN_OR_RAISE(auto serializer, factory_->CreateSerializer());
    PAIMON_RETURN_NOT_OK(serializer(out, key_));

    // Write metadata: version, key, code, offset, size, keysLength, fixedLength
    out->WriteValue<int8_t>(version_);
    out->WriteValue<int32_t>(code_);
    out->WriteValue<int32_t>(offset_);
    out->WriteValue<int32_t>(size_);
    out->WriteValue<int32_t>(0);  // keysLength - chunks don't serialize keys
    out->WriteValue<int32_t>(0);  // fixedLength - not used in this simplified version

    int64_t total = out->CurrentSize();
    auto buffer = Bytes::AllocateBytes(static_cast<int32_t>(total), pool);
    const auto& segs = out->Segments();
    int64_t pos = 0;
    for (size_t i = 0; i < segs.size(); ++i) {
        int32_t seg_len = (i + 1 == segs.size())
                              ? static_cast<int32_t>(out->CurrentPositionInSegment())
                              : segs[i].Size();
        memcpy(buffer->data() + pos, segs[i].GetArray()->data(), seg_len);
        pos += seg_len;
    }
    return buffer;
}

Result<PAIMON_UNIQUE_PTR<Bytes>> FixedLengthChunk::SerializeKeys(MemoryPool* pool) const {
    // Chunks don't serialize keys - keys are serialized at the dictionary level
    return Bytes::AllocateBytes(0, pool);
}

FixedLengthChunk::FixedLengthChunk(Literal key, const int32_t code, const int32_t offset,
                                   const int32_t size, const std::shared_ptr<KeyFactory>& factory,
                                   const std::shared_ptr<InputStream>& input_stream,
                                   const int32_t keys_base_offset, const int32_t keys_length,
                                   const int32_t fixed_length)
    : version_(CURRENT_VERSION),
      key_(std::move(key)),
      code_(code),
      offset_(offset),
      size_(size),
      factory_(factory),
      input_stream_(input_stream),
      keys_base_offset_(keys_base_offset),
      keys_length_(keys_length),
      fixed_length_(fixed_length),
      deserializer_({std::nullopt}),
      keys_stream_({std::nullopt}),
      keys_({std::nullopt}),
      keys_length_limit(0),
      keys_current_idx(0) {}

FixedLengthChunk::FixedLengthChunk(Literal key, int32_t code, int32_t keys_length_limit,
                                   const std::shared_ptr<KeyFactory>& factory, int32_t fixed_length)
    : version_(CURRENT_VERSION),
      key_(key),
      code_(code),
      offset_(0),
      size_(0),
      factory_(factory),
      input_stream_(nullptr),
      keys_base_offset_(0),
      keys_length_(0),
      fixed_length_(fixed_length),
      deserializer_({std::nullopt}),
      keys_stream_({std::nullopt}),
      keys_({}),
      keys_length_limit(keys_length_limit),
      keys_current_idx(0) {}

}  // namespace paimon
