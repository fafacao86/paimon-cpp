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

#include "paimon/common/file_index/rangebitmap/dictionary/chunked_dictionary.h"

#include <algorithm>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/fixed_length_chunk.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<int32_t> ChunkedDictionary::Find(const Literal& key) {
    int32_t low = 0;
    int32_t high = size_ - 1;
    while (low <= high) {
        const int32_t mid = low + (high-low)/2;
        PAIMON_ASSIGN_OR_RAISE(const auto chunk, GetChunk(mid));
        PAIMON_ASSIGN_OR_RAISE(const int32_t result, chunk->Key().CompareTo(key));
        if (result > 0) {
            high = mid - 1;
        } else if (result < 0) {
            low = mid + 1;
        } else {
            // Exact match found - return the chunk's code
            return chunk->Code();
        }
    }
    if (low == 0) {
        return -(low + 1);  // Follow Java's return when no chunk found: -(low + 1)
    }
    PAIMON_ASSIGN_OR_RAISE(const auto prev_chunk, GetChunk(low - 1));
    return prev_chunk->Find(key);
}

Result<std::optional<Literal>> ChunkedDictionary::Find(const int32_t code) {
    if (code < 0) {
        return Status::Invalid("Invalid code: " + std::to_string(code));
    }
    int32_t low = 0;
    int32_t high = size_ - 1;

    while (low <= high) {
        const int32_t mid = low + (high-low)/2;
        PAIMON_ASSIGN_OR_RAISE(const auto chunk, GetChunk(mid));

        const int32_t chunk_code = chunk->Code();
        if (chunk_code > code) {
            high = mid - 1;
        } else if (chunk_code < code) {
            low = mid + 1;
        } else {
            return {chunk->Key()};
        }
    }
    PAIMON_ASSIGN_OR_RAISE(const auto prev_chunk, GetChunk(low - 1));
    return prev_chunk->Find(code);
}

Result<std::shared_ptr<Chunk>> ChunkedDictionary::GetChunk(int32_t index) {
    if (index < 0 || index >= size_) {
        return Status::Invalid(fmt::format("Invalid chunk index: {}", index));
    }
    if (!offsets_bytes_.has_value() || !chunks_bytes_.has_value()) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_, FS_SEEK_SET));
        auto offsets = Bytes::AllocateBytes(offsets_length_, pool_);
        PAIMON_RETURN_NOT_OK(input_stream_->Read(offsets->data(), offsets_length_));
        offsets_bytes_ = std::move(offsets);
        auto chunks = Bytes::AllocateBytes(chunks_length_, pool_);
        PAIMON_RETURN_NOT_OK(input_stream_->Read(chunks->data(), chunks_length_));
        chunks_bytes_ = std::move(chunks);
    }
    if (chunks_cache_[index]) {
        return chunks_cache_[index];
    }
    const int32_t chunk_offset = *reinterpret_cast<int32_t*>(offsets_bytes_.value()->data() + index * sizeof(int32_t));
    PAIMON_ASSIGN_OR_RAISE(auto chunk, factory_->MmapChunk(input_stream_,
        body_offset_+offsets_length_+chunk_offset,
        body_offset_+chunks_length_+offsets_length_));
    chunks_cache_[index] = std::move(chunk);
    return chunks_cache_[index];
}

ChunkedDictionary::Appender::Appender(FieldType field_type, int32_t chunk_size_bytes)
     {}

Status ChunkedDictionary::Appender::AppendSorted(const Literal& key, int32_t code) {
    // keys_.push_back(key);
    // current_keys_length_++;
    //
    // if (chunks_.empty() || current_keys_length_ >= chunk_size_bytes_) {
    //     if (current_keys_length_ > 0 && factory_) {
    //         const Literal& representative_key = keys_[current_chunk_start_];
    //         int32_t chunk_code = current_chunk_start_;
    //         PAIMON_ASSIGN_OR_RAISE(
    //             auto chunk, factory_->CreateChunk(representative_key, chunk_code,
    //                                               current_chunk_start_, current_keys_length_));
    //         chunks_.push_back(std::move(chunk));
    //
    //         current_chunk_start_ += current_keys_length_;
    //         current_keys_length_ = 0;
    //     }
    // }

    return Status::OK();
}

Status ChunkedDictionary::Appender::Finalize() {
    // if (current_keys_length_ > 0 && factory_) {
    //     const Literal& representative_key = keys_[current_chunk_start_];
    //     int32_t chunk_code = current_chunk_start_;
    //     PAIMON_ASSIGN_OR_RAISE(
    //         auto chunk, factory_->CreateChunk(representative_key, chunk_code, current_chunk_start_,
    //                                           current_keys_length_));
    //     chunks_.push_back(std::move(chunk));
    // }
    // current_keys_length_ = 0;
    return Status::OK();
}
Result<std::unique_ptr<ChunkedDictionary>> ChunkedDictionary::Appender::Build() {
    return {nullptr};
}

Result<PAIMON_UNIQUE_PTR<Bytes>> ChunkedDictionary::Appender::Serialize(MemoryPool* pool) const {
    // int32_t header_length = sizeof(int32_t) + sizeof(int8_t) + 5 * sizeof(int32_t);
    // int32_t size = static_cast<int32_t>(chunks_.size());
    // int32_t offsets_length = size * sizeof(int32_t);
    //
    // std::vector<int32_t> offsets;
    // std::vector<PAIMON_UNIQUE_PTR<Bytes>> chunk_bytes;
    //
    // int32_t current_offset = header_length + offsets_length;
    // for (const auto& chunk : chunks_) {
    //     PAIMON_ASSIGN_OR_RAISE(auto chunk_data, chunk->SerializeChunk(pool));
    //     offsets.push_back(current_offset);
    //     current_offset += static_cast<int32_t>(chunk_data->size());
    //     chunk_bytes.push_back(std::move(chunk_data));
    // }
    // int32_t chunks_length = current_offset - (header_length + offsets_length);
    //
    // const int32_t segment_size = 4096;
    // auto mem_pool = std::shared_ptr<MemoryPool>(pool, [](MemoryPool*) {});
    // auto keys_out = std::make_shared<MemorySegmentOutputStream>(segment_size, mem_pool);
    //
    // if (factory_) {
    //     PAIMON_ASSIGN_OR_RAISE(auto serializer, factory_->CreateSerializer());
    //     for (const auto& key : keys_) {
    //         PAIMON_RETURN_NOT_OK(serializer(keys_out, key));
    //     }
    // }
    // int64_t keys_total = keys_out->CurrentSize();
    // auto keys_buffer = Bytes::AllocateBytes(static_cast<int32_t>(keys_total), pool);
    // const auto& keys_segs = keys_out->Segments();
    // int64_t keys_pos = 0;
    // for (size_t i = 0; i < keys_segs.size(); ++i) {
    //     int32_t seg_len = (i + 1 == keys_segs.size())
    //                           ? static_cast<int32_t>(keys_out->CurrentPositionInSegment())
    //                           : keys_segs[i].Size();
    //     memcpy(keys_buffer->data() + keys_pos, keys_segs[i].GetArray()->data(), seg_len);
    //     keys_pos += seg_len;
    // }
    // int32_t keys_length = static_cast<int32_t>(keys_total);
    //
    // int32_t total_size = header_length + offsets_length + chunks_length + keys_length;
    // auto buffer = Bytes::AllocateBytes(total_size, pool);
    // uint8_t* data = reinterpret_cast<uint8_t*>(buffer->data());
    //
    // *reinterpret_cast<int32_t*>(data) = header_length;
    // data += sizeof(int32_t);
    // *data = CURRENT_VERSION;
    // data += sizeof(int8_t);
    // *reinterpret_cast<int32_t*>(data) = size;
    // data += sizeof(int32_t);
    // *reinterpret_cast<int32_t*>(data) = offsets_length;
    // data += sizeof(int32_t);
    // *reinterpret_cast<int32_t*>(data) = chunks_length;
    // data += sizeof(int32_t);
    // for (int32_t off : offsets) {
    //     *reinterpret_cast<int32_t*>(data) = off;
    //     data += sizeof(int32_t);
    // }
    //
    // for (auto& cb : chunk_bytes) {
    //     memcpy(data, cb->data(), cb->size());
    //     data += cb->size();
    // }
    //
    // memcpy(data, keys_buffer->data(), keys_length);
    // return buffer;
    return Status::NotImplemented("Appender::Serialize not implemented");
}


Result<std::unique_ptr<ChunkedDictionary>> ChunkedDictionary::Create(
    MemoryPool* pool, const FieldType field_type, const std::shared_ptr<InputStream>& input_stream,
    const int64_t offset) {
    PAIMON_RETURN_NOT_OK(input_stream->Seek(offset, FS_SEEK_SET));
    const auto data_in = std::make_unique<DataInputStream>(input_stream);
    PAIMON_ASSIGN_OR_RAISE(const auto header_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto version, data_in->ReadValue<int8_t>());
    if (version != CURRENT_VERSION) {
        return Status::Invalid("Unknown version of ChunkedDictionary");
    }
    PAIMON_ASSIGN_OR_RAISE(const auto size, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto offsets_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto chunks_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(auto factory, KeyFactory::Create(pool, field_type));
    return std::make_unique<ChunkedDictionary>(pool, input_stream, offset, field_type,
        std::move(factory), size, offsets_length, chunks_length, offset + header_length);
}

ChunkedDictionary::ChunkedDictionary(MemoryPool* pool,
                                     const std::shared_ptr<InputStream>& input_stream,
                                     int64_t start_of_dictionary, FieldType field_type,
                                     std::unique_ptr<KeyFactory> factory, int32_t size,
                                     int32_t offsets_length, int32_t chunks_length,
                                     int64_t body_offset)
    : pool_(pool),
      field_type_(field_type),
      factory_(std::move(factory)),
      input_stream_(input_stream),
      start_of_dictionary_(start_of_dictionary),
      size_(size),
      offsets_length_(offsets_length),
      chunks_length_(chunks_length),
      body_offset_(body_offset),
      offsets_bytes_({std::nullopt}),
      chunks_bytes_({std::nullopt}),
      chunks_cache_(std::vector<std::shared_ptr<Chunk>>(size)) {}
}  // namespace paimon
