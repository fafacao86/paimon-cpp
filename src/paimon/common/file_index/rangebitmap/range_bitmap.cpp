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

#include "dictionary/chunked_dictionary.h"
#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {

Result<std::unique_ptr<RangeBitmap>> RangeBitmap::Create(
    const std::shared_ptr<InputStream>& input_stream, const int64_t offset, const int64_t length,
    FieldType field_type, MemoryPool* pool) {
    PAIMON_RETURN_NOT_OK(input_stream->Seek(offset, SeekOrigin::FS_SEEK_SET));
    const auto data_in = std::make_shared<DataInputStream>(input_stream);
    PAIMON_ASSIGN_OR_RAISE(const auto header_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int8_t version, data_in->ReadValue<int8_t>());
    if (version != VERSION) {
        return Status::Invalid(
            fmt::format("RangeBitmap unsupported version {} (expected {})", version, VERSION));
    }
    PAIMON_ASSIGN_OR_RAISE(const auto rid, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto cardinality, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(auto key_factory, KeyFactory::Create(pool, field_type));
    PAIMON_ASSIGN_OR_RAISE(const auto key_deserializer, key_factory->CreateDeserializer());
    PAIMON_ASSIGN_OR_RAISE(auto min, key_deserializer(data_in, pool));
    PAIMON_ASSIGN_OR_RAISE(auto max, key_deserializer(data_in, pool));
    PAIMON_ASSIGN_OR_RAISE(const auto dictionary_length, data_in->ReadValue<int32_t>());
    const auto dictionary_offset = static_cast<int32_t>(dictionary_length + sizeof(int32_t) + header_length);
    const auto bsi_offset = dictionary_offset + dictionary_length;
    return std::unique_ptr<RangeBitmap>(new RangeBitmap(pool, rid, cardinality, dictionary_offset, dictionary_length,
                                                        bsi_offset, std::move(min),
                                                        std::move(max), std::move(key_factory), input_stream));
}

Result<RoaringBitmap32> RangeBitmap::Eq(const Literal& key) const {
    return Status::NotImplemented("Eq not implemented");
}

Result<RoaringBitmap32> RangeBitmap::Neq(const Literal& key) const {
    return Status::NotImplemented("Neq not implemented");
}

Result<RoaringBitmap32> RangeBitmap::Lt(const Literal& key) const {
    return Status::NotImplemented("Lt not implemented");
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
    return Status::NotImplemented("Gt not implemented");
}

Result<RoaringBitmap32> RangeBitmap::Gte(const Literal& key) const {
    return Status::NotImplemented("Gte not implemented");
}

Result<RoaringBitmap32> RangeBitmap::In(const std::vector<Literal>& keys) const {
    return Status::NotImplemented("In not implemented");
}

Result<RoaringBitmap32> RangeBitmap::NotIn(const std::vector<Literal>& keys) const {
    return Status::NotImplemented("NotIn not implemented");
}

Result<RoaringBitmap32> RangeBitmap::IsNull() const {
    return Status::NotImplemented("IsNull not implemented");
}

Result<RoaringBitmap32> RangeBitmap::IsNotNull() const {
    return Status::NotImplemented("IsNotNull not implemented");
}

RangeBitmap::RangeBitmap(MemoryPool* pool, const int32_t rid, const int32_t cardinality,
                         const int32_t dictionary_offset, const int32_t dictionary_length, const int32_t bsi_offset, Literal&& min, Literal&& max,
                         std::unique_ptr<KeyFactory> key_factory, const std::shared_ptr<InputStream>& input_stream)
    : pool_(pool),
      rid_(rid),
      cardinality_(cardinality),
      bsi_offset_(bsi_offset),
      dictionary_offset_(dictionary_offset),
      dictionary_length_(dictionary_length),
      min_(std::move(min)),
      max_(std::move(max)),
      key_factory_(std::move(key_factory)),
      input_stream_(input_stream),
      bsi_({std::nullopt}),
      dictionary_({std::nullopt})
      {}

Result<const BitSliceIndexBitmap*> RangeBitmap::GetBitSliceIndex() {
    if (!bsi_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(bsi_, BitSliceIndexBitmap::Create(pool_, input_stream_, bsi_offset_));
    }
    return bsi_.value().get();
}

Result<const Dictionary*> RangeBitmap::GetDictionary() {
    if (!dictionary_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(dictionary_, ChunkedDictionary::Create(
            pool_, key_factory_->GetFieldType(), input_stream_, dictionary_offset_));
    }
    return dictionary_.value().get();
}

Result<int32_t> RangeBitmap::FindCode(const Literal& key) const {
    return Status::NotImplemented("FindCode not implemented");
}

RangeBitmap::Appender::Appender(FieldType field_type, int32_t chunk_size_bytes)
    : field_type_(field_type), chunk_size_bytes_(chunk_size_bytes) {
}

Status RangeBitmap::Appender::Append(const Literal& key) {
}

Result<PAIMON_UNIQUE_PTR<Bytes>> RangeBitmap::Appender::Serialize(MemoryPool* pool) const {

}

}  // namespace paimon
