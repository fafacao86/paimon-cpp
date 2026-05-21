/*
 * Copyright 2026-present Alibaba Inc.
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

#include <string_view>

#include "paimon/common/lookup/vortex/vortex_sst_ffi.h"

namespace paimon::vortex_sst {

class OwnedBytes {
 public:
    OwnedBytes() = default;

    explicit OwnedBytes(VortexOwnedBytes bytes) : bytes_(bytes) {}

    OwnedBytes(const OwnedBytes&) = delete;
    OwnedBytes& operator=(const OwnedBytes&) = delete;

    OwnedBytes(OwnedBytes&& other) noexcept : bytes_(other.Release()) {}

    OwnedBytes& operator=(OwnedBytes&& other) noexcept {
        if (this != &other) {
            Reset();
            bytes_ = other.Release();
        }
        return *this;
    }

    ~OwnedBytes() {
        Reset();
    }

    std::string_view View() const {
        return std::string_view(reinterpret_cast<const char*>(bytes_.data), bytes_.len);
    }

    const VortexOwnedBytes& Raw() const {
        return bytes_;
    }

    VortexOwnedBytes Release() {
        VortexOwnedBytes bytes = bytes_;
        bytes_ = VortexOwnedBytes{nullptr, 0};
        return bytes;
    }

    void Reset() {
        vortex_sst_free_owned_bytes(bytes_);
        bytes_ = VortexOwnedBytes{nullptr, 0};
    }

 private:
    VortexOwnedBytes bytes_{nullptr, 0};
};

class LeafBlockCodec {
 public:
    static VortexSstStatusCode BuildFromArrow(struct ArrowSchema* schema, struct ArrowArray* array,
                                              const VortexSstLeafBuildOptions& options,
                                              OwnedBytes* out) {
        VortexOwnedBytes bytes{nullptr, 0};
        VortexSstStatusCode status =
            vortex_sst_build_leaf_block_from_ffi(schema, array, &options, &bytes);
        if (status == VORTEX_SST_OK) {
            *out = OwnedBytes(bytes);
        }
        return status;
    }

    static VortexSstStatusCode Lookup(std::string_view block, struct ArrowSchema* key_schema,
                                      struct ArrowArray* key_array, bool* found,
                                      OwnedBytes* value) {
        VortexSstLookupResult result{0, VortexOwnedBytes{nullptr, 0}};
        VortexSstStatusCode status =
            vortex_sst_lookup_leaf_block(reinterpret_cast<const uint8_t*>(block.data()),
                                         block.size(), key_schema, key_array, &result);
        if (status == VORTEX_SST_OK) {
            *found = result.found != 0;
            *value = OwnedBytes(result.value);
        }
        return status;
    }
};

}  // namespace paimon::vortex_sst
