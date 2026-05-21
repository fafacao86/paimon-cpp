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

#include <stddef.h>
#include <stdint.h>

struct ArrowArray;
struct ArrowSchema;

#ifdef __cplusplus
extern "C" {
#endif

typedef struct VortexSstLeafBuildOptions {
    uint32_t key_column_count;
    uint32_t string_prefix_len;
} VortexSstLeafBuildOptions;

typedef struct VortexOwnedBytes {
    const uint8_t* data;
    size_t len;
} VortexOwnedBytes;

typedef struct VortexSstLookupResult {
    uint8_t found;
    VortexOwnedBytes value;
} VortexSstLookupResult;

typedef enum VortexSstStatusCode {
    VORTEX_SST_OK = 0,
    VORTEX_SST_NULL_POINTER = 1,
    VORTEX_SST_INVALID_INPUT = 2,
    VORTEX_SST_INTERNAL_ERROR = 3,
} VortexSstStatusCode;

/// Build one encoded Vortex-backed SST leaf block from one Arrow C StructArray batch.
///
/// Ownership contract: this call consumes `schema` and `array`. The caller must not release or
/// reuse those Arrow C wrapper structs after passing them to Rust.
VortexSstStatusCode vortex_sst_build_leaf_block_from_ffi(const struct ArrowSchema* schema,
                                                         const struct ArrowArray* array,
                                                         const VortexSstLeafBuildOptions* options,
                                                         VortexOwnedBytes* out_bytes);

/// Lookup one key in one encoded Vortex-backed SST leaf block.
///
/// `key_schema` and `key_array` must describe a one-row Arrow C StructArray containing only key
/// columns in primary-key order. This call consumes those Arrow C wrapper structs.
VortexSstStatusCode vortex_sst_lookup_leaf_block(const uint8_t* block_data, size_t block_len,
                                                 const struct ArrowSchema* key_schema,
                                                 const struct ArrowArray* key_array,
                                                 VortexSstLookupResult* out);

/// Release bytes returned by Rust.
void vortex_sst_free_owned_bytes(VortexOwnedBytes bytes);

#ifdef __cplusplus
}
#endif
