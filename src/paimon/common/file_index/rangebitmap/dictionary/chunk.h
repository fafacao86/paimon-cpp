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
#include <vector>

#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {

class Chunk {
 public:
  virtual ~Chunk() = default;

  /// Try to add a key to this chunk
  /// @return true if key was added, false if chunk is full
  virtual Result<bool> TryAdd(const Literal& key) = 0;

  virtual Result<int32_t> Find(const Literal& key) {
    // Default implementation: binary search through all keys in this chunk
    int32_t low = 0;
    int32_t high = Size() - 1;

    while (low <= high) {
      const int32_t mid = low + (high - low) / 2;
      PAIMON_ASSIGN_OR_RAISE(auto key_at_mid, GetKey(mid));
      PAIMON_ASSIGN_OR_RAISE(int32_t cmp, key_at_mid.CompareTo(key));

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return Code() + mid;
      }
    }
    return -1;
  }

  virtual Result<std::optional<Literal>> Find(const int32_t code) {
      if (code < 0) {
          return Status::Invalid("Invalid Code");
      }
      const int32_t local_index = code - Code();
    if (local_index >= 0 && local_index < Size()) {
      PAIMON_ASSIGN_OR_RAISE(auto key, GetKey(local_index));
      return std::optional<Literal>(std::move(key));
    }
    return {std::nullopt};
  }

  /// Get the representative key for this chunk
  virtual const Literal& Key() const = 0;

  /// Get the representative code for this chunk
  virtual int32_t Code() const = 0;

  /// Get the base offset in the dictionary's keys array
  virtual int32_t Offset() const = 0;

  /// Get the number of keys in this chunk
  virtual int32_t Size() const = 0;

  /// Serialize the chunk metadata (not the keys)
  virtual Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeChunk(MemoryPool* pool) const = 0;

  /// Serialize just the keys
  virtual Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeKeys(MemoryPool* pool) const = 0;

 protected:
  /// Get the key at the given local index within this chunk (0-based)
  /// @param index local index within the chunk (0 to Size()-1)
  /// @return the key at that index
  virtual Result<Literal> GetKey(int32_t index) = 0;
};

}  // namespace paimon