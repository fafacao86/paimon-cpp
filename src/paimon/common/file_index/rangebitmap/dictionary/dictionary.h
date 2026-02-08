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

#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {

class Dictionary {
 public:
  virtual ~Dictionary() = default;

  /// Find the code for a given key
  /// @return code if found, negative insertion point if not found
  virtual Result<int32_t> Find(const Literal& key) = 0;

  /// Find the key for a given code
  /// @return key if found, nullopt if code is invalid
  virtual Result<std::optional<Literal>> Find(int32_t code) = 0;

  /// Builder interface for creating dictionaries
  class Appender {
   public:
    virtual ~Appender() = default;

    /// Append a key-code pair (must be sorted by key)
    virtual Status AppendSorted(const Literal& key, int32_t code) = 0;

    /// Finalize the dictionary (move any pending chunks to the final list)
    virtual Status Finalize() = 0;

    /// Serialize the dictionary
    virtual Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize(MemoryPool* pool) const = 0;
  };
};

}  // namespace paimon