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

#include <functional>
#include <memory>
#include <string>
#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"

namespace paimon {

class KeyFactory {
 public:
  virtual ~KeyFactory() = default;

  /// Create a comparator for values of this type
  virtual Result<std::function<int(const Literal&, const Literal&)>> CreateComparator() const = 0;

  /// Create a serializer for values
  virtual Result<std::shared_ptr<KeySerializer>> CreateSerializer() const = 0;

  /// Create a deserializer for values
  virtual Result<std::shared_ptr<KeyDeserializer>> CreateDeserializer() const = 0;

  /// Create a converter from Literal to internal representation (optional)
  virtual Result<std::function<Result<Literal>(const Literal&)>> CreateConverter() const;

  /// Get default chunk size for dictionary
  virtual std::string DefaultChunkSize() const;

  /// Key serializer interface
  class KeySerializer {
   public:
    virtual ~KeySerializer() = default;
    virtual Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const = 0;
    virtual int32_t SerializedSizeInBytes(const Literal& literal) const = 0;
  };

  /// Key deserializer interface
  class KeyDeserializer {
   public:
    virtual ~KeyDeserializer() = default;
    virtual Result<Literal> Deserialize(::arrow::io::InputStream* in) const = 0;
  };
};

/// Factory method to create KeyFactory for a given FieldType
Result<std::shared_ptr<KeyFactory>> CreateKeyFactory(FieldType field_type);

/// Abstract base class for fixed-length key factories
class FixedLengthKeyFactory : public KeyFactory {
 public:
  Result<std::shared_ptr<KeySerializer>> CreateSerializer() const;
  Result<std::shared_ptr<KeyDeserializer>> CreateDeserializer() const;
};

/// Abstract base class for variable-length key factories
class VariableLengthKeyFactory : public KeyFactory {
 public:
  Result<std::shared_ptr<KeySerializer>> CreateSerializer() const;
  Result<std::shared_ptr<KeyDeserializer>> CreateDeserializer() const;
};

}  // namespace paimon