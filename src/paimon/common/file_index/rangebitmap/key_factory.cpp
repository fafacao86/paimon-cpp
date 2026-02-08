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

#include "dictionary/key_factory.h"

#include "paimon/common/file_index/rangebitmap/key_factory.h"

namespace paimon {

// Comparator implementations
Result<std::function<int(const Literal&, const Literal&)>> IntKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<int32_t>(a.value());
    auto b_val = std::get<int32_t>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

Result<std::function<int(const Literal&, const Literal&)>> BigIntKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<int64_t>(a.value());
    auto b_val = std::get<int64_t>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

Result<std::function<int(const Literal&, const Literal&)>> FloatKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<float>(a.value());
    auto b_val = std::get<float>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

Result<std::function<int(const Literal&, const Literal&)>> DoubleKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<double>(a.value());
    auto b_val = std::get<double>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

Result<std::function<int(const Literal&, const Literal&)>> BooleanKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<bool>(a.value());
    auto b_val = std::get<bool>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

Result<std::function<int(const Literal&, const Literal&)>> TinyIntKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<int8_t>(a.value());
    auto b_val = std::get<int8_t>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

Result<std::function<int(const Literal&, const Literal&)>> SmallIntKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<int16_t>(a.value());
    auto b_val = std::get<int16_t>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

Result<std::function<int(const Literal&, const Literal&)>> StringKeyFactory::CreateComparator() const {
  return [](const Literal& a, const Literal& b) -> int {
    auto a_val = std::get<std::string>(a.value());
    auto b_val = std::get<std::string>(b.value());
    return a_val < b_val ? -1 : (a_val > b_val ? 1 : 0);
  };
}

// Serializer implementations
class IntKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<int32_t>(literal.value());
    return out->Write(&value, sizeof(int32_t)).ok() ? Status::OK() : Status::IOError("Failed to write int32_t");
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    return sizeof(int32_t);
  }
};

class BigIntKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<int64_t>(literal.value());
    return out->Write(&value, sizeof(int64_t)).ok() ? Status::OK() : Status::IOError("Failed to write int64_t");
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    return sizeof(int64_t);
  }
};

class FloatKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<float>(literal.value());
    return out->Write(&value, sizeof(float)).ok() ? Status::OK() : Status::IOError("Failed to write float");
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    return sizeof(float);
  }
};

class DoubleKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<double>(literal.value());
    return out->Write(&value, sizeof(double)).ok() ? Status::OK() : Status::IOError("Failed to write double");
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    return sizeof(double);
  }
};

class BooleanKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<bool>(literal.value());
    uint8_t byte_value = value ? 1 : 0;
    return out->Write(&byte_value, sizeof(uint8_t)).ok() ? Status::OK() : Status::IOError("Failed to write bool");
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    return sizeof(uint8_t);
  }
};

class TinyIntKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<int8_t>(literal.value());
    return out->Write(&value, sizeof(int8_t)).ok() ? Status::OK() : Status::IOError("Failed to write int8_t");
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    return sizeof(int8_t);
  }
};

class SmallIntKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<int16_t>(literal.value());
    return out->Write(&value, sizeof(int16_t)).ok() ? Status::OK() : Status::IOError("Failed to write int16_t");
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    return sizeof(int16_t);
  }
};

class StringKeySerializer : public KeyFactory::KeySerializer {
 public:
  Status Serialize(::arrow::io::OutputStream* out, const Literal& literal) const override {
    auto value = std::get<std::string>(literal.value());
    int32_t length = static_cast<int32_t>(value.size());
    if (!out->Write(&length, sizeof(int32_t)).ok()) {
      return Status::IOError("Failed to write string length");
    }
    if (!out->Write(value.data(), length).ok()) {
      return Status::IOError("Failed to write string data");
    }
    return Status::OK();
  }

  int32_t SerializedSizeInBytes(const Literal& literal) const override {
    auto value = std::get<std::string>(literal.value());
    return sizeof(int32_t) + static_cast<int32_t>(value.size());
  }
};

// CreateSerializer implementations
Result<std::shared_ptr<KeyFactory::KeySerializer>> IntKeyFactory::CreateSerializer() const {
  return std::make_shared<IntKeySerializer>();
}

Result<std::shared_ptr<KeyFactory::KeySerializer>> BigIntKeyFactory::CreateSerializer() const {
  return std::make_shared<BigIntKeySerializer>();
}

Result<std::shared_ptr<KeyFactory::KeySerializer>> FloatKeyFactory::CreateSerializer() const {
  return std::make_shared<FloatKeySerializer>();
}

Result<std::shared_ptr<KeyFactory::KeySerializer>> DoubleKeyFactory::CreateSerializer() const {
  return std::make_shared<DoubleKeySerializer>();
}

Result<std::shared_ptr<KeyFactory::KeySerializer>> BooleanKeyFactory::CreateSerializer() const {
  return std::make_shared<BooleanKeySerializer>();
}

Result<std::shared_ptr<KeyFactory::KeySerializer>> TinyIntKeyFactory::CreateSerializer() const {
  return std::make_shared<TinyIntKeySerializer>();
}

Result<std::shared_ptr<KeyFactory::KeySerializer>> SmallIntKeyFactory::CreateSerializer() const {
  return std::make_shared<SmallIntKeySerializer>();
}

Result<std::shared_ptr<KeyFactory::KeySerializer>> StringKeyFactory::CreateSerializer() const {
  return std::make_shared<StringKeySerializer>();
}

// Deserializer implementations
class IntKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    int32_t value;
    auto result = in->Read(sizeof(int32_t));
    if (!result.ok()) {
      return Status::IOError("Failed to read int32_t");
    }
    memcpy(&value, result.ValueUnsafe()->data(), sizeof(int32_t));
    return Literal(value);
  }
};

class BigIntKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    int64_t value;
    auto result = in->Read(sizeof(int64_t));
    if (!result.ok()) {
      return Status::IOError("Failed to read int64_t");
    }
    memcpy(&value, result.ValueUnsafe()->data(), sizeof(int64_t));
    return Literal(value);
  }
};

class FloatKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    float value;
    auto result = in->Read(sizeof(float));
    if (!result.ok()) {
      return Status::IOError("Failed to read float");
    }
    memcpy(&value, result.ValueUnsafe()->data(), sizeof(float));
    return Literal(value);
  }
};

class DoubleKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    double value;
    auto result = in->Read(sizeof(double));
    if (!result.ok()) {
      return Status::IOError("Failed to read double");
    }
    memcpy(&value, result.ValueUnsafe()->data(), sizeof(double));
    return Literal(value);
  }
};

class BooleanKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    uint8_t byte_value;
    auto result = in->Read(sizeof(uint8_t));
    if (!result.ok()) {
      return Status::IOError("Failed to read bool");
    }
    memcpy(&byte_value, result.ValueUnsafe()->data(), sizeof(uint8_t));
    return Literal(byte_value == 1);
  }
};

class TinyIntKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    int8_t value;
    auto result = in->Read(sizeof(int8_t));
    if (!result.ok()) {
      return Status::IOError("Failed to read int8_t");
    }
    memcpy(&value, result.ValueUnsafe()->data(), sizeof(int8_t));
    return Literal(value);
  }
};

class SmallIntKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    int16_t value;
    auto result = in->Read(sizeof(int16_t));
    if (!result.ok()) {
      return Status::IOError("Failed to read int16_t");
    }
    memcpy(&value, result.ValueUnsafe()->data(), sizeof(int16_t));
    return Literal(value);
  }
};

class StringKeyDeserializer : public KeyFactory::KeyDeserializer {
 public:
  Result<Literal> Deserialize(::arrow::io::InputStream* in) const override {
    int32_t length;
    auto length_result = in->Read(sizeof(int32_t));
    if (!length_result.ok()) {
      return Status::IOError("Failed to read string length");
    }
    memcpy(&length, length_result.ValueUnsafe()->data(), sizeof(int32_t));

    std::string value(length, '\0');
    auto data_result = in->Read(length);
    if (!data_result.ok()) {
      return Status::IOError("Failed to read string data");
    }
    memcpy(value.data(), data_result.ValueUnsafe()->data(), length);
    return Literal(value);
  }
};

// CreateDeserializer implementations
Result<std::shared_ptr<KeyFactory::KeyDeserializer>> IntKeyFactory::CreateDeserializer() const {
  return std::make_shared<IntKeyDeserializer>();
}

Result<std::shared_ptr<KeyFactory::KeyDeserializer>> BigIntKeyFactory::CreateDeserializer() const {
  return std::make_shared<BigIntKeyDeserializer>();
}

Result<std::shared_ptr<KeyFactory::KeyDeserializer>> FloatKeyFactory::CreateDeserializer() const {
  return std::make_shared<FloatKeyDeserializer>();
}

Result<std::shared_ptr<KeyFactory::KeyDeserializer>> DoubleKeyFactory::CreateDeserializer() const {
  return std::make_shared<DoubleKeyDeserializer>();
}

Result<std::shared_ptr<KeyFactory::KeyDeserializer>> BooleanKeyFactory::CreateDeserializer() const {
  return std::make_shared<BooleanKeyDeserializer>();
}

Result<std::shared_ptr<KeyFactory::KeyDeserializer>> TinyIntKeyFactory::CreateDeserializer() const {
  return std::make_shared<TinyIntKeyDeserializer>();
}

Result<std::shared_ptr<KeyFactory::KeyDeserializer>> SmallIntKeyFactory::CreateDeserializer() const {
  return std::make_shared<SmallIntKeyDeserializer>();
}

Result<std::shared_ptr<KeyFactory::KeyDeserializer>> StringKeyFactory::CreateDeserializer() const {
  return std::make_shared<StringKeyDeserializer>();
}

// Factory creation function
Result<std::shared_ptr<KeyFactory>> CreateKeyFactory(FieldType field_type) {
  switch (field_type) {
    case FieldType::INT32:
      return std::make_shared<IntKeyFactory>();
    case FieldType::INT64:
      return std::make_shared<BigIntKeyFactory>();
    case FieldType::FLOAT:
      return std::make_shared<FloatKeyFactory>();
    case FieldType::DOUBLE:
      return std::make_shared<DoubleKeyFactory>();
    case FieldType::BOOL:
      return std::make_shared<BooleanKeyFactory>();
    case FieldType::INT8:
      return std::make_shared<TinyIntKeyFactory>();
    case FieldType::INT16:
      return std::make_shared<SmallIntKeyFactory>();
    case FieldType::STRING:
      return std::make_shared<StringKeyFactory>();
    default:
      return Status::NotImplemented("Unsupported field type for RangeBitmap index: " + std::to_string(static_cast<int>(field_type)));
  }
}

}  // namespace paimon