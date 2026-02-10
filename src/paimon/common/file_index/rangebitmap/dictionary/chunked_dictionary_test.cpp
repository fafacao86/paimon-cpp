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

#include <memory>

#include "gtest/gtest.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ChunkedDictionaryTest, WriteAndReadViaStream) {
  auto pool = GetDefaultPool();
  const FieldType field_type = FieldType::INT;
  const int32_t chunk_size_bytes = 256;

  ChunkedDictionary::Appender appender(field_type, chunk_size_bytes);

  // Append sorted (key, code) pairs
  ASSERT_OK(appender.AppendSorted(Literal(10), 0));
  ASSERT_OK(appender.AppendSorted(Literal(20), 1));
  ASSERT_OK(appender.AppendSorted(Literal(30), 2));
  ASSERT_OK(appender.AppendSorted(Literal(40), 3));
  ASSERT_OK(appender.AppendSorted(Literal(50), 4));
  ASSERT_OK(appender.Finalize());

  ASSERT_OK_AND_ASSIGN(auto dict, appender.Build());

  // Find by key
  ASSERT_OK_AND_ASSIGN(int32_t code_10, dict->Find(Literal(10)));
  ASSERT_EQ(code_10, 0);
  ASSERT_OK_AND_ASSIGN(int32_t code_30, dict->Find(Literal(30)));
  ASSERT_EQ(code_30, 2);
  ASSERT_OK_AND_ASSIGN(int32_t code_50, dict->Find(Literal(50)));
  ASSERT_EQ(code_50, 4);

  // Not found
  ASSERT_OK_AND_ASSIGN(int32_t code_99, dict->Find(Literal(99)));
  ASSERT_EQ(code_99, -1);

  // Find by code
  ASSERT_OK_AND_ASSIGN(auto lit_0, dict->Find(0));
  ASSERT_TRUE(lit_0.has_value());
  ASSERT_OK_AND_ASSIGN(int cmp, lit_0->CompareTo(Literal(10)));
  ASSERT_EQ(cmp, 0);

  ASSERT_OK_AND_ASSIGN(auto lit_4, dict->Find(4));
  ASSERT_TRUE(lit_4.has_value());
  ASSERT_OK_AND_ASSIGN(cmp, lit_4->CompareTo(Literal(50)));
  ASSERT_EQ(cmp, 0);

  // Invalid code
  ASSERT_OK_AND_ASSIGN(auto lit_invalid, dict->Find(100));
  ASSERT_FALSE(lit_invalid.has_value());
}

TEST(ChunkedDictionaryTest, MultipleChunks) {
  auto pool = GetDefaultPool();
  const FieldType field_type = FieldType::INT;
  const int32_t chunk_size_bytes = 32;  // Small so we get multiple chunks

  ChunkedDictionary::Appender appender(field_type, chunk_size_bytes);

  for (int32_t i = 0; i < 20; ++i) {
    ASSERT_OK(appender.AppendSorted(Literal(100 + i), i));
  }
  ASSERT_OK(appender.Finalize());

  ASSERT_OK_AND_ASSIGN(auto dict, appender.Build());

  for (int32_t i = 0; i < 20; ++i) {
    ASSERT_OK_AND_ASSIGN(int32_t code, dict->Find(Literal(100 + i)));
    ASSERT_EQ(code, i);
    ASSERT_OK_AND_ASSIGN(auto lit, dict->Find(i));
    ASSERT_TRUE(lit.has_value());
    ASSERT_OK_AND_ASSIGN(int cmp, lit->CompareTo(Literal(100 + i)));
    ASSERT_EQ(cmp, 0);
  }
}

}  // namespace paimon::test
