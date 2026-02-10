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

#include "gtest/gtest.h"

#include "paimon/common/file_index/rangebitmap/range_bitmap.h"
#include "paimon/predicate/literal.h"

namespace paimon {

TEST(RangeBitmapTest, RangeBitmapAppender) {
  RangeBitmap::Appender appender(FieldType::INT, 16 * 1024);

  // Add some test values
  Literal val1(10);
  Literal val2(20);
  Literal val3(30);

  EXPECT_TRUE(appender.Append(val1).ok());
  EXPECT_TRUE(appender.Append(val2).ok());
  EXPECT_TRUE(appender.Append(val3).ok());

  // Serialize
  auto serialized = appender.Serialize(nullptr);  // TODO: Need proper MemoryPool
  EXPECT_TRUE(serialized.ok());
  EXPECT_TRUE(serialized.value()->size() > 0);
}

}  // namespace paimon