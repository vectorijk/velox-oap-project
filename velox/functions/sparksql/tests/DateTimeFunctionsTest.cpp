/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdint.h>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class DateTimeFunctionsTest : public SparkFunctionBaseTest {
 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

 public:
  template <typename T>
  std::optional<T> evaluateWithTimestampWithTimezone(
      const std::string& expression,
      std::optional<int64_t> timestamp,
      const std::optional<std::string>& timeZoneName) {
    if (!timestamp.has_value() || !timeZoneName.has_value()) {
      return evaluateOnce<T>(
          expression,
          makeRowVector({makeRowVector(
              {
                  makeNullableFlatVector<int64_t>({std::nullopt}),
                  makeNullableFlatVector<int16_t>({std::nullopt}),
              },
              [](vector_size_t /*row*/) { return true; })}));
    }

    const std::optional<int64_t> tzid =
        util::getTimeZoneID(timeZoneName.value());
    return evaluateOnce<T>(
        expression,
        makeRowVector({makeRowVector({
            makeNullableFlatVector<int64_t>({timestamp}),
            makeNullableFlatVector<int16_t>({tzid}),
        })}));
  }
};

TEST_F(DateTimeFunctionsTest, year) {
  const auto year = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int32_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, 9000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1969, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, 12300000000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, yearDate) {
  const auto year = [&](std::optional<Date> date) {
    return evaluateOnce<int32_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Date(0)));
  EXPECT_EQ(1969, year(Date(-1)));
  EXPECT_EQ(2020, year(Date(18262)));
  EXPECT_EQ(1920, year(Date(-18262)));
}

TEST_F(DateTimeFunctionsTest, yearTimestampWithTimezone) {
  EXPECT_EQ(
      1969,
      evaluateWithTimestampWithTimezone<int32_t>("year(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1970,
      evaluateWithTimestampWithTimezone<int32_t>("year(c0)", 0, "+00:00"));
  EXPECT_EQ(
      1973,
      evaluateWithTimestampWithTimezone<int32_t>(
          "year(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      1966,
      evaluateWithTimestampWithTimezone<int32_t>(
          "year(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      2001,
      evaluateWithTimestampWithTimezone<int32_t>(
          "year(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      1938,
      evaluateWithTimestampWithTimezone<int32_t>(
          "year(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int32_t>(
          "year(c0)", std::nullopt, std::nullopt));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test