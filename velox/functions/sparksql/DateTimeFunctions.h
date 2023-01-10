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
#include <velox/type/Timestamp.h>
#include "velox/core/QueryConfig.h"
#include "velox/external/date/tz.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Date.h"

using facebook::velox::Date;
using facebook::velox::Timestamp;
using facebook::velox::TimestampWithTimezone;
using facebook::velox::core::QueryConfig;
namespace {
inline constexpr int64_t kSecondsInDay = 86'400;

FOLLY_ALWAYS_INLINE const date::time_zone* getTimeZoneFromConfig(
    const QueryConfig& config) {
  if (config.adjustTimestampToTimezone()) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      return date::locate_zone(sessionTzName);
    }
  }
  return nullptr;
}

FOLLY_ALWAYS_INLINE int64_t
getSeconds(Timestamp timestamp, const date::time_zone* timeZone) {
  if (timeZone != nullptr) {
    timestamp.toTimezone(*timeZone);
    return timestamp.getSeconds();
  } else {
    return timestamp.getSeconds();
  }
}

FOLLY_ALWAYS_INLINE
std::tm getDateTime(Timestamp timestamp, const date::time_zone* timeZone) {
  int64_t seconds = getSeconds(timestamp, timeZone);
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  return dateTime;
}

FOLLY_ALWAYS_INLINE
std::tm getDateTime(Date date) {
  int64_t seconds = date.days() * kSecondsInDay;
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  return dateTime;
}

template <typename T>
struct InitSessionTimezone {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  const date::time_zone* timeZone_{nullptr};

  FOLLY_ALWAYS_INLINE void initialize(
      const QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);
  }
};

template <typename T>
struct TimestampWithTimezoneSupport {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Convert timestampWithTimezone to a timestamp representing the moment at the
  // zone in timestampWithTimezone.
  FOLLY_ALWAYS_INLINE
  Timestamp toTimestamp(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto milliseconds = *timestampWithTimezone.template at<0>();
    Timestamp timestamp = Timestamp::fromMillis(milliseconds);
    timestamp.toTimezone(*timestampWithTimezone.template at<1>());

    return timestamp;
  }
};

} // namespace
namespace facebook::velox::functions::sparksql {
template <typename T>
struct YearFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int32_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      TInput& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      TInput& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getYear(getDateTime(timestamp, nullptr));
  }
};
} // namespace facebook::velox::functions::sparksql