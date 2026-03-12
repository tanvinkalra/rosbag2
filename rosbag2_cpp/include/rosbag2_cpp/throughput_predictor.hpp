// Copyright 2024 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ROSBAG2_CPP__THROUGHPUT_PREDICTOR_HPP_
#define ROSBAG2_CPP__THROUGHPUT_PREDICTOR_HPP_

#include <cstdint>
#include <cstddef>
#include <deque>
#include <mutex>
#include <vector>

#include "rosbag2_cpp/visibility_control.hpp"

namespace rosbag2_cpp
{

/**
 * Configuration for throughput-based flush prediction.
 */
struct ROSBAG2_CPP_PUBLIC ThroughputPredictorConfig
{
  // Duration of each bucket for throughput time series (seconds).
  double bucket_duration_sec = 0.5;
  // Minimum number of full cycles observed before enabling prediction.
  size_t min_cycles_before_predict = 3;
  // Minimum time (seconds) of data to observe before allowing predictions. 0 = no delay.
  double min_learning_time_sec = 20.0;
  // Minimum dip (as fraction of max dip) to count as trough; 0 = any local minimum.
  double min_trough_dip_ratio = 0.30;
  // Half-width of the predicted trough window (seconds). Flush when time is in [trough - w, trough + w].
  double trough_window_half_width_sec = 0.5;
  // Maximum number of (t_ns, bytes) samples to keep in the sliding window.
  size_t max_samples = 10000;
  // Maximum history time (nanoseconds) for samples. Older samples are dropped.
  int64_t max_history_ns = 20LL * 1000 * 1000 * 1000;  // 20 s
  // Plausible period range for autocorrelation (seconds): [min_period_sec, max_period_sec].
  double min_period_sec = 2.0;
  double max_period_sec = 15.0;
};

/**
 * Predicts low-throughput (trough) windows from observed message timestamps and sizes
 * using a simple periodic model, for scheduling flush during troughs.
 */
class ROSBAG2_CPP_PUBLIC ThroughputPredictor
{
public:
  explicit ThroughputPredictor(const ThroughputPredictorConfig & config = ThroughputPredictorConfig());

  /**
   * Record one message: timestamp (nanoseconds) and serialized size (bytes).
   */
  void feed(int64_t timestamp_ns, size_t bytes);

  /**
   * Return true if current time is inside a predicted low-throughput window.
   * Caller should trigger flush at most once per window (e.g. track last flush time).
   */
  bool is_in_predicted_trough_now(int64_t current_time_ns) const;

  /** Optional: get predicted next trough time (nanoseconds) for logging. Returns 0 if unknown. */
  int64_t get_next_trough_time_ns() const;

  /** Whether the predictor has enough data to produce predictions. */
  bool is_ready() const;

private:
  void prune_old_samples(int64_t now_ns);
  void update_buckets();
  void estimate_period_and_phase();
  bool in_trough_window(int64_t t_ns, int64_t trough_ns) const;

  ThroughputPredictorConfig config_;
  std::deque<std::pair<int64_t, size_t>> samples_;
  size_t feed_count_ = 0;  // throttle bucket/period updates
  int64_t first_sample_time_ns_ = -1;  // set on first feed(), used for min_learning_time
  mutable std::mutex mutex_;  // protects all members (feed() and is_in_predicted_trough_now() are called from different threads)

  // Bucketed throughput: bucket_start_ns -> sum_bytes.
  std::vector<std::pair<int64_t, uint64_t>> buckets_;
  int64_t period_ns_ = 0;
  int64_t last_trough_ns_ = 0;
  int64_t next_trough_ns_ = 0;
  bool ready_ = false;
};

}  // namespace rosbag2_cpp

#endif  // ROSBAG2_CPP__THROUGHPUT_PREDICTOR_HPP_
