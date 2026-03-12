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

#include "rosbag2_cpp/throughput_predictor.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>
#include <utility>
#include <vector>

namespace rosbag2_cpp
{

namespace
{
constexpr int64_t NSEC_PER_SEC = 1000LL * 1000 * 1000;
// Run full bucket rebuild and period estimation every N feeds to amortize cost.
constexpr size_t kFeedThrottleInterval = 50;
}

ThroughputPredictor::ThroughputPredictor(const ThroughputPredictorConfig & config)
: config_(config)
{}

void ThroughputPredictor::feed(int64_t timestamp_ns, size_t bytes)
{
  if (samples_.empty() && first_sample_time_ns_ < 0) {
    first_sample_time_ns_ = timestamp_ns;
  }
  if (config_.max_samples > 0 && samples_.size() >= config_.max_samples) {
    samples_.pop_front();
  }
  samples_.emplace_back(timestamp_ns, bytes);
  prune_old_samples(timestamp_ns);

  ++feed_count_;
  // Throttle heavy work: run every N feeds, plus once at start to bootstrap
  if (feed_count_ == 1 || feed_count_ % kFeedThrottleInterval == 0) {
    update_buckets();
    estimate_period_and_phase();
  }
}

void ThroughputPredictor::prune_old_samples(int64_t now_ns)
{
  if (config_.max_history_ns <= 0) {
    return;
  }
  const int64_t cutoff = now_ns - config_.max_history_ns;
  while (!samples_.empty() && samples_.front().first < cutoff) {
    samples_.pop_front();
  }
}

void ThroughputPredictor::update_buckets()
{
  buckets_.clear();
  if (samples_.empty()) {
    return;
  }
  const int64_t bucket_ns = static_cast<int64_t>(
    config_.bucket_duration_sec * NSEC_PER_SEC);
  if (bucket_ns <= 0) {
    return;
  }
  int64_t bucket_start = (samples_.front().first / bucket_ns) * bucket_ns;
  uint64_t sum_bytes = 0;
  for (const auto & s : samples_) {
    int64_t b = (s.first / bucket_ns) * bucket_ns;
    if (b != bucket_start) {
      if (sum_bytes > 0) {
        buckets_.emplace_back(bucket_start, sum_bytes);
      }
      bucket_start = b;
      sum_bytes = 0;
    }
    sum_bytes += s.second;
  }
  if (sum_bytes > 0) {
    buckets_.emplace_back(bucket_start, sum_bytes);
  }
}

void ThroughputPredictor::estimate_period_and_phase()
{
  if (buckets_.size() < 3) {
    ready_ = false;
    return;
  }
  // Throughput per bucket (bytes/sec)
  const double bucket_sec = config_.bucket_duration_sec;
  std::vector<std::pair<int64_t, double>> tp;
  tp.reserve(buckets_.size());
  for (const auto & b : buckets_) {
    double rate = (bucket_sec > 0 && b.second > 0) ?
      (static_cast<double>(b.second) / bucket_sec) : 0.0;
    tp.emplace_back(b.first, rate);
  }
  // Find local minima and their dip depth (peak - trough); peak = max(neighbors)
  std::vector<std::pair<int64_t, double>> candidates;
  candidates.reserve(tp.size() / 2u);
  for (size_t i = 1; i + 1 < tp.size(); ++i) {
    if (tp[i].second <= tp[i - 1].second && tp[i].second <= tp[i + 1].second) {
      const double peak = std::max(tp[i - 1].second, tp[i + 1].second);
      const double dip = peak - tp[i].second;
      candidates.emplace_back(tp[i].first, dip);
    }
  }
  double max_dip = 0.0;
  for (const auto & c : candidates) {
    if (c.second > max_dip) {
      max_dip = c.second;
    }
  }
  std::vector<int64_t> trough_times_ns;
  trough_times_ns.reserve(candidates.size());
  const double min_dip = config_.min_trough_dip_ratio * max_dip;
  for (const auto & c : candidates) {
    if (max_dip <= 0.0 || config_.min_trough_dip_ratio <= 0.0 || c.second >= min_dip) {
      trough_times_ns.push_back(c.first);
    }
  }
  if (trough_times_ns.size() < 2) {
    ready_ = false;
    return;
  }
  // Median interval between consecutive troughs = period (nth_element is O(n) vs sort O(n log n))
  std::vector<int64_t> intervals;
  intervals.reserve(trough_times_ns.size() - 1);
  for (size_t i = 1; i < trough_times_ns.size(); ++i) {
    intervals.push_back(trough_times_ns[i] - trough_times_ns[i - 1]);
  }
  const size_t mid = intervals.size() / 2;
  std::nth_element(intervals.begin(), intervals.begin() + mid, intervals.end());
  int64_t period_ns_candidate = intervals[mid];
  const int64_t min_period_ns = static_cast<int64_t>(config_.min_period_sec * NSEC_PER_SEC);
  const int64_t max_period_ns = static_cast<int64_t>(config_.max_period_sec * NSEC_PER_SEC);
  if (period_ns_candidate < min_period_ns || period_ns_candidate > max_period_ns) {
    ready_ = false;
    return;
  }
  period_ns_ = period_ns_candidate;
  last_trough_ns_ = trough_times_ns.back();
  next_trough_ns_ = last_trough_ns_ + period_ns_;
  // Need at least min_cycles_before_predict full cycles (troughs)
  if (trough_times_ns.size() >= config_.min_cycles_before_predict) {
    ready_ = true;
  }
}

bool ThroughputPredictor::in_trough_window(int64_t t_ns, int64_t trough_ns) const
{
  const int64_t half_ns = static_cast<int64_t>(
    config_.trough_window_half_width_sec * NSEC_PER_SEC);
  return std::abs(t_ns - trough_ns) <= half_ns;
}

bool ThroughputPredictor::is_in_predicted_trough_now(int64_t current_time_ns) const
{
  if (!ready_ || period_ns_ <= 0) {
    return false;
  }
  const int64_t min_learning_ns = static_cast<int64_t>(config_.min_learning_time_sec * NSEC_PER_SEC);
  if (min_learning_ns > 0 && first_sample_time_ns_ >= 0 &&
      (current_time_ns - first_sample_time_ns_) < min_learning_ns) {
    return false;
  }
  // Find the predicted trough time T nearest to current_time_ns (T = last_trough + k*period).
  const int64_t k = (current_time_ns - last_trough_ns_) / period_ns_;
  const int64_t trough = last_trough_ns_ + k * period_ns_;
  ROSBAG2_CPP_LOG_DEBUG_STREAM(
    "Throughput predictor: current_time_ns=" << current_time_ns
    << " last_trough_ns_=" << last_trough_ns_
    << " period_ns_=" << period_ns_
    << " k=" << k
    << " predicted_trough_ns=" << trough);
  const bool in_window = in_trough_window(current_time_ns, trough);
  ROSBAG2_CPP_LOG_DEBUG_STREAM(
    "Throughput predictor: in_trough_window=" << in_window);
  return in_window;
}

int64_t ThroughputPredictor::get_next_trough_time_ns() const
{
  return ready_ ? next_trough_ns_ : 0;
}

bool ThroughputPredictor::is_ready() const
{
  return ready_;
}

}  // namespace rosbag2_cpp
