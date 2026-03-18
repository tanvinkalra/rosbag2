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

#include "rosbag2_cpp/logging.hpp"

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
// Run full bucket rebuild and period estimation every N seconds.
constexpr int64_t kEstimationIntervalNs = 1LL * NSEC_PER_SEC;  // 1 s
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

  (void)feed_count_;  // kept for backward compatibility; throttling is time-based now.
  // Throttle heavy work using message timestamps (time-based rather than count-based).
  if (last_estimation_time_ns_ < 0 ||
    (timestamp_ns - last_estimation_time_ns_) >= kEstimationIntervalNs)
  {
    update_buckets();
    estimate_period_and_phase();
    last_estimation_time_ns_ = timestamp_ns;
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
  // Build throughput per bucket (bytes/sec) as a regularly sampled time series.
  const double bucket_sec = config_.bucket_duration_sec;
  if (bucket_sec <= 0.0) {
    ready_ = false;
    return;
  }

  // Use at most a reasonable number of recent buckets to bound computation.
  constexpr size_t kMaxBucketsForSpectrum = 512;
  const size_t total_buckets = buckets_.size();
  const size_t start_index =
    (total_buckets > kMaxBucketsForSpectrum) ? (total_buckets - kMaxBucketsForSpectrum) : 0u;
  const size_t series_length = total_buckets - start_index;

  if (series_length < 3) {
    ready_ = false;
    return;
  }

  std::vector<double> rates;
  rates.reserve(series_length);
  for (size_t i = start_index; i < total_buckets; ++i) {
    const auto & b = buckets_[i];
    const double rate = (bucket_sec > 0.0 && b.second > 0u) ?
      (static_cast<double>(b.second) / bucket_sec) : 0.0;
    rates.push_back(rate);
  }

  const int64_t first_bucket_ns = buckets_[start_index].first;
  const double window_sec = bucket_sec * static_cast<double>(rates.size());

  // Discrete Fourier transform (real-valued input) to obtain spectrum.
  // We only need bins 1..N/2; bin 0 (DC) is ignored.
  const size_t N = rates.size();
  if (N < 2) {
    ready_ = false;
    return;
  }

  const double two_pi_over_N = 2.0 * M_PI / static_cast<double>(N);
  const int64_t min_period_ns = static_cast<int64_t>(config_.min_period_sec * NSEC_PER_SEC);
  const int64_t max_period_ns = static_cast<int64_t>(config_.max_period_sec * NSEC_PER_SEC);

  double best_mag = 0.0;
  size_t best_k = 0;
  double best_re = 0.0;
  double best_im = 0.0;

  // DFT: O(N^2) but N is small (bounded by kMaxBucketsForSpectrum).
  for (size_t k = 1; k <= N / 2; ++k) {
    double re = 0.0;
    double im = 0.0;
    for (size_t n = 0; n < N; ++n) {
      const double angle = -two_pi_over_N * static_cast<double>(k * n);
      const double v = rates[n];
      re += v * std::cos(angle);
      im += v * std::sin(angle);
    }

    // Map frequency bin k to period in nanoseconds.
    const double freq_hz = static_cast<double>(k) / (static_cast<double>(N) * bucket_sec);
    if (freq_hz <= 0.0) {
      continue;
    }
    const double period_sec = 1.0 / freq_hz;
    const int64_t period_ns_candidate =
      static_cast<int64_t>(period_sec * static_cast<double>(NSEC_PER_SEC));
    if (period_ns_candidate < min_period_ns || period_ns_candidate > max_period_ns) {
      continue;
    }

    const double mag_sq = re * re + im * im;
    if (mag_sq > best_mag) {
      best_mag = mag_sq;
      best_k = k;
      best_re = re;
      best_im = im;
      period_ns_ = period_ns_candidate;
    }
  }

  if (best_k == 0 || period_ns_ <= 0) {
    ready_ = false;
    return;
  }

  // Derive phase for the dominant bin and compute the time of the last trough
  // (minimum) within the current window. Model as A * cos(2π t / T + phi).
  const double phi = std::atan2(best_im, best_re);
  const double period_sec = static_cast<double>(period_ns_) / static_cast<double>(NSEC_PER_SEC);

  // Time of first minimum relative to start of window in [0, T).
  const double t_min_base =
    std::fmod(((M_PI - phi) / (2.0 * M_PI)) * period_sec, period_sec);

  const double t_window_max = window_sec;
  double last_min_rel = t_min_base;
  if (t_window_max > t_min_base) {
    const double extra = t_window_max - t_min_base;
    const double cycles = std::floor(extra / period_sec);
    last_min_rel = t_min_base + cycles * period_sec;
  }

  const int64_t last_min_offset_ns =
    static_cast<int64_t>(last_min_rel * static_cast<double>(NSEC_PER_SEC));
  last_trough_ns_ = first_bucket_ns + last_min_offset_ns;
  next_trough_ns_ = last_trough_ns_ + period_ns_;

  // Require at least min_cycles_before_predict cycles within the window.
  const double cycles_observed = window_sec / period_sec;
  ready_ = cycles_observed >= static_cast<double>(config_.min_cycles_before_predict);
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
  ROSBAG2_CPP_LOG_DEBUG_STREAM("Throughput predictor: current_time_ns=" << current_time_ns << " last_trough_ns_=" << last_trough_ns_ << " period_ns_=" << period_ns_ << " k=" << k << " predicted_trough_ns=" << trough);
  const bool in_window = in_trough_window(current_time_ns, trough);
  ROSBAG2_CPP_LOG_DEBUG_STREAM("Throughput predictor: in_trough_window=" << in_window);
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
