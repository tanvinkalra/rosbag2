// Copyright 2020, Robotec.ai sp. z o.o.
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

#include <chrono>
#include <memory>
#include <sstream>

#include "rosbag2_cpp/cache/cache_consumer.hpp"
#include "rosbag2_cpp/logging.hpp"

namespace rosbag2_cpp
{
namespace cache
{

CacheConsumer::CacheConsumer(
  std::shared_ptr<MessageCacheInterface> message_cache,
  consume_callback_function_t consume_callback)
: message_cache_(message_cache),
  consume_callback_(consume_callback)
{
  consumer_thread_ = std::thread(&CacheConsumer::exec_consuming, this);
}

CacheConsumer::~CacheConsumer()
{
  stop();
}

void CacheConsumer::stop()
{
  message_cache_->begin_flushing();
  is_stop_issued_ = true;

  ROSBAG2_CPP_LOG_INFO_STREAM(
    "Writing remaining messages from cache to the bag. It may take a while");

  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }
  message_cache_->done_flushing();
}

void CacheConsumer::start()
{
  is_stop_issued_ = false;
  if (!consumer_thread_.joinable()) {
    consumer_thread_ = std::thread(&CacheConsumer::exec_consuming, this);
  }
}

void CacheConsumer::exec_consuming()
{
  bool exit_flag = false;
  bool flushing = false;
  while (!exit_flag) {
    message_cache_->wait_for_data();
    message_cache_->swap_buffers();
    // Get the current consumer buffer.
    auto consumer_buffer = message_cache_->get_consumer_buffer();

    // Log flush time and buffer space utilisation (PROFILE format for parsing/visualization)
    static uint64_t flush_id = 0;
    const uint64_t id = flush_id++;
    size_t used_bytes = 0u;
    size_t capacity_bytes = 0u;
    if (!consumer_buffer->get_utilisation(used_bytes, capacity_bytes)) {
      for (const auto & msg : consumer_buffer->data()) {
        if (msg && msg->serialized_data) {
          used_bytes += msg->serialized_data->buffer_length;
        }
      }
    }
    const auto flush_time = std::chrono::system_clock::now();
    const auto wall_s = std::chrono::duration_cast<std::chrono::seconds>(
      flush_time.time_since_epoch()).count();
    std::ostringstream log_msg;
    log_msg << "PROFILE component=cache flush_id=" << id << " wall_s=" << wall_s
            << " messages=" << consumer_buffer->size() << " used_B=" << used_bytes
            << " capacity_B=" << capacity_bytes;
    if (capacity_bytes > 0u) {
      const double ratio_pct = (100.0 * static_cast<double>(used_bytes)) /
        static_cast<double>(capacity_bytes);
      log_msg << " ratio_pct=" << ratio_pct;
    } else {
      log_msg << " ratio_pct=N/A";
    }
    ROSBAG2_CPP_LOG_DEBUG_STREAM(log_msg.str());

    consume_callback_(consumer_buffer->data());
    consumer_buffer->clear();
    message_cache_->release_consumer_buffer();

    if (flushing) {exit_flag = true;}  // this was the final run
    if (is_stop_issued_) {flushing = true;}  // run one final time to flush
  }
}

}  // namespace cache
}  // namespace rosbag2_cpp
