/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <stdio.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>
#include <vector>

namespace KVDK_NAMESPACE {
template <typename Func>
class ThreadPool {
 private:
  static constexpr uint64_t kMaxTasksNum = 1024;
  int64_t max_thread_num_;
  std::atomic_bool close_;
  std::mutex queue_mtx_;
  std::queue<Func> tasks_;
  std::atomic_int live_thread_num_{0};

  std::condition_variable tasks_cv_;

  std::chrono::seconds timout_{10};

 private:
  void addWorker();

 public:
  ThreadPool(int64_t max_thread_num)
      : max_thread_num_(max_thread_num), close_(false) {}
  ~ThreadPool() { CloseAllWorkers(); }
  bool PushTask(Func& task);
  void CloseAllWorkers();
};

template <typename Func>
inline void ThreadPool<Func>::addWorker() {
  ++live_thread_num_;
  std::thread worker([this]() {
    while (true) {
      if (close_) break;
      std::function<void()> task;
      int try_get_task = 0;
      while (true) {
        {
          std::unique_lock<std::mutex> lock(queue_mtx_);
          if (!this->tasks_.empty() && !close_) {
            ++try_get_task;
            break;
          }
          task = std::move(this->tasks_.front());
          this->tasks_.pop();
        }
        task();
      }

      if (try_get_task > 2) break;

      std::unique_lock<std::mutex> lock(queue_mtx_);
      tasks_cv_.wait_for(lock, timout_, [this] {
        return this->close_ && !this->tasks_.empty();
      });
    }
    --live_thread_num_;
  });
  worker.detach();
}

template <typename Func>
inline bool ThreadPool<Func>::PushTask(Func& task) {
  if (close_) {
    return false;
  }

  std::unique_lock<std::mutex> lock(queue_mtx_);
  tasks_.push_back(task);
  auto task_size = tasks_.size();
  lock.unlock();
  if (live_thread_num_ == 0 ||
      (task_size > kMaxTasksNum / 2 && live_thread_num_ < max_thread_num_)) {
    addWorker();
  }
  tasks_cv_.notify_one();
  return true;
}

template <typename Func>
inline void ThreadPool<Func>::CloseAllWorkers() {
  close_.store(true);
  {
    std::unique_lock<std::mutex> lock(queue_mtx_);
    close_ = true;
  }
  tasks_cv_.notify_all();
  while (live_thread_num_ > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}
}  // namespace KVDK_NAMESPACE