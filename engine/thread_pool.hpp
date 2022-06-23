/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <stdio.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>
#include <vector>

namespace KVDK_NAMESPACE {

class ThreadPool {
 private:
  static constexpr uint64_t kMaxTasksNum = 1024;
  int64_t max_thread_num_;
  std::atomic_bool close_;
  std::mutex queue_mtx_;
  std::queue<std::function<void()>> tasks_;
  int live_thread_num_;
  int idle_thread_num_;
  std::unordered_map<std::thread::id, std::thread> workers_;
  std::queue<std::thread::id> recycle_worker_ids_;

  std::condition_variable tasks_cv_;

  std::chrono::seconds timout_{10};

 private:
  void addWorker();
  void joinWorker();

 public:
  ThreadPool(int64_t max_thread_num)
      : max_thread_num_(max_thread_num),
        close_(false),
        live_thread_num_(0),
        idle_thread_num_(0) {}

  ~ThreadPool() { CloseAllWorkers(); }
  template <typename Func>
  bool PushTask(const Func& task);
  int ThreadNum() { return live_thread_num_; };
  bool Busy() {
    return live_thread_num_ == max_thread_num_ &&
           live_thread_num_ < tasks_.size();
  }
  void CloseAllWorkers();
};

inline void ThreadPool::addWorker() {
  while (true) {
    if (close_) break;
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(queue_mtx_);
      ++idle_thread_num_;
      auto is_timeout = !tasks_cv_.wait_for(lock, timout_, [this] {
        return this->close_ || !this->tasks_.empty();
      });

      --idle_thread_num_;

      if (tasks_.empty()) {
        if (close_) {
          --live_thread_num_;
          return;
        }
        if (is_timeout) {
          --live_thread_num_;
          recycle_worker_ids_.emplace(std::this_thread::get_id());
          joinWorker();
          return;
        }
      }

      task = std::move(tasks_.front());
      tasks_.pop();
    }
    task();
  }
}

template <typename Func>
inline bool ThreadPool::PushTask(const Func& task) {
  auto packed_task = std::make_shared<std::packaged_task<void()>>(task);
  std::unique_lock<std::mutex> lock(queue_mtx_);
  if (tasks_.size() > live_thread_num_) {
    return false;
  }
  tasks_.emplace([packed_task]() { (*packed_task)(); });
  if (idle_thread_num_ > 0) {
    tasks_cv_.notify_one();
  } else if (live_thread_num_ < max_thread_num_) {
    std::thread worker(&ThreadPool::addWorker, this);
    assert(workers_.find(worker.get_id()) == workers_.end());
    workers_[worker.get_id()] = std::move(worker);
    ++live_thread_num_;
  }
  return true;
}

inline void ThreadPool::joinWorker() {
  while (!recycle_worker_ids_.empty()) {
    auto id = recycle_worker_ids_.front();
    recycle_worker_ids_.pop();
    auto iter = workers_.find(id);

    kvdk_assert(iter != workers_.end(),
                "joined thread id should be in live worker threads");
    if (iter->second.joinable()) {
      iter->second.join();
    }
    workers_.erase(iter);
  }
}

inline void ThreadPool::CloseAllWorkers() {
  {
    std::unique_lock<std::mutex> lock(queue_mtx_);
    close_ = true;
    tasks_cv_.notify_all();
  }
  for (auto& work : workers_) {
    if (work.second.joinable()) {
      work.second.join();
    }
  }
}
}  // namespace KVDK_NAMESPACE