/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */
#pragma once

#include <stdio.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "alias.hpp"
#include "collection.hpp"
#include "hash_table.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

struct PendingFreeSpaceEntries {
  std::vector<SpaceEntry> entries;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely free these entries
  TimeStampType release_time;
};

struct PendingFreeSpaceEntry {
  SpaceEntry entry;
  // Indicate timestamp of the oldest refered snapshot of kvdk instance while we
  // could safely free this entry
  TimeStampType release_time;
};

struct PendingPurgeStrRecords {
  std::vector<StringRecord*> records;
  TimeStampType release_time;
};

struct PendingPurgeDLRecords {
  std::vector<DLRecord*> records;
  TimeStampType release_time;
};

struct PendingPrugeFreeRecords {
  using ListPtr = std::unique_ptr<List>;
  using HashListPtr = std::unique_ptr<HashList>;

  std::deque<std::pair<TimeStampType, ListPtr>> outdated_lists;
  std::deque<std::pair<TimeStampType, HashListPtr>> outdated_hash_lists;
  std::deque<std::pair<TimeStampType, Skiplist*>> outdated_skip_lists;
  std::deque<PendingPurgeStrRecords> pending_purge_strings;
  std::deque<PendingPurgeDLRecords> pending_purge_dls;
  std::deque<Skiplist*> no_index_skiplists;
  size_t Size() {
    return outdated_lists.size() + outdated_hash_lists.size() +
           outdated_skip_lists.size() + pending_purge_strings.size() +
           pending_purge_dls.size();
  }
};

class KVEngine;

class SpaceReclaimer {
 public:
  static constexpr size_t kSlotBlockUnit = 1024;
  static constexpr double kWakeUpThreshold = 0.1;

  SpaceReclaimer(KVEngine* kv_engine, int64_t max_cleaner_threads)
      : kv_engine_(kv_engine),
        max_thread_num_(max_cleaner_threads),
        close_(false),
        live_thread_num_(0) {}

  ~SpaceReclaimer() { CloseAllWorkers(); }

  void CloseAllWorkers();
  void AdjustThread(int64_t advice_thread_num);

  void StartReclaiming();

 private:
  enum class ThreadStatus { Continued, Recycled };
  struct ThreadWorker {
    ThreadStatus status;
    std::thread worker;
  };
  KVEngine* kv_engine_;
  PendingPrugeFreeRecords pending_clean_records_;

  int64_t max_thread_num_;
  std::atomic_bool close_;
  std::atomic_int live_thread_num_;
  int64_t slot_segment_;
  std::unordered_map<size_t, ThreadWorker> workers_;
  std::queue<size_t> recycle_worker_ids_;

  SpinMutex mtx_;
  std::condition_variable tasks_cv_;

 private:
  void addWorker(size_t thread_id);
  void joinWorker();
  void cleanOutDated(size_t start_slot_idx, size_t end_slot_idx);
};

inline void SpaceReclaimer::CloseAllWorkers() {
  close_ = true;
  for (auto& work : workers_) {
    if (work.second.worker.joinable()) {
      work.second.worker.join();
    }
  }
}

inline void SpaceReclaimer::joinWorker() {
  std::unique_lock<SpinMutex> worker_lock(mtx_);
  while (!recycle_worker_ids_.empty()) {
    auto id = recycle_worker_ids_.front();
    recycle_worker_ids_.pop();
    auto iter = workers_.find(id);

    kvdk_assert(iter != workers_.end(),
                "joined thread id should be in live worker threads");
    if (iter->second.worker.joinable()) {
      iter->second.worker.join();
    }
    workers_.erase(iter);
  }
}

}  // namespace KVDK_NAMESPACE