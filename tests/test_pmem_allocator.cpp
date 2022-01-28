/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <chrono>
#include <ctime>
#include <deque>
#include <future>
#include <string>
#include <sys/time.h>
#include <thread>
#include <vector>

#include "../engine/kv_engine.hpp"
#include "../engine/logger.hpp"
#include "../engine/thread_manager.hpp"

#include "gtest/gtest.h"

#include "kvdk/engine.hpp"
#include "pmem_allocator/free_list.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "test_util.h"

using namespace KVDK_NAMESPACE;

class EnginePMemAllocatorTest : public testing::Test {
protected:
  Engine *engine = nullptr;
  Configs configs;
  std::shared_ptr<ThreadManager> thread_manager_;
  std::string pmem_path;

  virtual void SetUp() override {
    pmem_path = "/mnt/pmem0/kvdk_pmem_allocator";
    GlobalLogger.Init(stdout, LogLevel::All);
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", pmem_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  virtual void TearDown() { // delete db_path
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", pmem_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }
};

TEST_F(EnginePMemAllocatorTest, TestBasicAlloc) {
  uint64_t pmem_size = 128ULL << 20; // 128MB
  uint64_t alloc_size = 8;

  // params config
  std::vector<uint64_t> num_segment_blocks{1024, 2 * 1024, 2 * 1024 * 1024};
  std::vector<uint32_t> block_sizes{16, 32, 64};
  std::vector<uint32_t> num_threads = {1, 16};

  for (int i = 0; i < num_segment_blocks.size(); ++i) {
    for (auto num_thread : num_threads) {
      thread_manager_.reset(new (std::nothrow) ThreadManager(num_thread));

      // Test function.
      auto TestPmemAlloc = [&](uint64_t id) {
        std::vector<SpaceEntry> records;
        thread_manager_->MaybeInitThread(access_thread);
        PMEMAllocator *pmem_alloc = PMEMAllocator::NewPMEMAllocator(
            pmem_path, pmem_size, num_segment_blocks[i], block_sizes[i],
            num_thread, true, false, nullptr);
        ASSERT_NE(pmem_alloc, nullptr);

        uint64_t kvpairs = pmem_size / block_sizes[i];
        for (uint64_t j = 0; j < kvpairs; ++j) {
          auto space_entry = pmem_alloc->Allocate(alloc_size);
          records.push_back(space_entry);
        }

        auto alloc_bytes = pmem_alloc->PMemUsageInBytes();

        for (uint64_t j = 0; j < records.size(); ++j) {
          pmem_alloc->Free(records[j]);
        }

        ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), (std::int64_t)0);
        records.clear();

        uint64_t alloc_cnt = 1;
        // again allocate pmem
        while (true) {
          auto space_entry = pmem_alloc->Allocate(alloc_size);
          if (pmem_alloc->PMemUsageInBytes() == alloc_bytes)
            break;
          alloc_cnt++;
        }
        ASSERT_EQ(kvpairs, alloc_cnt);
        delete pmem_alloc;
      };

      LaunchNThreads(num_thread, TestPmemAlloc);
    }
  }
}

TEST_F(EnginePMemAllocatorTest, TestPMemFragmentation) {
  uint32_t num_thread = 16;
  uint64_t pmem_size = 64ULL << 10;
  uint64_t num_segment_block = 1024;
  uint64_t block_size = 64;
  std::vector<uint64_t> alloc_size{8 * 64, 8 * 64, 16 * 64, 32 * 64};
  thread_manager_.reset(new ThreadManager(num_thread));
  PMEMAllocator *pmem_alloc = PMEMAllocator::NewPMEMAllocator(
      pmem_path, pmem_size, num_segment_block, block_size, num_thread, true,
      false, nullptr);
  ASSERT_NE(pmem_alloc, nullptr);

  /* Allocated pmem status (block nums):
   * | 8 | 8 | 16 | 32 | 8 | 8 | 16 | 32 | 8 | 8 | 16 | 32 | 8 | 8 | 16 | 32 |
   */

  std::vector<SpaceEntry> records(num_thread);
  thread_manager_->MaybeInitThread(access_thread);
  for (uint32_t i = 0; i < records.size(); ++i) {
    SpaceEntry space_entry = pmem_alloc->Allocate(alloc_size[i % 4]);
    records[i] = space_entry;
    ASSERT_NE(space_entry.size, 0);
  }

  /* Allocated pmem status:
   * | null | null | null | 32 | null | null | null | 32 | null | null | null
   * | 32 | null | null | null | 32 |
   */
  auto TestPmemFree = [&](uint64_t id) {
    thread_manager_->MaybeInitThread(access_thread);
    if ((id + 1) % 4 != 0) {
      pmem_alloc->Free(records[id]);
    }
  };
     pmem_alloc->BackgroundWork();
  // Test merge free memory
  auto TestPmemFrag = [&](uint64_t id) {
    thread_manager_->MaybeInitThread(access_thread);
    if ((id + 1) % 4 == 0) {
      SpaceEntry space_entry = pmem_alloc->Allocate(alloc_size[id % 4]);
      ASSERT_NE(space_entry.size, 0);
    }
  };
  LaunchNThreads(num_thread, TestPmemFree);
  LaunchNThreads(num_thread, TestPmemFrag);

  delete pmem_alloc;
}

// TODO: Add more cases
TEST_F(EnginePMemAllocatorTest, TestPMemAlocFreeList) {
  uint32_t num_thread = 1;
  uint64_t num_segment_block = 4 * kMinPaddingBlocks;
  uint64_t block_size = 64;
  uint64_t pmem_size = num_segment_block * block_size;
  std::vector<int> alloc_size{1024, 512};
  std::deque<SpaceEntry> records;
  thread_manager_.reset(new ThreadManager(num_thread));
  PMEMAllocator *pmem_alloc = PMEMAllocator::NewPMEMAllocator(
      pmem_path, pmem_size, num_segment_block, block_size, num_thread, true,
      false, nullptr);
  ASSERT_NE(pmem_alloc, nullptr);

  thread_manager_->MaybeInitThread(access_thread);
  // allocate 1024 bytes
  records.push_back(pmem_alloc->Allocate(alloc_size[0]));
  ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), (std::int64_t)1024);
  //  allocate 512 bytes
  records.push_back(pmem_alloc->Allocate(alloc_size[1]));
  ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), (std::int64_t)1536);
  //  allocate 512 bytes
  records.push_back(pmem_alloc->Allocate(alloc_size[1]));
  ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), pmem_size);

  // free 1024 bytes
  pmem_alloc->Free(records.front());
  ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), (std::int64_t)1024);
  records.pop_front();

  // allocate 512 bytes, reuse freed 1024 bytes space
  records.push_back(pmem_alloc->Allocate(alloc_size[1]));
  ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), (std::int64_t)1536);
  // allocate 512 bytes
  records.push_back(pmem_alloc->Allocate(alloc_size[1]));
  ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), pmem_size);

  // free 512 bytes and again free 512 bytes
  std::vector<SpaceEntry> frees;
  frees.push_back(records.back());
  records.pop_back();
  frees.push_back(records.front());
  records.pop_front();
  pmem_alloc->BatchFree(frees);

  // need to merge
  pmem_alloc->BackgroundWork();

  // allocate 1024 bytes
  records.push_back(pmem_alloc->Allocate(alloc_size[0]));
  ASSERT_EQ(pmem_alloc->PMemUsageInBytes(), pmem_size);
  delete pmem_alloc;
}