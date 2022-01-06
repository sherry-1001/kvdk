/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <future>
#include <string>
#include <thread>
#include <vector>

#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"

#include "../engine/kv_engine.hpp"
#include "../engine/pmem_allocator/pmem_allocator.hpp"

#include "test_util.h"

using namespace KVDK_NAMESPACE;
static const uint64_t str_pool_length = 1024000;

using SetOpsFunc =
    std::function<Status(const std::string &collection, const std::string &key,
                         const std::string &value)>;
using DeleteOpsFunc = std::function<Status(const std::string &collection,
                                           const std::string &key)>;

using GetOpsFunc = std::function<Status(
    const std::string &collection, const std::string &key, std::string *value)>;

enum Types { kString, kSorted, kHash, kQueue };

class EngineBasicTest : public testing::Test {
protected:
  Engine *engine = nullptr;
  Configs configs;
  std::string db_path;
  std::string str_pool;

  virtual void SetUp() override {
    str_pool.resize(str_pool_length);
    random_str(&str_pool[0], str_pool_length);
    configs.pmem_file_size = (16ULL << 30);
    configs.populate_pmem_space = false;
    configs.hash_bucket_num = (1 << 10);
    configs.hash_bucket_size = 64;
    configs.pmem_segment_blocks = 8 * 1024;
    // For faster test, no interval so it would not block engine closing
    configs.background_work_interval = 0.1;
    configs.log_level = LogLevel::All;
    configs.max_write_threads = 1;
    db_path = "/mnt/pmem0/data";
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    int res __attribute__((unused)) = system(cmd);
    config_option = OptionConfig::kDefault;
    cnt = 500;
  }

  virtual void TearDown() { Destroy(); }

  void AssignData(std::string &data, int len) {
    data.assign(str_pool.data() + (rand() % (str_pool_length - len)), len);
  }

  void Destroy() {
    // delete db_path
    char cmd[1024];
    sprintf(cmd, "rm -rf %s\n", db_path.c_str());
    int res __attribute__((unused)) = system(cmd);
  }

  bool ChangedConfig() {
    config_option++;
    if (config_option >= kEnd) {
      return false;
    } else {
      ReopenEngine();
      return true;
    }
  }

  void ReopenEngine() {
    delete engine;
    engine = nullptr;
    configs = CurrentConfigs();
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
  }

  // Return the current configuration.
  Configs CurrentConfigs() {
    switch (config_option) {
    case kMultiThread:
      configs.max_write_threads = 16;
      break;
    case kOptRestore:
      configs.opt_large_sorted_collection_restore = true;
      break;
    default:
      break;
    }
    return configs;
  }

  // Set/Get/Delete
  void GlobalBasicOperator(const std::string &collection, SetOpsFunc SetFunc,
                           GetOpsFunc GetFunc, DeleteOpsFunc DeleteFunc,
                           Types type) {
    // Maybe having create collection for all collection types.
    if (type == Types::kSorted) {
      Collection *collection_ptr;
      ASSERT_EQ(engine->CreateSortedCollection(collection, &collection_ptr),
                Status::Ok);
    }
    TestEmptyKey(collection, SetFunc, GetFunc, DeleteFunc);
    LaunchNThreads(configs.max_write_threads,
                   BasicOperator(collection, SetFunc, GetFunc, DeleteFunc));
  }

  void LocalCollectionBasicOperator(const std::string &collection,
                                    SetOpsFunc SetFunc, GetOpsFunc GetFunc,
                                    DeleteOpsFunc DeleteFunc) {
    auto Local_XSetXGetXDelete = [&](uint64_t id) {
      std::string thread_local_collection = collection + std::to_string(id);
      Collection *local_collection_ptr;
      ASSERT_EQ(engine->CreateSortedCollection(thread_local_collection,
                                               &local_collection_ptr),
                Status::Ok);

      TestEmptyKey(thread_local_collection, SetFunc, GetFunc, DeleteFunc);

      auto BasicOpFunc =
          BasicOperator(thread_local_collection, SetFunc, GetFunc, DeleteFunc);
      BasicOpFunc(id);
    };
    LaunchNThreads(configs.max_write_threads, Local_XSetXGetXDelete);
  }

  void SeekIterator(const std::string &collection, Types type,
                    bool is_local = false) {
    if (type != Types::kSorted && type != Types::kHash) {
      return;
    }

    auto IteratingThrough = [&](uint32_t id) {
      std::atomic<int> entries(0);
      std::string new_collection = collection;
      if (is_local) {
        new_collection += std::to_string(id);
      }
      auto iter = type == Types::kSorted
                      ? engine->NewSortedIterator(new_collection)
                      : engine->NewUnorderedIterator(new_collection);

      ASSERT_TRUE(iter != nullptr);
      iter->SeekToFirst();
      if (iter->Valid()) {
        ++entries;
        std::string prev = iter->Key();
        iter->Next();
        while (iter->Valid()) {
          ++entries;
          std::string k = iter->Key();
          iter->Next();
          if (type == Types::kSorted) {
            ASSERT_EQ(true, k.compare(prev) > 0);
          }
          prev = k;
        }
      }
      if (is_local) {
        ASSERT_EQ(cnt, entries);
      } else {
        ASSERT_EQ(cnt * configs.max_write_threads, entries);
      }

      iter->SeekToLast();
      if (iter->Valid()) {
        --entries;
        std::string next = iter->Key();
        iter->Prev();
        while (iter->Valid()) {
          --entries;
          std::string k = iter->Key();
          iter->Prev();
          if (type == Types::kSorted) {
            ASSERT_EQ(true, k.compare(next) < 0);
          }
          next = k;
        }
      }
      ASSERT_EQ(entries, 0);
    };
    LaunchNThreads(configs.max_write_threads, IteratingThrough);
  }

private:
  void TestEmptyKey(const std::string &collection, SetOpsFunc SetFunc,
                    GetOpsFunc GetFunc, DeleteOpsFunc DeleteFunc) {
    std::string key, val, got_val;
    key = "", val = "val";
    ASSERT_EQ(SetFunc(collection, key, val), Status::Ok);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::Ok);
    ASSERT_EQ(val, got_val);
    ASSERT_EQ(DeleteFunc(collection, key), Status::Ok);
    ASSERT_EQ(GetFunc(collection, key, &got_val), Status::NotFound);
    engine->ReleaseWriteThread();
  }

  std::function<void(uint32_t)> BasicOperator(const std::string &collection,
                                              SetOpsFunc SetFunc,
                                              GetOpsFunc GetFunc,
                                              DeleteOpsFunc DeleteFunc) {
    return [&](uint32_t id) {
      std::string val1, val2, got_val1, got_val2;
      int t_cnt = cnt;
      while (t_cnt--) {
        std::string key1(std::string(id + 1, 'a') + std::to_string(t_cnt));
        std::string key2(std::string(id + 1, 'b') + std::to_string(t_cnt));
        AssignData(val1, fast_random_64() % 1024);
        AssignData(val2, fast_random_64() % 1024);

        // Set
        ASSERT_EQ(SetFunc(collection, key1, val1), Status::Ok);
        ASSERT_EQ(SetFunc(collection, key2, val2), Status::Ok);

        // Get
        ASSERT_EQ(GetFunc(collection, key1, &got_val1), Status::Ok);
        ASSERT_EQ(val1, got_val1);
        ASSERT_EQ(GetFunc(collection, key2, &got_val2), Status::Ok);
        ASSERT_EQ(val2, got_val2);

        // Delete
        ASSERT_EQ(DeleteFunc(collection, key1), Status::Ok);
        ASSERT_EQ(GetFunc(collection, key1, &got_val1), Status::NotFound);

        // Update
        AssignData(val2, fast_random_64() % 1024);
        ASSERT_EQ(SetFunc(collection, key2, val2), Status::Ok);
        ASSERT_EQ(GetFunc(collection, key2, &got_val2), Status::Ok);
        ASSERT_EQ(got_val2, val2);
      }
    };
  }

private:
  // Sequence of option configurations to try
  enum OptionConfig { kDefault, kMultiThread, kOptRestore, kEnd };
  int config_option;
  int cnt;
};

TEST_F(EngineBasicTest, TestThreadManager) {
  int max_write_threads = 1;
  configs.max_write_threads = max_write_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  std::string key("k");
  std::string val("value");

  ASSERT_EQ(engine->Set(key, val), Status::Ok);

  // Reach max write threads
  auto s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::TooManyWriteThreads);
  // Manually release write thread
  engine->ReleaseWriteThread();
  s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::Ok);
  // Release write thread on thread exits
  s = std::async(&Engine::Set, engine, key, val);
  ASSERT_EQ(s.get(), Status::Ok);
  delete engine;
}

TEST_F(EngineBasicTest, TestBasicStringOperations) {
  auto StringSetFunc = [&](const std::string &collection,
                           const std::string &key,
                           const std::string &value) -> Status {
    return engine->Set(key, value);
  };

  auto StringGetFunc =
      [&](const std::string &collection, const std::string &key,
          std::string *value) -> Status { return engine->Get(key, value); };

  auto StringDeleteFunc = [&](const std::string &collection,
                              const std::string &key) -> Status {
    return engine->Delete(key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  do {
    GlobalBasicOperator("global_string", StringSetFunc, StringGetFunc,
                        StringDeleteFunc, Types::kString);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestBatchWrite) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  int batch_size = 10;
  int count = 500;
  auto BatchSetDelete = [&](uint32_t id) {
    std::string key_prefix(std::string(id, 'a'));
    std::string got_val;
    WriteBatch batch;
    int cnt = count;
    while (cnt--) {
      for (size_t i = 0; i < batch_size; i++) {
        auto key = key_prefix + std::to_string(i) + std::to_string(cnt);
        auto val = std::to_string(i * id);
        batch.Put(key, val);
      }
      ASSERT_EQ(engine->BatchWrite(batch), Status::Ok);
      batch.Clear();
      for (size_t i = 0; i < batch_size; i++) {
        if ((i * cnt) % 2 == 1) {
          auto key = key_prefix + std::to_string(i) + std::to_string(cnt);
          auto val = std::to_string(i * id);
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, val);
          batch.Delete(key);
        }
      }
      engine->BatchWrite(batch);
      batch.Clear();
    }
  };

  LaunchNThreads(num_threads, BatchSetDelete);

  delete engine;

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (uint32_t id = 0; id < num_threads; id++) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    int cnt = count;
    while (cnt--) {
      for (size_t i = 0; i < batch_size; i++) {
        auto key = key_prefix + std::to_string(i) + std::to_string(cnt);
        if ((i * cnt) % 2 == 1) {
          ASSERT_EQ(engine->Get(key, &got_val), Status::NotFound);
        } else {
          auto val = std::to_string(i * id);
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, val);
        }
      }
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestFreeList) {
  // TODO: Add more cases
  configs.pmem_segment_blocks = 4 * kMinPaddingBlocks;
  configs.max_write_threads = 1;
  configs.pmem_block_size = 64;
  configs.pmem_file_size =
      configs.pmem_segment_blocks * configs.pmem_block_size;
  configs.background_work_interval = 0.5;

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::string key1("a1");
  std::string key2("a2");
  std::string key3("a3");
  std::string key4("a4");
  std::string small_value(64 * (kMinPaddingBlocks - 1) + 1, 'a');
  std::string large_value(64 * (kMinPaddingBlocks * 2 - 1) + 1, 'a');
  // We have 4 kMinimalPaddingBlockSize size chunk of blocks, this will take
  // up 2 of them

  ASSERT_EQ(engine->Set(key1, large_value), Status::Ok);

  // update large value, new value will be stored in 3th chunk
  ASSERT_EQ(engine->Set(key1, small_value), Status::Ok);

  // key2 will be stored in 4th chunk
  ASSERT_EQ(engine->Set(key2, small_value), Status::Ok);

  // new key 1 and new key 2 will be stored in updated 1st and 2nd chunks
  ASSERT_EQ(engine->Set(key1, small_value), Status::Ok);

  ASSERT_EQ(engine->Set(key2, small_value), Status::Ok);

  // No more space to store large_value
  ASSERT_EQ(engine->Set(key3, large_value), Status::PmemOverflow);

  // Wait bg thread finish merging space of 3th and 4th chunks
  sleep(2);

  // large key3 will be stored in merged 3th and 4th chunks
  ASSERT_EQ(engine->Set(key3, large_value), Status::Ok);

  // No more space
  ASSERT_EQ(engine->Set(key4, small_value), Status::PmemOverflow);

  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // Still no more space after re-open
  ASSERT_EQ(engine->Set(key4, small_value), Status::PmemOverflow);
}

TEST_F(EngineBasicTest, TestLocalSortedCollection) {
  auto SortedSetFunc = [&](const std::string &collection,
                           const std::string &key,
                           const std::string &value) -> Status {
    return engine->SSet(collection, key, value);
  };

  auto SortedGetFunc = [&](const std::string &collection,
                           const std::string &key,
                           std::string *value) -> Status {
    return engine->SGet(collection, key, value);
  };

  auto SortedDeleteFunc = [&](const std::string &collection,
                              const std::string &key) -> Status {
    return engine->SDelete(collection, key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  do {
    LocalCollectionBasicOperator("thread_skiplist", SortedSetFunc,
                                 SortedGetFunc, SortedDeleteFunc);
    SeekIterator("thread_skiplist", Types::kSorted, true);
  } while (ChangedConfig());

  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalSortedCollection) {
  std::atomic<int> n_global_entries{0};

  auto SortedSetFunc = [&](const std::string &collection,
                           const std::string &key,
                           const std::string &value) -> Status {
    return engine->SSet(collection, key, value);
  };

  auto SortedGetFunc = [&](const std::string &collection,
                           const std::string &key,
                           std::string *value) -> Status {
    return engine->SGet(collection, key, value);
  };

  auto SortedDeleteFunc = [&](const std::string &collection,
                              const std::string &key) -> Status {
    return engine->SDelete(collection, key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::string collection = "global_skiplist";
  do {
    GlobalBasicOperator(collection, SortedSetFunc, SortedGetFunc,
                        SortedDeleteFunc, Types::kSorted);
    SeekIterator(collection, Types::kSorted, false);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestSeek) {
  std::string val;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // Test Seek
  std::string collection = "col1";
  Collection *collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(collection, &collection_ptr),
            Status::Ok);
  uint64_t z = 0;
  auto zero_filled_str = uint64_to_string(z);
  printf("%s\n", zero_filled_str.c_str());
  ASSERT_EQ(engine->SSet(collection, zero_filled_str, zero_filled_str),
            Status::Ok);
  ASSERT_EQ(engine->SGet(collection, zero_filled_str, &val), Status::Ok);
  auto iter = engine->NewSortedIterator(collection);
  ASSERT_NE(iter, nullptr);
  iter->Seek(zero_filled_str);
  ASSERT_TRUE(iter->Valid());

  // Test SeekToFirst
  collection.assign("col2");
  ASSERT_EQ(engine->CreateSortedCollection(collection, &collection_ptr),
            Status::Ok);
  ASSERT_EQ(engine->SSet(collection, "foo", "bar"), Status::Ok);
  ASSERT_EQ(engine->SGet(collection, "foo", &val), Status::Ok);
  ASSERT_EQ(engine->SDelete(collection, "foo"), Status::Ok);
  ASSERT_EQ(engine->SGet(collection, "foo", &val), Status::NotFound);
  ASSERT_EQ(engine->SSet(collection, "foo2", "bar2"), Status::Ok);
  iter = engine->NewSortedIterator(collection);
  ASSERT_NE(iter, nullptr);
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->Value(), "bar2");
}

TEST_F(EngineBasicTest, TestStringRestore) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  int count = 1000;
  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (int i = 1; i <= count; i++) {
      std::string key(key_prefix + std::to_string(i));
      std::string val(std::to_string(i));
      std::string update_val(std::to_string(i * 2));
      ASSERT_EQ(engine->Set(key, val), Status::Ok);
      if ((i * id) % 2 == 1) {
        ASSERT_EQ(engine->Delete(key), Status::Ok);
        if ((i * id) % 3 == 0) {
          // Update after delete
          ASSERT_EQ(engine->Set(key, update_val), Status::Ok);
          ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
          ASSERT_EQ(got_val, update_val);
        } else {
          ASSERT_EQ(engine->Get(key, &got_val), Status::NotFound);
        }
      } else {
        ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
        ASSERT_EQ(got_val, val);
      }
    }
  };

  LaunchNThreads(num_threads, SetupEngine);

  delete engine;

  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  for (uint32_t id = 0; id < num_threads; id++) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (int i = 1; i <= count; i++) {
      std::string key(key_prefix + std::to_string(i));
      std::string val(std::to_string(i));
      std::string updated_val(std::to_string(i * 2));
      Status s = engine->Get(key, &got_val);
      if ((i * id) % 3 == 0 && (id * i) % 2 == 1) { // deleted then updated ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(got_val, updated_val);
      } else if ((i * id) % 2 == 0) { // not deleted ones
        ASSERT_EQ(s, Status::Ok);
        ASSERT_EQ(got_val, val);
      } else { // deleted ones
        ASSERT_EQ(s, Status::NotFound);
      }
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestSortedRestore) {
  int num_threads = 16;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  int count = 100;
  std::string overall_skiplist = "skiplist";
  Collection *overall_collection_ptr;
  ASSERT_EQ(
      engine->CreateSortedCollection(overall_skiplist, &overall_collection_ptr),
      Status::Ok);
  std::string thread_skiplist = "t_skiplist";
  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    std::string t_skiplist(thread_skiplist + std::to_string(id));
    Collection *thread_collection_ptr;
    ASSERT_EQ(
        engine->CreateSortedCollection(t_skiplist, &thread_collection_ptr),
        Status::Ok);
    for (int i = 1; i <= count; i++) {
      auto key = key_prefix + std::to_string(i);
      auto overall_val = std::to_string(i);
      auto t_val = std::to_string(i * 2);
      ASSERT_EQ(engine->SSet(overall_skiplist, key, overall_val), Status::Ok);
      ASSERT_EQ(engine->SSet(t_skiplist, key, t_val), Status::Ok);
      ASSERT_EQ(engine->SGet(overall_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, overall_val);
      ASSERT_EQ(engine->SGet(t_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, t_val);
      if (i % 2 == 1) {
        ASSERT_EQ(engine->SDelete(overall_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SDelete(t_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SGet(overall_skiplist, key, &got_val),
                  Status::NotFound);
        ASSERT_EQ(engine->SGet(t_skiplist, key, &got_val), Status::NotFound);
      }
    }
  };

  LaunchNThreads(num_threads, SetupEngine);

  delete engine;
  std::vector<int> opt_restore_skiplists{0, 1};
  for (auto is_opt : opt_restore_skiplists) {
    GlobalLogger.Info("Restore with opt_large_sorted_collection_restore: %d\n",
                      is_opt);
    configs.max_write_threads = num_threads;
    configs.opt_large_sorted_collection_restore = is_opt;
    // reopen and restore engine and try gets
    ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
              Status::Ok);
    for (uint32_t id = 0; id < num_threads; id++) {
      std::string t_skiplist(thread_skiplist + std::to_string(id));
      std::string key_prefix(id, 'a');
      std::string got_val;
      for (int i = 1; i <= count; i++) {
        std::string key(key_prefix + std::to_string(i));
        std::string overall_val(std::to_string(i));
        std::string t_val(std::to_string(i * 2));
        Status s = engine->SGet(overall_skiplist, key, &got_val);
        if (i % 2 == 1) {
          ASSERT_EQ(s, Status::NotFound);
        } else {
          ASSERT_EQ(s, Status::Ok);
          ASSERT_EQ(got_val, overall_val);
        }
        s = engine->SGet(t_skiplist, key, &got_val);
        if (i % 2 == 1) {
          ASSERT_EQ(s, Status::NotFound);
        } else {
          ASSERT_EQ(s, Status::Ok);
          ASSERT_EQ(got_val, t_val);
        }
      }

      auto iter = engine->NewSortedIterator(t_skiplist);
      ASSERT_TRUE(iter != nullptr);
      int data_entries_scan = 0;
      iter->SeekToFirst();
      if (iter->Valid()) {
        data_entries_scan++;
        std::string prev = iter->Key();
        iter->Next();
        while (iter->Valid()) {
          data_entries_scan++;
          std::string k = iter->Key();
          iter->Next();
          ASSERT_TRUE(k.compare(prev) > 0);
          prev = k;
        }
      }
      ASSERT_EQ(data_entries_scan, count / 2);

      iter->SeekToLast();
      if (iter->Valid()) {
        data_entries_scan--;
        std::string next = iter->Key();
        iter->Prev();
        while (iter->Valid()) {
          data_entries_scan--;
          std::string k = iter->Key();
          iter->Prev();
          ASSERT_TRUE(k.compare(next) < 0);
          next = k;
        }
      }
      ASSERT_EQ(data_entries_scan, 0);
    }

    int data_entries_scan = 0;
    auto iter = engine->NewSortedIterator(overall_skiplist);
    ASSERT_TRUE(iter != nullptr);
    iter->SeekToFirst();
    if (iter->Valid()) {
      std::string prev = iter->Key();
      data_entries_scan++;
      iter->Next();
      while (iter->Valid()) {
        data_entries_scan++;
        std::string k = iter->Key();
        iter->Next();
        ASSERT_TRUE(k.compare(prev) > 0);
        prev = k;
      }
    }
    ASSERT_EQ(data_entries_scan, (count / 2) * num_threads);

    iter->SeekToLast();
    if (iter->Valid()) {
      std::string next = iter->Key();
      data_entries_scan--;
      iter->Prev();
      while (iter->Valid()) {
        data_entries_scan--;
        std::string k = iter->Key();
        iter->Prev();
        ASSERT_TRUE(k.compare(next) < 0);
        next = k;
      }
    }
    ASSERT_EQ(data_entries_scan, 0);

    delete engine;
  }
}

TEST_F(EngineBasicTest, TestMultiThreadSortedRestore) {
  int num_threads = 48;
  int num_collections = 16;
  configs.max_write_threads = num_threads;
  configs.opt_large_sorted_collection_restore = true;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys
  uint64_t count = 1024;

  std::set<std::string> avg_nums, random_nums;
  for (uint64_t i = 1; i <= count; ++i) {
    std::string average_skiplist("a_skiplist" +
                                 std::to_string(i % num_collections));
    Collection *avg_collection_ptr;
    ASSERT_EQ(
        engine->CreateSortedCollection(average_skiplist, &avg_collection_ptr),
        Status::Ok);
  }
  for (uint32_t i = 0; i < num_threads; ++i) {
    std::string r_skiplist("r_skiplist" + std::to_string(i));
    Collection *r_collection_ptr;
    ASSERT_EQ(engine->CreateSortedCollection(r_skiplist, &r_collection_ptr),
              Status::Ok);
  }
  auto SetupEngine = [&](uint32_t id) {
    std::string key_prefix(id, 'a');
    std::string got_val;
    for (uint64_t i = 1; i <= count; ++i) {
      std::string average_skiplist("a_skiplist" +
                                   std::to_string(i % num_collections));

      std::string r_skiplist("r_skiplist" +
                             std::to_string(rand() % num_threads));

      auto key = key_prefix + std::to_string(i);
      auto average_val = std::to_string(i);
      ASSERT_EQ(engine->SSet(average_skiplist, key, average_val), Status::Ok);
      ASSERT_EQ(engine->SGet(average_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, average_val);
      auto r_val = std::to_string(i * 2);
      ASSERT_EQ(engine->SSet(r_skiplist, key, r_val), Status::Ok);
      ASSERT_EQ(engine->SGet(r_skiplist, key, &got_val), Status::Ok);
      ASSERT_EQ(got_val, r_val);
      if ((rand() % i) == 0) {
        ASSERT_EQ(engine->SDelete(average_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SDelete(r_skiplist, key), Status::Ok);
        ASSERT_EQ(engine->SGet(average_skiplist, key, &got_val),
                  Status::NotFound);
        ASSERT_EQ(engine->SGet(r_skiplist, key, &got_val), Status::NotFound);
      }
    }
  };

  LaunchNThreads(num_threads, SetupEngine);

  delete engine;
  // reopen and restore engine and try gets
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  auto skiplists = (dynamic_cast<KVEngine *>(engine))->GetSkiplists();
  for (int h = 1; h <= 32; ++h) {
    for (auto s : skiplists) {
      ASSERT_EQ(s->CheckConnection(h), Status::Ok);
    }
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestLocalUnorderedCollection) {
  auto UnorderedSetFunc = [&](const std::string &collection,
                              const std::string &key,
                              const std::string &value) -> Status {
    return engine->HSet(collection, key, value);
  };

  auto UnorderedGetFunc = [&](const std::string &collection,
                              const std::string &key,
                              std::string *value) -> Status {
    return engine->HGet(collection, key, value);
  };

  auto UnorderedDeleteFunc = [&](const std::string &collection,
                                 const std::string &key) -> Status {
    return engine->HDelete(collection, key);
  };

  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  do {
    LocalCollectionBasicOperator("thread_unordered", UnorderedSetFunc,
                                 UnorderedGetFunc, UnorderedDeleteFunc);
    SeekIterator("thread_unordered", Types::kHash, true);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestGlobalUnorderedCollection) {
  auto UnorderedSetFunc = [&](const std::string &collection,
                              const std::string &key,
                              const std::string &value) -> Status {
    return engine->HSet(collection, key, value);
  };

  auto UnorderedGetFunc = [&](const std::string &collection,
                              const std::string &key,
                              std::string *value) -> Status {
    return engine->HGet(collection, key, value);
  };

  auto UnorderedDeleteFunc = [&](const std::string &collection,
                                 const std::string &key) -> Status {
    return engine->HDelete(collection, key);
  };
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  do {
    GlobalBasicOperator("global_unordered", UnorderedSetFunc, UnorderedGetFunc,
                        UnorderedDeleteFunc, Types::kHash);
    SeekIterator("global_unordered", Types::kHash, false);
  } while (ChangedConfig());
  delete engine;
}

TEST_F(EngineBasicTest, TestUnorderedCollectionRestore) {
  int count = 100;
  int num_threads = 16;

  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  // kv-pairs for insertion.
  // Remaining kvs in kv engine are also stored in global/tlocal_kvs_remaining
  std::vector<std::vector<std::pair<std::string, std::string>>>
      global_kvs_inserting(num_threads);
  std::vector<std::vector<std::pair<std::string, std::string>>>
      tlocal_kvs_inserting(num_threads);
  std::mutex lock_global_kvs_remaining;
  std::map<std::string, std::string> global_kvs_remaining;
  std::vector<std::map<std::string, std::string>> tlocal_kvs_remaining(
      num_threads);

  std::string global_collection_name = "global_uncoll";
  std::vector<std::string> tlocal_collection_names(num_threads);
  std::string updated_value_suffix = "_new";
  for (size_t tid = 0; tid < num_threads; tid++) {
    tlocal_collection_names[tid] = "local_uncoll_t" + std::to_string(tid);
    for (size_t j = 0; j < count * 2; j++) {
      std::string global_key = std::string{"global_key_t"} +
                               std::to_string(tid) + "_key_" +
                               std::to_string(j);
      global_kvs_inserting[tid].emplace_back(global_key, GetRandomString(1024));

      std::string thread_local_key =
          std::string{"local_key_"} + std::to_string(j);
      tlocal_kvs_inserting[tid].emplace_back(thread_local_key,
                                             GetRandomString(1024));
    }
  }

  auto HSetHGetHDeleteGlobal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      // Insert first kv-pair in global collection
      ASSERT_EQ(engine->HSet(global_collection_name,
                             global_kvs_inserting[tid][j].first,
                             global_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j].second);

      // Update first kv-pair in global collection
      global_kvs_inserting[tid][j].second += updated_value_suffix;
      ASSERT_EQ(engine->HSet(global_collection_name,
                             global_kvs_inserting[tid][j].first,
                             global_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j].second);
      {
        std::lock_guard<std::mutex> lg{lock_global_kvs_remaining};
        global_kvs_remaining.emplace(global_kvs_inserting[tid][j].first,
                                     global_kvs_inserting[tid][j].second);
      }

      // Insert second kv-pair in global collection
      ASSERT_EQ(engine->HSet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             global_kvs_inserting[tid][j + count].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j + count].second);

      // Delete second kv-pair in global collection
      ASSERT_EQ(engine->HDelete(global_collection_name,
                                global_kvs_inserting[tid][j + count].first),
                Status::Ok);
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  auto HSetHGetHDeleteThreadLocal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      // Insert first kv-pair in global collection
      ASSERT_EQ(engine->HSet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first,
                             tlocal_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j].second);

      // Update first kv-pair in global collection
      tlocal_kvs_inserting[tid][j].second += updated_value_suffix;
      ASSERT_EQ(engine->HSet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first,
                             tlocal_kvs_inserting[tid][j].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j].second);
      tlocal_kvs_remaining[tid].emplace(tlocal_kvs_inserting[tid][j].first,
                                        tlocal_kvs_inserting[tid][j].second);

      // Insert second kv-pair in global collection
      ASSERT_EQ(engine->HSet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             tlocal_kvs_inserting[tid][j + count].second),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j + count].second);

      // Delete second kv-pair in global collection
      ASSERT_EQ(engine->HDelete(tlocal_collection_names[tid],
                                tlocal_kvs_inserting[tid][j + count].first),
                Status::Ok);
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  // Setup engine
  LaunchNThreads(num_threads, HSetHGetHDeleteGlobal);
  LaunchNThreads(num_threads, HSetHGetHDeleteThreadLocal);

  auto IteratingThroughGlobal = [&](uint32_t tid) {
    int n_entry = 0;
    auto global_kvs_remaining_copy{global_kvs_remaining};

    auto iter_global_collection =
        engine->NewUnorderedIterator(global_collection_name);
    ASSERT_TRUE(iter_global_collection != nullptr);
    for (iter_global_collection->SeekToFirst(); iter_global_collection->Valid();
         iter_global_collection->Next()) {
      ++n_entry;
      auto key = iter_global_collection->Key();
      auto value = iter_global_collection->Value();
      auto iter_found = global_kvs_remaining_copy.find(key);
      ASSERT_NE(iter_found, global_kvs_remaining_copy.end());
      ASSERT_EQ(value, iter_found->second);
      global_kvs_remaining_copy.erase(key);
    }
    ASSERT_EQ(n_entry, num_threads * count);
    ASSERT_TRUE(global_kvs_remaining_copy.empty());
  };

  auto IteratingThroughThreadLocal = [&](uint32_t tid) {
    int n_entry = 0;
    auto tlocal_kvs_remaining_copy{tlocal_kvs_remaining[tid]};

    auto iter_tlocal_collection =
        engine->NewUnorderedIterator(tlocal_collection_names[tid]);
    ASSERT_TRUE(iter_tlocal_collection != nullptr);
    for (iter_tlocal_collection->SeekToFirst(); iter_tlocal_collection->Valid();
         iter_tlocal_collection->Next()) {
      ++n_entry;
      auto key = iter_tlocal_collection->Key();
      auto value = iter_tlocal_collection->Value();
      auto iter_found = tlocal_kvs_remaining_copy.find(key);
      ASSERT_NE(iter_found, tlocal_kvs_remaining_copy.end());
      ASSERT_EQ(value, iter_found->second);
      tlocal_kvs_remaining_copy.erase(key);
    }
    ASSERT_EQ(n_entry, count);
    ASSERT_TRUE(tlocal_kvs_remaining_copy.empty());
  };

  auto HGetGlobal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, global_kvs_inserting[tid][j].second);

      ASSERT_EQ(engine->HGet(global_collection_name,
                             global_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  auto HGetThreadLocal = [&](uint32_t tid) {
    std::string value_got;
    for (int j = 0; j < count; j++) {
      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j].first, &value_got),
                Status::Ok);
      ASSERT_EQ(value_got, tlocal_kvs_inserting[tid][j].second);

      ASSERT_EQ(engine->HGet(tlocal_collection_names[tid],
                             tlocal_kvs_inserting[tid][j + count].first,
                             &value_got),
                Status::NotFound);
    }
  };

  LaunchNThreads(num_threads, IteratingThroughGlobal);
  LaunchNThreads(num_threads, IteratingThroughThreadLocal);
  LaunchNThreads(num_threads, HGetGlobal);
  LaunchNThreads(num_threads, HGetThreadLocal);

  delete engine;

  // reopen and restore engine
  configs.max_write_threads = 1;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  LaunchNThreads(num_threads, IteratingThroughGlobal);
  LaunchNThreads(num_threads, IteratingThroughThreadLocal);
  LaunchNThreads(num_threads, HGetGlobal);
  LaunchNThreads(num_threads, HGetThreadLocal);

  delete engine;
}

TEST_F(EngineBasicTest, TestLocalQueue) {
  int num_threads = 16;
  int count = 100;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys

  std::vector<std::vector<std::string>> local_values(num_threads);
  std::vector<std::string> local_collection_names(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    local_collection_names[i] = "local_uncoll_t" + std::to_string(i);
    for (size_t j = 0; j < count * 3; j++) {
      local_values[i].push_back(GetRandomString(1024));
    }
  }

  auto LPushRPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->LPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  auto RPushLPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->RPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  LaunchNThreads(num_threads, LPushRPop);
  LaunchNThreads(num_threads, RPushLPop);

  delete engine;
}

TEST_F(EngineBasicTest, TestQueueRestoration) {
  int num_threads = 16;
  int count = 100;
  configs.max_write_threads = num_threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  // insert and delete some keys, then re-insert some deleted keys

  std::vector<std::vector<std::string>> local_values(num_threads);
  std::vector<std::string> local_collection_names(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    local_collection_names[i] = "local_uncoll_t" + std::to_string(i);
    for (size_t j = 0; j < count; j++) {
      local_values[i].push_back(GetRandomString(1024));
    }
  }

  auto LPush = [&](uint32_t tid) {
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->LPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
  };

  auto RPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->RPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  auto RPush = [&](uint32_t tid) {
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(
          engine->RPush(local_collection_names[tid], local_values[tid][j]),
          Status::Ok);
    }
  };

  auto LPop = [&](uint32_t tid) {
    std::string value_got;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
                Status::Ok);
      ASSERT_EQ(local_values[tid][j], value_got);
    }
    ASSERT_EQ(engine->LPop(local_collection_names[tid], &value_got),
              Status::NotFound);
  };

  LaunchNThreads(num_threads, LPush);
  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  LaunchNThreads(num_threads, RPop);

  LaunchNThreads(num_threads, RPush);
  delete engine;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);
  LaunchNThreads(num_threads, LPop);

  delete engine;
}

TEST_F(EngineBasicTest, TestStringHotspot) {
  int n_thread_reading = 16;
  int n_thread_writing = 16;
  configs.max_write_threads = n_thread_writing;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  int count = 100000;
  std::string key{"SuperHotspot"};
  std::string val1(1024, 'a');
  std::string val2(1023, 'b');

  ASSERT_EQ(engine->Set(key, val1), Status::Ok);
  engine->ReleaseWriteThread();

  auto EvenWriteOddRead = [&](uint32_t id) {
    for (size_t i = 0; i < count; i++) {
      if (id % 2 == 0) {
        // Even Write
        if (id % 4 == 0) {
          ASSERT_EQ(engine->Set(key, val1), Status::Ok);
        } else {
          ASSERT_EQ(engine->Set(key, val2), Status::Ok);
        }
      } else {
        // Odd Read
        std::string got_val;
        ASSERT_EQ(engine->Get(key, &got_val), Status::Ok);
        bool match = false;
        match = match || (got_val == val1);
        match = match || (got_val == val2);
        if (!match) {
          std::string msg;
          msg.append("Wrong value!\n");
          msg.append("The value should be 1024 of a's or 1023 of b's.\n");
          msg.append("Actual result is:\n");
          msg.append(got_val);
          msg.append("\n");
          msg.append("Length: ");
          msg.append(std::to_string(got_val.size()));
          msg.append("\n");
          GlobalLogger.Error(msg.data());
        }
        ASSERT_TRUE(match);
      }
    }
  };

  LaunchNThreads(n_thread_reading + n_thread_writing, EvenWriteOddRead);
  delete engine;
}

TEST_F(EngineBasicTest, TestSortedHotspot) {
  int n_thread_reading = 16;
  int n_thread_writing = 16;
  configs.max_write_threads = n_thread_writing;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  int count = 100000;
  std::string collection_name{"collection"};
  std::vector<std::string> keys{"SuperHotSpot0", "SuperHotSpot2",
                                "SuperHotSpot1"};
  std::string val1(1024, 'a');
  std::string val2(1024, 'b');
  Collection *collection_ptr;
  ASSERT_EQ(engine->CreateSortedCollection(collection_name, &collection_ptr),
            Status::Ok);

  for (const std::string &key : keys) {
    ASSERT_EQ(engine->SSet(collection_name, key, val1), Status::Ok);
    engine->ReleaseWriteThread();

    auto EvenWriteOddRead = [&](uint32_t id) {
      for (size_t i = 0; i < count; i++) {
        if (id % 2 == 0) {
          // Even Write
          if (id % 4 == 0) {
            ASSERT_EQ(engine->SSet(collection_name, key, val1), Status::Ok);
          } else {
            ASSERT_EQ(engine->SSet(collection_name, key, val2), Status::Ok);
          }
        } else {
          // Odd Read
          std::string got_val;
          ASSERT_EQ(engine->SGet(collection_name, key, &got_val), Status::Ok);
          bool match = false;
          match = match || (got_val == val1);
          match = match || (got_val == val2);
          if (!match) {
            std::string msg;
            msg.append("Wrong value!\n");
            msg.append("The value should be 1024 of a's or 1023 of b's.\n");
            msg.append("Actual result is:\n");
            msg.append(got_val);
            msg.append("\n");
            msg.append("Length: ");
            msg.append(std::to_string(got_val.size()));
            msg.append("\n");
            GlobalLogger.Error(msg.data());
          }
          ASSERT_TRUE(match);
        }
      }
    };

    LaunchNThreads(n_thread_reading + n_thread_writing, EvenWriteOddRead);
  }
  delete engine;
}

TEST_F(EngineBasicTest, TestSortedCustomCompareFunction) {
  using kvpair = std::pair<std::string, std::string>;
  int threads = 16;
  configs.max_write_threads = threads;
  ASSERT_EQ(Engine::Open(db_path.c_str(), &engine, configs, stdout),
            Status::Ok);

  std::vector<std::string> collections{"collection0", "collection1",
                                       "collection2"};

  auto val_cmp0 = [](const pmem::obj::string_view &a,
                     const pmem::obj::string_view &b) -> int {
    double scorea = std::stod(a.data());
    double scoreb = std::stod(b.data());
    if (scorea == scoreb)
      return 0;
    else if (scorea < scoreb)
      return 1;
    else
      return -1;
  };

  auto val_cmp1 = [](const pmem::obj::string_view &a,
                     const pmem::obj::string_view &b) -> int {
    double scorea = std::stod(a.data());
    double scoreb = std::stod(b.data());
    if (scorea == scoreb)
      return 0;
    else if (scorea > scoreb)
      return 1;
    else
      return -1;
  };

  int count = 10;
  std::vector<kvpair> key_values(count);
  std::map<std::string, std::string> dedup_kvs;
  std::generate(key_values.begin(), key_values.end(), [&]() {
    const char k = rand() % (90 - 65 + 1) + 65;
    std::string v = std::to_string(rand() % 100);
    dedup_kvs[std::string(1, k)] = v;
    return std::make_pair(std::string(1, k), v);
  });

  // registed compare function
  engine->SetCompareFunc("collection0_cmp", val_cmp0);
  engine->SetCompareFunc("collection1_cmp", val_cmp1);
  for (size_t i = 0; i < collections.size(); ++i) {
    Collection *collection_ptr;
    Status s;
    if (i < 2) {
      std::string comp_name = "collection" + std::to_string(i) + "_cmp";
      s = engine->CreateSortedCollection(collections[i], &collection_ptr,
                                         comp_name, SortedBy::VALUE);
    } else {
      s = engine->CreateSortedCollection(collections[i], &collection_ptr);
    }
    ASSERT_EQ(s, Status::Ok);
  }
  for (size_t i = 0; i < collections.size(); ++i) {
    auto Write = [&](uint32_t id) {
      for (size_t j = 0; j < count; j++) {
        ASSERT_EQ(engine->SSet(collections[i], key_values[j].first,
                               key_values[j].second),
                  Status::Ok);
      }
    };
    LaunchNThreads(threads, Write);
  }

  for (size_t i = 0; i < collections.size(); ++i) {
    std::vector<kvpair> expected_res(dedup_kvs.begin(), dedup_kvs.end());
    if (i == 0) {
      std::sort(expected_res.begin(), expected_res.end(),
                [&](const kvpair &a, const kvpair &b) -> bool {
                  int cmp = val_cmp0(a.second, b.second);
                  if (cmp == 0)
                    return a.first < b.first;
                  return cmp > 0 ? false : true;
                });

    } else if (i == 1) {
      std::sort(expected_res.begin(), expected_res.end(),
                [&](const kvpair &a, const kvpair &b) -> bool {
                  int cmp = val_cmp1(a.second, b.second);
                  if (cmp == 0)
                    return a.first < b.first;
                  return cmp > 0 ? false : true;
                });
    }
    auto iter = engine->NewSortedIterator(collections[i]);
    ASSERT_TRUE(iter != nullptr);
    iter->SeekToFirst();
    int cnt = 0;
    while (iter->Valid()) {
      std::string key = iter->Key();
      std::string val = iter->Value();
      ASSERT_EQ(key, expected_res[cnt].first);
      ASSERT_EQ(val, expected_res[cnt].second);
      iter->Next();
      cnt++;
    }
  }
  ASSERT_EQ(engine->SDelete("collection0", "a"), Status::Ok);
  delete engine;
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
