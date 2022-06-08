/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include "kv_engine.hpp"
#include "thread_pool.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

template <typename T>
T* KVEngine::removeOutDatedVersion(T* record) {
  static_assert(
      std::is_same<T, StringRecord>::value || std::is_same<T, DLRecord>::value,
      "Invalid record type, should be StringRecord or DLRecord.");
  auto old_record = record;
  while (old_record && old_record->entry.meta.timestamp >
                           version_controller_.OldestSnapshotTS()) {
    old_record =
        static_cast<T*>(pmem_allocator_->offset2addr(old_record->old_version));
  }

  // the snapshot should access the old record, so we need to purge and free the
  // older version of the old record
  if (old_record && old_record->old_version != kNullPMemOffset) {
    auto old_offset = old_record->old_version;
    old_record->PersistOldVersion(kNullPMemOffset);
    return static_cast<T*>(pmem_allocator_->offset2addr(old_offset));
  }
  return nullptr;
}

template <typename T>
T* KVEngine::removeListOutDatedVersion(T* list) {
  static_assert(
      std::is_same<T, List>::value || std::is_same<T, HashList>::value,
      "Invalid collection type, should be list or hashlist.");
  T* old_list = list;
  while (old_list &&
         old_list->GetTimeStamp() > version_controller_.OldestSnapshotTS()) {
    old_list = old_list->OldVersion();
  }

  // the snapshot should access the old record, so we need to purge and free the
  // older version of the old record
  if (old_list && old_list->OldVersion()) {
    auto older_list = old_list->OldVersion();
    old_list->RemoveOldVersion();
    return older_list;
  }
  return nullptr;
}

void KVEngine::purgeAndFreeStringRecords(
    const std::vector<StringRecord*>& old_records) {
  std::vector<SpaceEntry> entries;
  for (auto old_record : old_records) {
    while (old_record) {
      switch (old_record->GetRecordType()) {
        case StringDataRecord:
          old_record->entry.Destroy();
          entries.emplace_back(pmem_allocator_->addr2offset(old_record),
                               old_record->entry.header.record_size);
          break;
        case StringDeleteRecord:
          entries.emplace_back(pmem_allocator_->addr2offset(old_record),
                               old_record->entry.header.record_size);
          break;
        default:
          std::abort();
      }
      old_record = static_cast<StringRecord*>(
          pmem_allocator_->offset2addr(old_record->old_version));
    }
  }
  pmem_allocator_->BatchFree(entries);
}

void KVEngine::purgeAndFreeDLRecords(
    const std::vector<DLRecord*>& old_records) {
  std::vector<SpaceEntry> entries;
  std::vector<CollectionIDType> outdated_skiplists;
  for (auto pmem_record : old_records) {
    while (pmem_record) {
      switch (pmem_record->GetRecordType()) {
        case RecordType::SortedElem:
        case RecordType::SortedHeader:
        case RecordType::SortedElemDelete: {
          pmem_record->entry.Destroy();
          entries.emplace_back(pmem_allocator_->addr2offset(pmem_record),
                               pmem_record->entry.header.record_size);
          pmem_record = static_cast<DLRecord*>(
              pmem_allocator_->offset2addr(pmem_record->old_version));
          break;
        }
        case RecordType::SortedHeaderDelete: {
          auto skiplist_id = Skiplist::SkiplistID(pmem_record);
          pmem_record = static_cast<DLRecord*>(
              pmem_allocator_->offset2addr(pmem_record->old_version));
          // For the skiplist header, we should disconnect the old version list
          // of sorted header delete record. In order that `DestroyAll` function
          // could easily deal with destroying a sorted collection, instead of
          // may recusively destroy sorted collection, example case:
          // sortedHeaderDelete->sortedHeader->sortedHeaderDelete.
          skiplists_[skiplist_id]->HeaderRecord()->PersistOldVersion(
              kNullPMemOffset);
          skiplists_[skiplist_id]->DestroyAll();
          removeSkiplist(skiplist_id);
          break;
        }
        default:
          std::abort();
      }
    }
  }
  pmem_allocator_->BatchFree(entries);
}

void KVEngine::cleanNoHashIndexedSkiplist(
    Skiplist* skiplist, std::vector<DLRecord*>& purge_dl_records) {
  auto header = skiplist->HeaderRecord();
  auto cur_node = skiplist->HeaderNode();
  auto cur_record = header;
  do {
    auto ul = hash_table_->AcquireLock(cur_record->Key());
    // iter old version list
    auto old_record = removeOutDatedVersion<DLRecord>(cur_record);
    if (old_record) {
      purge_dl_records.emplace_back(old_record);
    }

    SkiplistNode* dram_node = nullptr;
    // check record has dram skiplist node and update skiplist node;
    if (cur_node && cur_node->record == cur_record) {
      kvdk_assert(
          equal_string_view(cur_node->record->Key(), cur_record->Key()),
          "the record of dram skiplist node should be equal the record");
      dram_node = cur_node;
      cur_node = cur_node->Next(1).RawPointer();
    }
    switch (cur_record->GetRecordType()) {
      case SortedElemDelete: {
        if (cur_record->GetTimestamp() <
            version_controller_.OldestSnapshotTS()) {
          TEST_SYNC_POINT(
              "KVEngine::BackgroundCleaner::IterSkiplist::UnlinkDeleteRecord");
          /* Notice: a user thread firstly update this key, its old version
           * record is delete record(cur_record). So the cur_record is not in
           * this skiplist, `Remove` function returns false. Nothing to do for
           * this cur_record which will be purged and freed in the next
           * iteration.
           */
          if (Skiplist::Remove(cur_record, dram_node, pmem_allocator_.get(),
                               skiplist_locks_.get())) {
            purge_dl_records.emplace_back(cur_record);
          }
        }
        break;
        case SortedHeader:
        case SortedElem:
          break;
        default:
          std::abort();
      }
    }
    cur_record =
        static_cast<DLRecord*>(pmem_allocator_->offset2addr(cur_record->next));
  } while (cur_record != header);
}

double KVEngine::CleanOutDated(size_t start_slot_idx, size_t end_slot_idx) {
  constexpr uint64_t kMaxCachedOldRecords = 1024;
  size_t total_num = 0;
  size_t need_purge_num = 0;
  size_t slot_num = 0;
  version_controller_.UpdatedOldestSnapshot();

  PendingCleanOutDatedRecords pending_clean_records;
  std::vector<StringRecord*> purge_string_records;
  std::vector<DLRecord*> purge_dl_records;

  // Iterate hash table
  auto hashtable_iter = hash_table_->GetIterator(start_slot_idx, end_slot_idx);
  while (hashtable_iter.Valid()) {
    /* Update min snapshot timestamp per slot segment to avoid snapshot
     * lock conflict.
     */
    if (slot_num++ % kSlotSegment == 0) {
      version_controller_.UpdatedOldestSnapshot();
    }
    auto min_snapshot_ts = version_controller_.OldestSnapshotTS();
    auto now = TimeUtils::millisecond_time();

    std::vector<Skiplist*> no_index_skiplists;

    {  // Slot lock section
      auto slot_lock(hashtable_iter.AcquireSlotLock());
      auto slot_iter = hashtable_iter.Slot();
      while (slot_iter.Valid()) {
        if (!slot_iter->Empty()) {
          switch (slot_iter->GetIndexType()) {
            case PointerType::StringRecord: {
              total_num++;
              auto string_record = slot_iter->GetIndex().string_record;
              auto old_record =
                  removeOutDatedVersion<StringRecord>(string_record);
              if (old_record) {
                purge_string_records.emplace_back(old_record);
                need_purge_num++;
              }
              if ((string_record->GetRecordType() ==
                       RecordType::StringDeleteRecord ||
                   string_record->GetExpireTime() <= now) &&
                  string_record->GetTimestamp() < min_snapshot_ts) {
                hash_table_->Erase(&(*slot_iter));
                purge_string_records.emplace_back(string_record);
                need_purge_num++;
              }
              break;
            }
            case PointerType::SkiplistNode: {
              total_num++;
              auto dl_record = slot_iter->GetIndex().skiplist_node->record;
              auto old_record = removeOutDatedVersion<DLRecord>(dl_record);
              if (old_record) {
                purge_dl_records.emplace_back(old_record);
                need_purge_num++;
              }
              if (slot_iter->GetRecordType() == RecordType::SortedElemDelete &&
                  dl_record->entry.meta.timestamp < min_snapshot_ts) {
                hash_table_->Erase(&(*slot_iter));
                Skiplist::Remove(static_cast<DLRecord*>(dl_record),
                                 slot_iter->GetIndex().skiplist_node,
                                 pmem_allocator_.get(), skiplist_locks_.get());
                purge_dl_records.emplace_back(dl_record);
                need_purge_num++;
              }
              break;
            }
            case PointerType::DLRecord: {
              total_num++;
              auto dl_record = slot_iter->GetIndex().dl_record;
              auto old_record = removeOutDatedVersion<DLRecord>(dl_record);
              if (old_record) {
                purge_dl_records.emplace_back(old_record);
                need_purge_num++;
              }
              if (slot_iter->GetRecordType() == RecordType::SortedElemDelete &&
                  dl_record->entry.meta.timestamp < min_snapshot_ts) {
                Skiplist::Remove(static_cast<DLRecord*>(dl_record), nullptr,
                                 pmem_allocator_.get(), skiplist_locks_.get());
                hash_table_->Erase(&(*slot_iter));
                purge_dl_records.emplace_back(dl_record);
                need_purge_num++;
              }
              break;
            }
            case PointerType::Skiplist: {
              Skiplist* skiplist = slot_iter->GetIndex().skiplist;
              total_num += skiplist->Size();
              auto head_record = skiplist->HeaderRecord();
              auto old_record = removeOutDatedVersion<DLRecord>(head_record);
              if (old_record) {
                purge_dl_records.emplace_back(old_record);
                need_purge_num++;
              }
              if ((slot_iter->GetRecordType() ==
                       RecordType::SortedHeaderDelete ||
                   head_record->GetExpireTime() <= now) &&
                  head_record->entry.meta.timestamp < min_snapshot_ts) {
                hash_table_->Erase(&(*slot_iter));
                pending_clean_records.outdated_skip_lists.emplace_back(
                    std::make_pair(version_controller_.GetCurrentTimestamp(),
                                   skiplist));
                need_purge_num += skiplist->Size();
              } else if (!skiplist->IndexWithHashtable()) {
                no_index_skiplists.emplace_back(skiplist);
              }
              break;
            }
            case PointerType::List: {
              List* list = slot_iter->GetIndex().list;
              total_num += list->Size();
              auto current_ts = version_controller_.GetCurrentTimestamp();
              auto old_list = removeListOutDatedVersion(list);
              if (old_list) {
                pending_clean_records.outdated_lists.emplace_back(
                    std::make_pair(current_ts, old_list));
              }
              if (list->GetExpireTime() <= now &&
                  list->GetTimeStamp() < min_snapshot_ts) {
                hash_table_->Erase(&(*slot_iter));
                pending_clean_records.outdated_lists.emplace_back(
                    std::make_pair(current_ts, list));
                need_purge_num += list->Size();
                std::unique_lock<std::mutex> guard{lists_mu_};
                lists_.erase(list);
              }
              break;
            }
            case PointerType::HashList: {
              HashList* hlist = slot_iter->GetIndex().hlist;
              total_num += hlist->Size();
              auto current_ts = version_controller_.GetCurrentTimestamp();
              auto old_list = removeListOutDatedVersion(hlist);
              if (old_list) {
                pending_clean_records.outdated_hash_lists.emplace_back(
                    std::make_pair(current_ts, old_list));
              }
              if (hlist->GetExpireTime() <= now &&
                  hlist->GetTimeStamp() < min_snapshot_ts) {
                pending_clean_records.outdated_hash_lists.emplace_back(
                    std::make_pair(version_controller_.GetCurrentTimestamp(),
                                   hlist));
                hash_table_->Erase(&(*slot_iter));
                need_purge_num += hlist->Size();
                std::unique_lock<std::mutex> guard{hlists_mu_};
                hash_lists_.erase(hlist);
              }
              break;
            }
            default:
              break;
          }
        }
        slot_iter++;
      }
      hashtable_iter.Next();
    }  // Finish a slot.

    auto new_ts = version_controller_.GetCurrentTimestamp();

    if (purge_string_records.size() > kMaxCachedOldRecords) {
      pending_clean_records.pending_purge_strings.emplace_back(
          PendingPurgeStrRecords{purge_string_records, new_ts});
      purge_string_records.clear();
    }

    if (purge_dl_records.size() > kMaxCachedOldRecords) {
      pending_clean_records.pending_purge_dls.emplace_back(
          PendingPurgeDLRecords{purge_dl_records, new_ts});
      purge_dl_records.clear();
    }

    {  // purge and free pending string records
      while (!pending_clean_records.pending_purge_strings.empty()) {
        auto& pending_strings =
            pending_clean_records.pending_purge_strings.front();
        if (pending_strings.release_time <
            version_controller_.OldestSnapshotTS()) {
          purgeAndFreeStringRecords(pending_strings.records);
          pending_clean_records.pending_purge_strings.pop_front();
        } else {
          break;
        }
      }
    }

    {  // purge and free pending old dl records
      while (!pending_clean_records.pending_purge_dls.empty()) {
        auto& pending_dls = pending_clean_records.pending_purge_dls.front();
        if (pending_dls.release_time < version_controller_.OldestSnapshotTS()) {
          purgeAndFreeDLRecords(pending_dls.records);
          pending_clean_records.pending_purge_dls.pop_front();
        } else {
          break;
        }
      }
    }

    {  // Destroy skiplist
      while (!pending_clean_records.outdated_skip_lists.empty()) {
        auto& ts_skiplist = pending_clean_records.outdated_skip_lists.front();
        if (ts_skiplist.first < min_snapshot_ts) {
          ts_skiplist.second->DestroyAll();
          removeSkiplist(ts_skiplist.second->ID());
          pending_clean_records.outdated_skip_lists.pop_front();
        } else {
          break;
        }
      }
    }

    {  // Deal with skiplist with hash index: remove outdated records.
      if (!no_index_skiplists.empty()) {
        for (auto& skiplist : no_index_skiplists) {
          cleanNoHashIndexedSkiplist(skiplist, purge_dl_records);
        }
      }
    }

    {  // Destroy list
      while (!pending_clean_records.outdated_lists.empty()) {
        auto& ts_list = pending_clean_records.outdated_lists.front();
        if (ts_list.first < min_snapshot_ts) {
          listDestroy(ts_list.second.release());
          pending_clean_records.outdated_lists.pop_front();
        } else {
          break;
        }
      }
    }

    {  // Destroy hash
      while (!pending_clean_records.outdated_hash_lists.empty()) {
        auto& ts_hlist = pending_clean_records.outdated_hash_lists.front();
        if (ts_hlist.first < min_snapshot_ts) {
          hashListDestroy(ts_hlist.second.release());
          pending_clean_records.outdated_hash_lists.pop_front();
        } else {
          break;
        }
      }
    }
  }  // Finsh iterating hash table

  // Push the remaining need purged records to global pool.
  auto new_ts = version_controller_.GetCurrentTimestamp();
  if (!purge_string_records.empty()) {
    pending_clean_records.pending_purge_strings.emplace_back(
        PendingPurgeStrRecords{purge_string_records, new_ts});
    purge_string_records.clear();
  }

  if (!purge_dl_records.empty()) {
    pending_clean_records.pending_purge_dls.emplace_back(
        PendingPurgeDLRecords{purge_dl_records, new_ts});
    pending_clean_records.pending_purge_dls.clear();
  }

  {
    std::unique_lock<SpinMutex> lock(clean_records_mtx_);
    clean_records_set_.emplace_back(std::move(pending_clean_records));
  }
  return total_num == 0 ? 0.0f : (need_purge_num / (double)total_num);
}

void KVEngine::backgroundCleanRecords() {
  constexpr double kWakeUpThreshold = 0.3;
  while (!bg_work_signals_.terminating) {
    for (size_t slot_idx = 0; hash_table_->GetSlotsNum();) {
      // sleep second.
      if (CleanOutDated(slot_idx, slot_idx + kSlotSegment) < kWakeUpThreshold) {
        sleep(1);
        slot_idx += kSlotSegment;
      } else {
      }
    }
  }
}

}  // namespace KVDK_NAMESPACE