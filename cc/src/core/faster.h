// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>

#include "device/null_disk.h"
#include "device/file_system_disk.h"

#include "alloc.h"
#include "checkpoint_locks.h"
#include "checkpoint_state.h"
#include "constants.h"
#include "gc_state.h"
#include "grow_state.h"
#include "guid.h"
#include "hash_table.h"
#include "internal_contexts.h"
#include "key_hash.h"
#include "malloc_fixed_page_size.h"
#include "persistent_memory_malloc.h"
#include "record.h"
#include "recovery_status.h"
#include "state_transitions.h"
#include "status.h"
#include "utility.h"
#include "migration.h"
#include "log_scan.h"
#include "compact.h"
#include "indirection.h"

using namespace std::chrono_literals;

/// The FASTER key-value store, and related classes.

namespace FASTER {
namespace core {

class alignas(Constants::kCacheLineBytes) ThreadContext {
 public:
  ThreadContext()
    : contexts_{}
    , cur_{ 0 } {
  }

  inline const ExecutionContext& cur() const {
    return contexts_[cur_];
  }

  inline ExecutionContext& cur() {
    return contexts_[cur_];
  }

  inline const ExecutionContext& prev() const {
    return contexts_[(cur_ + 1) % 2];
  }

  inline ExecutionContext& prev() {
    return contexts_[(cur_ + 1) % 2];
  }

  inline void swap() {
    cur_ = (cur_ + 1) % 2;
  }

 private:
  ExecutionContext contexts_[2];
  uint8_t cur_;
};
static_assert(sizeof(ThreadContext) == 448, "sizeof(ThreadContext) != 448");

/// The FASTER key-value store.
template <class K, class V, class D>
class FasterKv {
 public:
  typedef FasterKv<K, V, D> faster_t;

  /// Key and value types stored in this key-value store.
  typedef K key_t;
  typedef V value_t;

  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef typename D::log_file_t log_file_t;

  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  /// Contexts that have been deep-copied, for async continuations, and must be
  /// accessed via virtual function calls.
  typedef AsyncPendingReadContext<key_t> async_pending_read_context_t;
  typedef AsyncPendingUpsertContext<key_t> async_pending_upsert_context_t;
  typedef AsyncPendingRmwContext<key_t> async_pending_rmw_context_t;
  typedef AsyncPendingDeleteContext<key_t> async_pending_delete_context_t;

  FasterKv(uint64_t table_size, uint64_t log_size, const std::string& filename,
           uint16_t id=1, const std::string& remote="",
           double log_mutable_fraction = 0.9)
    : min_table_size_{ table_size }
    , disk{ filename, epoch_, id, remote }
    , hlog{ log_size, epoch_, disk, disk.log(), log_mutable_fraction }
    , system_state_{ Action::None, Phase::REST, 1 }
    , num_pending_ios{ 0 }
  {
    if(!Utility::IsPowerOfTwo(table_size)) {
      throw std::invalid_argument{ " Size is not a power of 2" };
    }

    if(table_size > INT32_MAX) {
      throw std::invalid_argument{ " Cannot allocate such a large hash table " };
    }

    resize_info_.version = 0;
    state_[0].Initialize(table_size, disk.log().alignment());
    overflow_buckets_allocator_[0].Initialize(disk.log().alignment(), epoch_);

    InitializeSamplingLocks();

    migrate_.dfsId = id;
  }

  /// No copy constructor.
  FasterKv(const FasterKv& other) = delete;

 public:
  /// Thread-related operations
  Guid StartSession();
  uint64_t ContinueSession(const Guid& guid);
  void StopSession();
  void Refresh();

  /// Store interface
  template <class RC>
  inline Status Read(RC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class UC>
  inline Status Upsert(UC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class MC>
  inline Status Rmw(MC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class DC>
  inline Status Delete(DC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class UC>
  inline Status CondUpsert(UC& ctxt, AsyncCallback cb, uint64_t monotonic);

  inline bool CompletePending(bool wait = false);

  /// Checkpoint/recovery operations.
  bool Checkpoint(void(*index_persistence_callback)(Status result),
                  void(*hybrid_log_persistence_callback)(Status result,
                      uint64_t persistent_serial_num), Guid& token);
  bool CheckpointIndex(void(*index_persistence_callback)(Status result), Guid& token);
  bool CheckpointHybridLog(void(*hybrid_log_persistence_callback)(Status result,
                           uint64_t persistent_serial_num), Guid& token);
  Status Recover(const Guid& index_token, const Guid& hybrid_log_token, uint32_t& version,
                 std::vector<Guid>& session_ids);

  /// Data migration operations.
  bool Migrate(KeyHash start, KeyHash end, bool src,
               ownership_cb_t ownerTx, migration_cb_t dataMovt,
               cntrlpath_cb_t control, iopath_cb_t io,
               const std::string& ip, uint16_t port);
  uint32_t Collect(void* opaque, uint8_t* buffer, uint32_t left);
  uint32_t CollectSamples(uint8_t* buffer, uint32_t size);
  void PushTarget(KeyHash lHash, KeyHash rHash, ownership_cb_t o,
                  migration_cb_t d, cntrlpath_cb_t c, iopath_cb_t io);
  bool Receive(uint8_t* buffer, uint32_t size, KeyHash lHash,
                      KeyHash rHash, ownership_cb_t o, migration_cb_t d,
                      cntrlpath_cb_t c, iopath_cb_t io);
  bool ReceiveSampled(uint8_t* buffer, uint32_t size, KeyHash lHash,
                      KeyHash rHash, ownership_cb_t o, migration_cb_t d,
                      cntrlpath_cb_t c, iopath_cb_t io);
  bool CompleteMigration(KeyHash lHash, KeyHash rHash);

  /// Log compaction entry method.
  bool Compact(uint64_t untilAddress, compaction_cb_t compact, bool pace=true);

  /// Truncating the head of the log.
  bool ShiftBeginAddress(Address address, GcState::truncate_callback_t truncate_callback,
                         GcState::complete_callback_t complete_callback);

  /// Make the hash table larger.
  bool GrowIndex(GrowState::callback_t caller_callback);

  /// HybridLog size.
  inline uint64_t Size() const {
    return hlog.GetTailAddress().control();
  }

  /// Statistics
  inline void DumpDistribution() {
    state_[resize_info_.version].DumpDistribution(
      overflow_buckets_allocator_[resize_info_.version]);
  }

  typedef Record<key_t, value_t> record_t;

 private:
  typedef PendingContext<key_t> pending_context_t;

  template <class C>
  inline OperationStatus InternalRead(C& pending_context);

  template <class C>
  inline OperationStatus InternalUpsert(C& pending_context);

  template <class C>
  inline OperationStatus InternalRmw(C& pending_context, bool retrying);

  inline OperationStatus InternalRetryPendingRmw(async_pending_rmw_context_t& pending_context);

  template<class C>
  inline OperationStatus InternalDelete(C& pending_context);

  OperationStatus InternalContinuePendingRead(ExecutionContext& ctx,
      AsyncIOContext& io_context);
  OperationStatus InternalContinuePendingCondUpsert(ExecutionContext& ctx,
      AsyncIOContext& io_context);
  OperationStatus InternalContinuePendingRmw(ExecutionContext& ctx,
      AsyncIOContext& io_context);

  // Find the hash bucket entry, if any, corresponding to the specified hash.
  inline AtomicHashBucketEntry* FindEntry(KeyHash hash, HashBucketEntry& expected_entry);
  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // create a new entry. The caller can use the "expected_entry" to CAS its desired address into
  // the entry.
  inline AtomicHashBucketEntry* FindOrCreateEntry(KeyHash hash, HashBucketEntry& expected_entry,
      HashBucket*& bucket);
  inline Address TraceBackForKeyMatch(const key_t& key, Address from_address,
                                      Address min_offset) const;
  Address TraceBackForOtherChainStart(uint64_t old_size,  uint64_t new_size, Address from_address,
                                      Address min_address, uint8_t side);

  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // return an unused bucket entry.
  inline AtomicHashBucketEntry* FindTentativeEntry(KeyHash hash, HashBucket* bucket,
      uint8_t version, HashBucketEntry& expected_entry);
  // Looks for an entry that has the same
  inline bool HasConflictingEntry(KeyHash hash, const HashBucket* bucket, uint8_t version,
                                  const AtomicHashBucketEntry* atomic_entry) const;

  inline Address BlockAllocate(uint32_t record_size);

  inline Status HandleOperationStatus(ExecutionContext& ctx,
                                      pending_context_t& pending_context,
                                      OperationStatus internal_status, bool& async);
  inline Status PivotAndRetry(ExecutionContext& ctx, pending_context_t& pending_context,
                              bool& async);
  inline Status RetryLater(ExecutionContext& ctx, pending_context_t& pending_context,
                           bool& async);
 public:
  inline constexpr uint32_t MinIoRequestSize() const;

 private:
  inline Status IssueAsyncIoRequest(ExecutionContext& ctx, pending_context_t& pending_context,
                                    bool& async);

 public:
  void AsyncGetFromDisk(Address address, uint32_t num_records, AsyncIOCallback callback,
                        AsyncIOContext& context);

 private:
  static void AsyncGetFromDiskCallback(IAsyncContext* ctxt, Status result,
                                       size_t bytes_transferred);

  void CompleteIoPendingRequests(ExecutionContext& context);
  void CompleteRetryRequests(ExecutionContext& context);

  void InitializeCheckpointLocks();
  void InitializeSamplingLocks();

  /// Checkpoint/recovery methods.
  void HandleSpecialPhases();
  bool GlobalMoveToNextState(SystemState current_state);

  Status CheckpointFuzzyIndex();
  Status CheckpointFuzzyIndexComplete();
  Status RecoverFuzzyIndex();
  Status RecoverFuzzyIndexComplete(bool wait);

  Status WriteIndexMetadata();
  Status ReadIndexMetadata(const Guid& token);
  Status WriteCprMetadata();
  Status ReadCprMetadata(const Guid& token);
  Status WriteCprContext();
  Status ReadCprContexts(const Guid& token, const Guid* guids);

  Status RecoverHybridLog();
  Status RecoverHybridLogFromSnapshotFile();
  Status RecoverFromPage(Address from_address, Address to_address);
  Status RestoreHybridLog();

  void MarkAllPendingRequests();

  inline void HeavyEnter();
  bool CleanHashTableBuckets();
  void SplitHashTableBuckets();
  void AddHashEntry(HashBucket*& bucket, uint32_t& next_idx, uint8_t version,
                    HashBucketEntry entry);

  uint32_t CollectBucket(HashBucket* bucket, uint64_t entry, uint64_t offset,
                         Address head, uint8_t* buffer, uint32_t left,
                         int* nextEntry, int* nextOffset, uint64_t bucketIdx,
                         MigrationState::MigrationContext* s);
  uint32_t CollectChain(Address curr, uint64_t offset, Address head,
                        uint8_t* buffer, uint32_t left, int* nextOff,
                        uint32_t bucket, MigrationState::MigrationContext* s);
  int CollectIndirection(uint64_t bucket, uint64_t tag, uint8_t* buffer,
                         Address curr, Address head, uint32_t left);
  void InsertIndirectionBucket(uint64_t bucket, uint64_t tag,
                               IndirectionRecord* migrated, Address addr);
  uint32_t InsertIndirection(IndirectionRecord* migrated, Address addr);
  void InsertBatch(uint8_t* buffer, uint32_t size);

  typedef FasterKv<K, V, device::NullDisk> tempkv_t;
  Address LogScanForValidity(Address from, tempkv_t* temp);
  bool ContainsKeyInMemory(key_t key, Address offset);

  /// Access the current (thread-local) execution contexts.
  const ExecutionContext& thread_ctx() const {
    return thread_contexts_[Thread::id()].cur();
  }

  /// Access the current (thread-local) execution contexts.
  ExecutionContext& thread_ctx() {
    return thread_contexts_[Thread::id()].cur();
  }

  /// Access the previous (thread-local) execution contexts.
  ExecutionContext& prev_thread_ctx() {
    return thread_contexts_[Thread::id()].prev();
  }

 private:
  LightEpoch epoch_;

 public:
  disk_t disk;
  hlog_t hlog;

 private:
  static constexpr bool kCopyReadsToTail = false;
  static constexpr uint64_t kGcHashTableChunkSize = 16384;
  static constexpr uint64_t kGrowHashTableChunkSize = 16384;

  bool fold_over_snapshot = true;

  /// Initial size of the table
  uint64_t min_table_size_;

  // Allocator for the hash buckets that don't fit in the hash table.
  MallocFixedPageSize<HashBucket, disk_t> overflow_buckets_allocator_[2];

  // An array of size two, that contains the old and new versions of the hash-table
  InternalHashTable<disk_t> state_[2];

  CheckpointLocks checkpoint_locks_;

  // Bucket locks required for sampling hot records during migration.
  CheckpointLocks sampling_locks_;

  ResizeInfo resize_info_;

  AtomicSystemState system_state_;

  /// Checkpoint/recovery state.
  CheckpointState<file_t> checkpoint_;
  /// Garbage collection state.
  GcState gc_;
  /// Grow (hash table) state.
  GrowState grow_;
  /// Migration state.
  MigrationState migrate_;

 public:
  /// Global count of pending I/Os, used for throttling.
  std::atomic<uint64_t> num_pending_ios;

 private:
  /// Space for two contexts per thread, stored inline.
  ThreadContext thread_contexts_[Thread::kMaxNumThreads];
};

// Implementations.
template <class K, class V, class D>
inline Guid FasterKv<K, V, D>::StartSession() {
  SystemState state = system_state_.load();
  if(state.phase != Phase::REST) {
    throw std::runtime_error{ "Can acquire only in REST phase!" };
  }
  thread_ctx().Initialize(state.phase, state.version, Guid::Create(), 0);
  Refresh();
  return thread_ctx().guid;
}

template <class K, class V, class D>
inline uint64_t FasterKv<K, V, D>::ContinueSession(const Guid& session_id) {
  auto iter = checkpoint_.continue_tokens.find(session_id);
  if(iter == checkpoint_.continue_tokens.end()) {
    throw std::invalid_argument{ "Unknown session ID" };
  }

  SystemState state = system_state_.load();
  if(state.phase != Phase::REST) {
    throw std::runtime_error{ "Can continue only in REST phase!" };
  }
  thread_ctx().Initialize(state.phase, state.version, session_id, iter->second);
  Refresh();
  return iter->second;
}

template <class K, class V, class D>
inline void FasterKv<K, V, D>::Refresh() {
  epoch_.ProtectAndDrain();
  // We check if we are in normal mode
  SystemState new_state = system_state_.load();
  if(thread_ctx().phase == Phase::REST && new_state.phase == Phase::REST) {
    return;
  }
  HandleSpecialPhases();
}

template <class K, class V, class D>
inline void FasterKv<K, V, D>::StopSession() {
  // If this thread is still involved in some activity, wait until it finishes.
  while(thread_ctx().phase != Phase::REST ||
        !thread_ctx().pending_ios.empty() ||
        !thread_ctx().retry_requests.empty()) {
    CompletePending(false);
    std::this_thread::yield();
  }

  assert(thread_ctx().retry_requests.empty());
  assert(thread_ctx().pending_ios.empty());
  assert(thread_ctx().io_responses.empty());

  assert(prev_thread_ctx().retry_requests.empty());
  assert(prev_thread_ctx().pending_ios.empty());
  assert(prev_thread_ctx().io_responses.empty());

  assert(thread_ctx().phase == Phase::REST);

  epoch_.Unprotect();
}

template <class K, class V, class D>
inline AtomicHashBucketEntry* FasterKv<K, V, D>::FindEntry(KeyHash hash,
    HashBucketEntry& expected_entry)
{
  expected_entry = HashBucketEntry::kInvalidEntry;
  // Truncate the hash to get a bucket page_index < state[version].size.
  uint32_t version = resize_info_.version;
  HashBucket* bucket = &state_[version].bucket(hash);
  assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

  while(true) {
    // Search through the bucket looking for our key. Last entry is reserved
    // for the overflow pointer.
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry.unused()) {
        continue;
      }
      if(hash.tag() == entry.tag()) {
        // Found a matching tag. (So, the input hash matches the entry on 14 tag bits +
        // log_2(table size) address bits.)
        if(!entry.tentative()) {
          // If (final key, return immediately)
          expected_entry = entry;
          return &bucket->entries[entry_idx];
        }
      }
    }

    // Go to next bucket in the chain
    HashBucketOverflowEntry entry = bucket->overflow_entry.load();
    if(entry.unused()) {
      // No more buckets in the chain.
      return nullptr;
    }
    bucket = &overflow_buckets_allocator_[version].Get(entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class K, class V, class D>
inline AtomicHashBucketEntry* FasterKv<K, V, D>::FindTentativeEntry(KeyHash hash,
    HashBucket* bucket,
    uint8_t version, HashBucketEntry& expected_entry) {
  expected_entry = HashBucketEntry::kInvalidEntry;
  AtomicHashBucketEntry* atomic_entry = nullptr;
  // Try to find a slot that contains the right tag or that's free.
  while(true) {
    // Search through the bucket looking for our key. Last entry is reserved
    // for the overflow pointer.
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry.unused()) {
        if(!atomic_entry) {
          // Found a free slot; keep track of it, and continue looking for a match.
          atomic_entry = &bucket->entries[entry_idx];
        }
        continue;
      }
      if(hash.tag() == entry.tag() && !entry.tentative()) {
        // Found a match. (So, the input hash matches the entry on 14 tag bits +
        // log_2(table size) address bits.) Return it to caller.
        expected_entry = entry;
        return &bucket->entries[entry_idx];
      }
    }
    // Go to next bucket in the chain
    HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
    if(overflow_entry.unused()) {
      // No more buckets in the chain.
      if(atomic_entry) {
        // We found a free slot earlier (possibly inside an earlier bucket).
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return atomic_entry;
      }
      // We didn't find any free slots, so allocate new bucket.
      FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
      bool success;
      do {
        HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };
        success = bucket->overflow_entry.compare_exchange_strong(overflow_entry,
                  new_bucket_entry);
      } while(!success && overflow_entry.unused());
      if(!success) {
        // Install failed, undo allocation; use the winner's entry
        overflow_buckets_allocator_[version].FreeAtEpoch(new_bucket_addr, 0);
      } else {
        // Install succeeded; we have a new bucket on the chain. Return its first slot.
        bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return &bucket->entries[0];
      }
    }
    // Go to the next bucket.
    bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::HasConflictingEntry(KeyHash hash, const HashBucket* bucket, uint8_t version,
    const AtomicHashBucketEntry* atomic_entry) const {
  uint16_t tag = atomic_entry->load().tag();
  while(true) {
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry != HashBucketEntry::kInvalidEntry &&
          entry.tag() == tag &&
          atomic_entry != &bucket->entries[entry_idx]) {
        // Found a conflict.
        return true;
      }
    }
    // Go to next bucket in the chain
    HashBucketOverflowEntry entry = bucket->overflow_entry.load();
    if(entry.unused()) {
      // Reached the end of the bucket chain; no conflicts found.
      return false;
    }
    // Go to the next bucket.
    bucket = &overflow_buckets_allocator_[version].Get(entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
}

template <class K, class V, class D>
inline AtomicHashBucketEntry* FasterKv<K, V, D>::FindOrCreateEntry(KeyHash hash,
    HashBucketEntry& expected_entry, HashBucket*& bucket) {
  bucket = nullptr;
  // Truncate the hash to get a bucket page_index < state[version].size.
  uint32_t version = resize_info_.version;
  assert(version <= 1);

  while(true) {
    bucket = &state_[version].bucket(hash);
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

    AtomicHashBucketEntry* atomic_entry = FindTentativeEntry(hash, bucket, version,
                                          expected_entry);
    if(expected_entry != HashBucketEntry::kInvalidEntry) {
      // Found an existing hash bucket entry; nothing further to check.
      return atomic_entry;
    }
    // We have a free slot.
    assert(atomic_entry);
    assert(expected_entry == HashBucketEntry::kInvalidEntry);
    // Try to install tentative tag in free slot.
    HashBucketEntry entry{ Address::kInvalidAddress, hash.tag(), true };
    if(atomic_entry->compare_exchange_strong(expected_entry, entry)) {
      // See if some other thread is also trying to install this tag.
      if(HasConflictingEntry(hash, bucket, version, atomic_entry)) {
        // Back off and try again.
        atomic_entry->store(HashBucketEntry::kInvalidEntry);
      } else {
        // No other thread was trying to install this tag, so we can clear our entry's "tentative"
        // bit.
        expected_entry = HashBucketEntry{ Address::kInvalidAddress, hash.tag(), false };
        atomic_entry->store(expected_entry);
        return atomic_entry;
      }
    }
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class K, class V, class D>
template <class RC>
inline Status FasterKv<K, V, D>::Read(RC& context, AsyncCallback callback,
                                      uint64_t monotonic_serial_num) {
  typedef RC read_context_t;
  typedef PendingReadContext<RC> pending_read_context_t;
  static_assert(std::is_base_of<value_t, typename read_context_t::value_t>::value,
                "value_t is not a base class of read_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename read_context_t::value_t),
                "alignof(value_t) != alignof(typename read_context_t::value_t)");

  pending_read_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalRead(pending_context);
  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if(internal_status == OperationStatus::NOT_FOUND) {
    status = Status::NotFound;
  } else if(internal_status == OperationStatus::NOT_READY) {
    status = Status::NotReady;
  } else {
    assert(internal_status == OperationStatus::RECORD_ON_DISK);
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

template <class K, class V, class D>
template <class UC>
inline Status FasterKv<K, V, D>::Upsert(UC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
  typedef UC upsert_context_t;
  typedef PendingUpsertContext<UC> pending_upsert_context_t;
  static_assert(std::is_base_of<value_t, typename upsert_context_t::value_t>::value,
                "value_t is not a base class of upsert_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename upsert_context_t::value_t),
                "alignof(value_t) != alignof(typename upsert_context_t::value_t)");

  pending_upsert_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalUpsert(pending_context);
  Status status;

  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if(internal_status == OperationStatus::NOT_READY) {
    status = Status::NotReady;
  } else {
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

/// Upserts a key-value pair if an indirection record for it does not exist.
template <class K, class V, class D>
template <class UC>
inline Status FasterKv<K, V, D>::CondUpsert(UC& ctxt, AsyncCallback cb,
                                            uint64_t monotonic)
{
  // First, try to read the key from the in-memory log.
  PendingCondUpsertContext<UC> pending(ctxt, cb);
  OperationStatus internal = InternalRead(pending);

  // The read succeeded. This means that there is a new version of this key
  // within this instance of FASTER. No need to perform an Upsert. Just
  // return to the caller indicating that the operation succeeded.
  //
  // We return success if the key was not found too, meaning that we
  // encountered a tombstone during the in-memory traversal.
  if (internal == OperationStatus::SUCCESS ||
      internal == OperationStatus::NOT_FOUND)
  {
    thread_ctx().serial_num = monotonic;
    return Status::Ok;
  }

  // The key was not found but it belongs to an incoming hash range. In this
  // case this operation is being issued as part of the migration so let it
  // go through.
  if (internal == OperationStatus::NOT_READY) {
    return Upsert(ctxt, cb, monotonic);
  }

  // If we need to go to disk, then check if we were directed there via an
  // indirection record. If we weren't then go through with the IO.
  bool indirection = (pending.address.control() >> Address::kAddressBits) > 0;
  if (internal == OperationStatus::RECORD_ON_DISK && !indirection) {
    bool async;
    thread_ctx().serial_num = monotonic;
    return HandleOperationStatus(thread_ctx(), pending, internal, async);
  }

  // We either did not find the key, or we hit an indirection record. In
  // either case, it is safe to go through with the upsert.
  return Upsert(ctxt, cb, monotonic);
}

template <class K, class V, class D>
template <class MC>
inline Status FasterKv<K, V, D>::Rmw(MC& context, AsyncCallback callback,
                                     uint64_t monotonic_serial_num) {
  typedef MC rmw_context_t;
  typedef PendingRmwContext<MC> pending_rmw_context_t;
  static_assert(std::is_base_of<value_t, typename rmw_context_t::value_t>::value,
                "value_t is not a base class of rmw_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename rmw_context_t::value_t),
                "alignof(value_t) != alignof(typename rmw_context_t::value_t)");

  pending_rmw_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalRmw(pending_context, false);
  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if(internal_status == OperationStatus::NOT_READY) {
    status = Status::NotReady;
  } else {
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

template <class K, class V, class D>
template <class DC>
inline Status FasterKv<K, V, D>::Delete(DC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
  typedef DC delete_context_t;
  typedef PendingDeleteContext<DC> pending_delete_context_t;
  static_assert(std::is_base_of<value_t, typename delete_context_t::value_t>::value,
                "value_t is not a base class of delete_context_t::value_t");
  static_assert(alignof(value_t) == alignof(typename delete_context_t::value_t),
                "alignof(value_t) != alignof(typename delete_context_t::value_t)");

  pending_delete_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalDelete(pending_context);
  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if(internal_status == OperationStatus::NOT_FOUND) {
    status = Status::NotFound;
  } else {
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  return status;
}

template <class K, class V, class D>
inline bool FasterKv<K, V, D>::CompletePending(bool wait) {
  do {
    disk.TryComplete();

    bool done = true;
    if(thread_ctx().phase != Phase::WAIT_PENDING && thread_ctx().phase != Phase::IN_PROGRESS) {
      CompleteIoPendingRequests(thread_ctx());
    }
    Refresh();
    CompleteRetryRequests(thread_ctx());

    done = (thread_ctx().pending_ios.empty() && thread_ctx().retry_requests.empty());

    if(thread_ctx().phase != Phase::REST) {
      CompleteIoPendingRequests(prev_thread_ctx());
      Refresh();
      CompleteRetryRequests(prev_thread_ctx());
      done = false;
    }
    if(done) {
      return true;
    }
  } while(wait);
  return false;
}

template <class K, class V, class D>
inline void FasterKv<K, V, D>::CompleteIoPendingRequests(ExecutionContext& context) {
  AsyncIOContext* ctxt;
  // Clear this thread's I/O response queue. (Does not clear I/Os issued by this thread that have
  // not yet completed.)
  while(context.io_responses.try_pop(ctxt)) {
    CallbackContext<AsyncIOContext> io_context{ ctxt };
    CallbackContext<pending_context_t> pending_context{ io_context->caller_context };
    // This I/O is no longer pending, since we popped its response off the queue.
    auto pending_io = context.pending_ios.find(io_context->io_id);
    assert(pending_io != context.pending_ios.end());
    context.pending_ios.erase(pending_io);

    // Issue the continue command
    OperationStatus internal_status;
    if(pending_context->type == OperationType::Read) {
      internal_status = InternalContinuePendingRead(context, *io_context.get());
    } else if(pending_context->type == OperationType::CondUpsert) {
      pending_context->type = OperationType::Upsert;
      internal_status = InternalContinuePendingCondUpsert(context,
                                                          *io_context.get());
    } else {
      assert(pending_context->type == OperationType::RMW);
      internal_status = InternalContinuePendingRmw(context, *io_context.get());
    }
    Status result;
    if(internal_status == OperationStatus::SUCCESS) {
      result = Status::Ok;
    } else if(internal_status == OperationStatus::NOT_FOUND) {
      result = Status::NotFound;
    } else if(internal_status == OperationStatus::NOT_READY) {
      result = Status::NotReady;
    } else {
      result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                     pending_context.async);
    }
    if(!pending_context.async) {
      pending_context->caller_callback(pending_context->caller_context, result);
    }
  }
}

template <class K, class V, class D>
inline void FasterKv<K, V, D>::CompleteRetryRequests(ExecutionContext& context) {
  // If we can't complete a request, it will be pushed back onto the deque. Retry each request
  // only once.
  size_t size = context.retry_requests.size();
  for(size_t idx = 0; idx < size; ++idx) {
    CallbackContext<pending_context_t> pending_context{ context.retry_requests.front() };
    context.retry_requests.pop_front();
    // Issue retry command
    OperationStatus internal_status;
    switch(pending_context->type) {
    case OperationType::RMW:
      internal_status = InternalRetryPendingRmw(
                          *static_cast<async_pending_rmw_context_t*>(pending_context.get()));
      break;
    case OperationType::Upsert:
      internal_status = InternalUpsert(
                          *static_cast<async_pending_upsert_context_t*>(pending_context.get()));
      break;
    default:
      assert(false);
      throw std::runtime_error{ "Cannot happen!" };
    }
    // Handle operation status
    Status result;
    if(internal_status == OperationStatus::SUCCESS) {
      result = Status::Ok;
    } else if(internal_status == OperationStatus::NOT_READY) {
      result = Status::NotReady;
    } else {
      result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                     pending_context.async);
    }

    // If done, callback user code.
    if(!pending_context.async) {
      pending_context->caller_callback(pending_context->caller_context, result);
    }
  }
}

template <class K, class V, class D>
template <class C>
inline OperationStatus FasterKv<K, V, D>::InternalRead(C& pending_context) {
  typedef C pending_read_context_t;

  if(thread_ctx().phase != Phase::REST) {
    const_cast<faster_t*>(this)->HeavyEnter();
  }

  const key_t& key = pending_context.key();
  KeyHash hash = key.GetHash();
  HashBucketEntry entry;
  AtomicHashBucketEntry* atomic_entry = FindEntry(hash, entry);
  if(!atomic_entry) {
    // No record found.
    return OperationStatus::NOT_FOUND;
  }

  Address address = entry.address();
  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();
  Address safe_read_only_address = hlog.safe_read_only_address.load();
  Address read_only_address = hlog.read_only_address.load();
  uint64_t latest_record_version = 0;

  while (address >= head_address) {
    // Look through the in-memory portion of the log, to find the first record
    // (if any) whose key matches. First make sure we haven't hit an indirection
    // record. If we have, then skip the in-memory lookup.
    auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) {
      const record_t* record =
              reinterpret_cast<const record_t*>(hlog.Get(address));
      latest_record_version = record->header.checkpoint_version;
      if(key != record->key()) {
        address = TraceBackForKeyMatch(key, record->header.previous_address(),
                                       head_address);
      }
    }

    // After the in-memory traversal above check if we hit an indirection
    // record. If we did, then check if the key's hash falls in the range
    // pointed to. If it does, then go async. Otherwise, continue traversing
    // down the in-memory hash chain. Also, make sure we're in-memory.
    if (address < head_address) break;
    hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) break;

    auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(address));
    if (hash >= std::get<0>(rec->hashRange) &&
        hash <= std::get<1>(rec->hashRange))
    {
      // If we're the target and we're receiving data on this hash range, then
      // this operation cannot complete because we might not have received
      // this record yet. This is required for correctness -- we want to avoid
      // going down to dfs for records that are actually going to be migrated.
      if ((thread_ctx().phase == Phase::PREPARE_FOR_RECEIVE ||
           thread_ctx().phase == Phase::RECV_DATA_FROM_TARGET) &&
          migrate_.ShouldMigrate(hash)) return OperationStatus::NOT_READY;

      // If we have too many IOs in progress, reissue this request later.
      if (num_pending_ios.load() > 120) return OperationStatus::NOT_READY;

      pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                               rec->address, entry);
      return OperationStatus::RECORD_ON_DISK;
    }

    address = hdr->previous_address();
  }

  bool migrating = false;
  CheckpointLockGuard sample_guard{ sampling_locks_, hash };

  switch(thread_ctx().phase) {
  case Phase::PREPARE:
    // Reading old version (v).
    if(latest_record_version > thread_ctx().version) {
      // CPR shift detected: we are in the "PREPARE" phase, and a record has
      // a version later than what we've seen.
      pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                                address, entry);
      return OperationStatus::CPR_SHIFT_DETECTED;
    }
    break;

  case Phase::SAMPLING:
    // If this thread is in the SAMPLING phase, then check if this record is
    // a candidate for sampling. If it is, then take an exclusive latch to sync
    // with threads performing Upserts and RMWs in the PREPARE_TO_SAMPLE phase,
    // also convert the operation to an RCU.
    if(migrate_.ShouldSample(hash, address)) {
      if(!sample_guard.try_lock_new()) {
        return OperationStatus::RETRY_NOW;
      }

      // Copy the record to the tail of the hlog.
      goto sample;
    }
    break;

  case Phase::PREPARE_FOR_RECEIVE:
    // If this thread is preparing to receive ownership of a hash range, then
    // it cannot service operations on that hash range.
    if(migrate_.ShouldMigrate(hash)) return OperationStatus::NOT_READY;
    break;

  case Phase::RECV_DATA_FROM_TARGET:
    // If this thread is the target of a migration and is actively receiving
    // migrated data *AND* if the key belongs to the hash range, then set a
    // flag. This way, if the key is not found below, we can return an
    // appropriate error message to the client.
    if(migrate_.ShouldMigrate(hash)) migrating = true;
    break;

  default:
    break;
  }

  if(address >= safe_read_only_address) {
    // Mutable or fuzzy region
    // concurrent read
    if (reinterpret_cast<const record_t*>(hlog.Get(address))->header.tombstone)
    {
      return OperationStatus::NOT_FOUND;
    }
    pending_context.GetAtomic(hlog.Get(address));
    return OperationStatus::SUCCESS;
  } else if(address >= head_address) {
    // Immutable region
    // single-thread read
    if (reinterpret_cast<const record_t*>(hlog.Get(address))->header.tombstone)
    {
      return OperationStatus::NOT_FOUND;
    }
    pending_context.Get(hlog.Get(address));
    return OperationStatus::SUCCESS;
  } else if(address >= begin_address) {
    // Record not available in-memory
    pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                              address, entry);
    return OperationStatus::RECORD_ON_DISK;
  } else if(migrating) {
    // The record was not found and belongs to a migrating hash range. It could
    // be the case that it is yet to arrive from the source.
    return OperationStatus::NOT_READY;
  } else {
    // No record found
    return OperationStatus::NOT_FOUND;
  }

// This code is executed only if there is a migration in progress and this
// record needs to be sampled. It basically copies the record to the tail.
sample:
  // First, allocate a new record at the tail of the hybrid log.
  uint32_t record_size = record_t::size(key, sizeof(value_t));
  Address new_address = BlockAllocate(record_size);
  record_t* new_record = reinterpret_cast<record_t*>(hlog.Get(new_address));

  new(new_record) record_t{
    RecordInfo{
      static_cast<uint16_t>(thread_ctx().version), true, false, false,
      entry.address() },
    key };

  // Allocating a block may have the side effect of advancing the head address,
  // so take a fresh snapshot of the current head address.
  head_address = hlog.head_address.load();

  if(address >= head_address) {
    // Copy out the value to the newly allocated record.
    record_t* old_record = reinterpret_cast<record_t*>(hlog.Get(address));
    void* src = reinterpret_cast<void*>(&(old_record->value()));
    void* des = reinterpret_cast<void*>(&(new_record->value()));
    memcpy(des, src, sizeof(value_t));
  } else {
    // The block we allocated for the new record caused the head address to
    // advance beyond the old record. Need to obtain the old record from disk.
    new_record->header.invalid = true;
    pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                              address, entry);
    return OperationStatus::RECORD_ON_DISK;
  }

  // Read the key into the context. This read is thread-safe because we're
  // currently holding a sampling lock on the hash table entry.
  pending_context.Get(hlog.Get(address));

  // Update the hash table to point to the newly allocated record.
  HashBucketEntry updated_entry{ new_address, hash.tag(), false };
  if(atomic_entry->compare_exchange_strong(entry, updated_entry)) {
    return OperationStatus::SUCCESS;
  } else {
    // CAS failed; forget about sampling and just return instead.
    new_record->header.invalid = true;
    return OperationStatus::SUCCESS;
  }
}

template <class K, class V, class D>
template <class C>
inline OperationStatus FasterKv<K, V, D>::InternalUpsert(C& pending_context) {
  typedef C pending_upsert_context_t;

  if(thread_ctx().phase != Phase::REST) {
    HeavyEnter();
  }

  const key_t& key = pending_context.key();
  KeyHash hash = key.GetHash();
  HashBucketEntry expected_entry;
  HashBucket* bucket;
  AtomicHashBucketEntry* atomic_entry =
        FindOrCreateEntry(hash, expected_entry, bucket);

  // Note that address will be Address::kInvalidAddress, if the atomic_entry
  // was created.
  Address address = expected_entry.address();
  Address head_address = hlog.head_address.load();
  Address read_only_address = hlog.read_only_address.load();
  uint64_t latest_record_version = 0;

  while (address >= head_address) {
    // Multiple keys may share the same hash. Try to find the most recent
    // record with a matching key that we might be able to update in place.
    auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) {
      record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
      latest_record_version = record->header.checkpoint_version;
      if(key != record->key()) {
        address = TraceBackForKeyMatch(key, record->header.previous_address(),
                                       head_address);
      }
    }

    // After the in-memory traversal above check if we hit an indirection
    // record. If we did, then check if the key's hash falls in the range
    // pointed to. If it does, then set address to kInvalidAddress. Also,
    // make sure we're still in memory.
    if (address < head_address) break;
    hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) break;

    auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(address));
    if (hash >= std::get<0>(rec->hashRange) &&
        hash <= std::get<1>(rec->hashRange))
    {
      address = Address::kInvalidAddress;
      break;
    }

    address = hdr->previous_address();
  }

  CheckpointLockGuard lock_guard{ checkpoint_locks_, hash };
  CheckpointLockGuard samp_guard{ sampling_locks_, hash };

  // The common case
  if(thread_ctx().phase == Phase::REST && address >= read_only_address) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.PutAtomic(record)) {
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  }

  // Acquire necessary locks.
  switch(thread_ctx().phase) {
  case Phase::PREPARE:
    // Working on old version (v).
    if(!lock_guard.try_lock_old()) {
      pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                                address, expected_entry);
      return OperationStatus::CPR_SHIFT_DETECTED;
    } else {
      if(latest_record_version > thread_ctx().version) {
        // CPR shift detected: we are in the "PREPARE" phase, and a record has
        // a version later than what we've seen.
        pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                                  address, expected_entry);
        return OperationStatus::CPR_SHIFT_DETECTED;
      }
    }
    break;
  case Phase::IN_PROGRESS:
    // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
    if(latest_record_version < thread_ctx().version) {
      // Will create new record or update existing record to new version (v+1).
      if(!lock_guard.try_lock_new()) {
        pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                                  address, expected_entry);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_PENDING:
    // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
    if(latest_record_version < thread_ctx().version) {
      if(lock_guard.old_locked()) {
        pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                                  address, expected_entry);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_FLUSH:
    // All other threads are in phase
    // {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
    if(latest_record_version < thread_ctx().version) {
      goto create_record;
    }
    break;

  case Phase::PREPARE_TO_SAMPLE:
    // If this thread is in the PREPARE_TO_SAMPLE phase, then check if this
    // record is a candidate for sampling. If it is, then take a shared latch
    // to synchronize with threads performing Reads and RMWs in the
    // SAMPLING phase.
    if(migrate_.ShouldSample(hash, address)) {
      if(!samp_guard.try_lock_old()) {
        return OperationStatus::RETRY_NOW;
      }
    }
    break;
  case Phase::SAMPLING:
    // If this thread is in the SAMPLING phase, then check if this record is a
    // candidate for sampling. If it is, then perform the Upsert at the tail of
    // the log (effectively sampling the record).
    if(migrate_.ShouldSample(hash, address)) {
      goto create_record;
    }
    break;

  case Phase::PREPARE_FOR_RECEIVE:
    // If this thread is preparing to receive ownership of a hash range, then
    // it cannot service operations on that hash range.
    if(migrate_.ShouldMigrate(hash)) return OperationStatus::NOT_READY;
    break;

  default:
    break;
  }

  if(address >= read_only_address) {
    // Mutable region; try to update in place.
    if(atomic_entry->load() != expected_entry) {
      // Some other thread may have RCUed the record before we locked it;
      // try again.
      return OperationStatus::RETRY_NOW;
    }
    // We acquired the necessary locks, so we can update the record's
    // bucket atomically.
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.PutAtomic(record)) {
      // Host successfully replaced record, atomically.
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  }

  // Create a record and attempt RCU.
create_record:
  uint32_t record_size = record_t::size(key, pending_context.value_size());
  Address new_address = BlockAllocate(record_size);
  record_t* record = reinterpret_cast<record_t*>(hlog.Get(new_address));
  new(record) record_t{
    RecordInfo{
      static_cast<uint16_t>(thread_ctx().version), true, false, false,
      expected_entry.address() },
    key };
  pending_context.Put(record);

  HashBucketEntry updated_entry{ new_address, hash.tag(), false };

  if(atomic_entry->compare_exchange_strong(expected_entry, updated_entry)) {
    // Installed the new record in the hash table.
    return OperationStatus::SUCCESS;
  } else {
    // Try again.
    record->header.invalid = true;
    return InternalUpsert(pending_context);
  }
}

template <class K, class V, class D>
template <class C>
inline OperationStatus FasterKv<K, V, D>::InternalRmw(C& pending_context, bool retrying) {
  typedef C pending_rmw_context_t;

  Phase phase = retrying ? pending_context.phase : thread_ctx().phase;
  uint32_t version = retrying ? pending_context.version : thread_ctx().version;

  if(phase != Phase::REST) {
    HeavyEnter();
  }

  const key_t& key = pending_context.key();
  KeyHash hash = key.GetHash();
  HashBucketEntry expected_entry;
  HashBucket* bucket;
  AtomicHashBucketEntry* atomic_entry =
          FindOrCreateEntry(hash, expected_entry, bucket);

  // Note that address will be Address::kInvalidAddress, if the atomic_entry
  // was created.
  Address address = expected_entry.address();
  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();
  Address read_only_address = hlog.read_only_address.load();
  Address safe_read_only_address = hlog.safe_read_only_address.load();
  uint64_t latest_record_version = 0;

  while (address >= head_address) {
    // Multiple keys may share the same hash. Try to find the most recent
    // record with a matching key that we might be able to update in place.
    auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) {
      record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
      latest_record_version = record->header.checkpoint_version;
      if(key != record->key()) {
        address = TraceBackForKeyMatch(key, record->header.previous_address(),
                                       head_address);
      }
    }

    // After the in-memory traversal above check if we hit an indirection
    // record. If we did, then check if the key's hash falls in the range
    // pointed to. If it does, then go async. Otherwise, continue traversing
    // down the in-memory hash chain. Also, make sure we're in-memory.
    if (address < head_address) break;
    hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) break;

    auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(address));
    if (hash >= std::get<0>(rec->hashRange) &&
        hash <= std::get<1>(rec->hashRange))
    {
      // If we're the target and we're receiving data on this hash range, then
      // this operation cannot complete because we might not have received
      // this record yet. This is required for correctness -- we want to avoid
      // going down to dfs for records that are actually going to be migrated.
      if ((phase == Phase::PREPARE_FOR_RECEIVE ||
           phase == Phase::RECV_DATA_FROM_TARGET) &&
          migrate_.ShouldMigrate(hash)) return OperationStatus::NOT_READY;

      // If we have too many IOs in progress, reissue this request later.
      if (num_pending_ios.load() > 120) return OperationStatus::NOT_READY;

      pending_context.go_async(thread_ctx().phase, thread_ctx().version,
                               rec->address, expected_entry);
      return OperationStatus::RECORD_ON_DISK;
    }

    address = hdr->previous_address();
  }

  bool migrating = false;
  CheckpointLockGuard lock_guard{ checkpoint_locks_, hash };
  CheckpointLockGuard samp_guard{ sampling_locks_, hash };

  // The common case.
  if(phase == Phase::REST && address >= read_only_address) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.RmwAtomic(record)) {
      // In-place RMW succeeded.
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  }

  // Acquire necessary locks.
  switch(phase) {
  case Phase::PREPARE:
    // Working on old version (v).
    if(!lock_guard.try_lock_old()) {
      // If we're retrying the operation, then we already have an old lock,
      // so we'll always succeed in obtaining a second. Otherwise, another
      // thread has acquired the new lock, so a CPR shift has occurred.
      assert(!retrying);
      pending_context.go_async(phase, version, address, expected_entry);
      return OperationStatus::CPR_SHIFT_DETECTED;
    } else {
      if(latest_record_version > version) {
        // CPR shift detected: we are in the "PREPARE" phase, and a mutable
        // record has a version later than what we've seen.
        assert(!retrying);
        pending_context.go_async(phase, version, address, expected_entry);
        return OperationStatus::CPR_SHIFT_DETECTED;
      }
    }
    break;
  case Phase::IN_PROGRESS:
    // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
    if(latest_record_version < version) {
      // Will create new record or update existing record to new version (v+1).
      if(!lock_guard.try_lock_new()) {
        if(!retrying) {
          pending_context.go_async(phase, version, address, expected_entry);
        } else {
          pending_context.continue_async(address, expected_entry);
        }
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_PENDING:
    // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
    if(latest_record_version < version) {
      if(lock_guard.old_locked()) {
        if(!retrying) {
          pending_context.go_async(phase, version, address, expected_entry);
        } else {
          pending_context.continue_async(address, expected_entry);
        }
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_FLUSH:
    // All other threads are in phase
    // {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
    if(latest_record_version < version) {
      goto create_record;
    }
    break;

  case Phase::PREPARE_TO_SAMPLE:
    // If this thread is in the PREPARE_TO_SAMPLE phase, then check if this
    // record is a candidate for sampling. If it is, then take a shared latch
    // to synchronize with threads performing Reads and RMWs in the
    // SAMPLING phase.
    if(migrate_.ShouldSample(hash, address)) {
      if(!samp_guard.try_lock_old()) {
        return OperationStatus::RETRY_NOW;
      }
    }
    break;
  case Phase::SAMPLING:
    // If this thread is in the SAMPLING phase, then check if this record is
    // a candidate for sampling. If it is, then take an exclusive latch to sync
    // with threads performing Upserts and RMWs in the PREPARE_TO_SAMPLE phase,
    // also convert the operation to an RCU.
    if(migrate_.ShouldSample(hash, address)) {
      if(!samp_guard.try_lock_new()) {
        return OperationStatus::RETRY_NOW;
      }

      goto create_record;
    }
    break;

  case Phase::PREPARE_FOR_RECEIVE:
    // If this thread is preparing to receive ownership of a hash range, then
    // it cannot service operations on that hash range.
    if(migrate_.ShouldMigrate(hash)) return OperationStatus::NOT_READY;
    break;
  case Phase::RECV_DATA_FROM_TARGET:
    // If this thread is the target of a migration and is actively receiving
    // migrated data *AND* if the key belongs to the hash range, then set a
    // flag. This way, if the key is not found below, we can return an
    // appropriate error message to the client.
    if(migrate_.ShouldMigrate(hash)) migrating = true;
    break;

  default:
    break;
  }

  if(address >= read_only_address) {
    // Mutable region. Try to update in place.
    if(atomic_entry->load() != expected_entry) {
      // Some other thread may have RCUed the record before we locked it;
      // try again.
      return OperationStatus::RETRY_NOW;
    }
    // We acquired the necessary locks, so we can update the record's
    // bucket atomically.
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(!record->header.tombstone && pending_context.RmwAtomic(record)) {
      // In-place RMW succeeded.
      return OperationStatus::SUCCESS;
    } else {
      // Must retry as RCU.
      goto create_record;
    }
  } else if(address >= safe_read_only_address &&
            !reinterpret_cast<record_t*>(hlog.Get(address))->header.tombstone)
  {
    // Fuzzy Region: Must go pending due to lost-update anomaly
    if(!retrying) {
      pending_context.go_async(phase, version, address, expected_entry);
    } else {
      pending_context.continue_async(address, expected_entry);
    }
    return OperationStatus::RETRY_LATER;
  } else if(address >= head_address) {
    goto create_record;
  } else if(address >= begin_address) {
    // Need to obtain old record from disk.
    if(!retrying) {
      pending_context.go_async(phase, version, address, expected_entry);
    } else {
      pending_context.continue_async(address, expected_entry);
    }
    return OperationStatus::RECORD_ON_DISK;
  } else if(migrating) {
    return OperationStatus::NOT_READY;
  } else {
    // Create a new record.
    goto create_record;
  }

  // Create a record and attempt RCU.
create_record:
  const record_t* old_record = nullptr;
  if(address >= head_address) {
    old_record = reinterpret_cast<const record_t*>(hlog.Get(address));
    if(old_record->header.tombstone) {
      old_record = nullptr;
    }
  }
  uint32_t record_size = old_record != nullptr ?
    old_record->size() :
    record_t::size(key, pending_context.value_size());

  Address new_address = BlockAllocate(record_size);
  record_t* new_record = reinterpret_cast<record_t*>(hlog.Get(new_address));

  // Allocating a block may have the side effect of advancing the head address.
  head_address = hlog.head_address.load();
  // Allocating a block may have the side effect of advancing the thread
  // context's version and phase.
  if(!retrying) {
    phase = thread_ctx().phase;
    version = thread_ctx().version;
  }

  new(new_record) record_t{
    RecordInfo{
      static_cast<uint16_t>(version), true, false, false,
      expected_entry.address() },
    key };
  if(old_record == nullptr || address < hlog.begin_address.load()) {
    pending_context.RmwInitial(new_record);
  } else if(address >= head_address) {
    pending_context.RmwCopy(old_record, new_record);
  } else {
    // The block we allocated for the new record caused the head address
    // to advance beyond the old record. Need to obtain the old record
    // from disk.
    new_record->header.invalid = true;
    if(!retrying) {
      pending_context.go_async(phase, version, address, expected_entry);
    } else {
      pending_context.continue_async(address, expected_entry);
    }
    return OperationStatus::RECORD_ON_DISK;
  }

  HashBucketEntry updated_entry{ new_address, hash.tag(), false };
  if(atomic_entry->compare_exchange_strong(expected_entry, updated_entry)) {
    return OperationStatus::SUCCESS;
  } else {
    // CAS failed; try again.
    new_record->header.invalid = true;
    if(!retrying) {
      pending_context.go_async(phase, version, address, expected_entry);
    } else {
      pending_context.continue_async(address, expected_entry);
    }
    return OperationStatus::RETRY_NOW;
  }
}

template <class K, class V, class D>
template<class C>
inline OperationStatus FasterKv<K, V, D>::InternalDelete(C& pending_context) {
  typedef C pending_delete_context_t;

  if(thread_ctx().phase != Phase::REST) {
    HeavyEnter();
  }

  const key_t& key = pending_context.key();
  KeyHash hash = key.GetHash();
  HashBucketEntry expected_entry;
  AtomicHashBucketEntry* atomic_entry = const_cast<AtomicHashBucketEntry*>(FindEntry(hash, expected_entry));
  if(!atomic_entry) {
    // no record found
    return OperationStatus::NOT_FOUND;
  }

  Address address = expected_entry.address();
  Address head_address = hlog.head_address.load();
  Address read_only_address = hlog.read_only_address.load();
  Address begin_address = hlog.begin_address.load();
  uint64_t latest_record_version = 0;

  while (address >= head_address) {
    auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) {
      const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(address));
      latest_record_version = record->header.checkpoint_version;
      if(key != record->key()) {
        address = TraceBackForKeyMatch(key, record->header.previous_address(), head_address);
      }
    }

    // After the in-memory traversal above check if we hit an indirection
    // record. If we did, then check if the key's hash falls in the range
    // pointed to. If it does, then set address to kInvalidAddress. Also,
    // make sure we're still in memory.
    if (address < head_address) break;
    hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) break;

    auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(address));
    if (hash >= std::get<0>(rec->hashRange) &&
        hash <= std::get<1>(rec->hashRange))
    {
      address = Address::kInvalidAddress;
      break;
    }

    address = hdr->previous_address();
  }

  CheckpointLockGuard lock_guard{ checkpoint_locks_, hash };

  // NO optimization for most common case

  // Acquire necessary locks.
  switch (thread_ctx().phase) {
  case Phase::PREPARE:
    // Working on old version (v).
    if(!lock_guard.try_lock_old()) {
      pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
      return OperationStatus::CPR_SHIFT_DETECTED;
    } else if(latest_record_version > thread_ctx().version) {
      // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
      // what we've seen.
      pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
      return OperationStatus::CPR_SHIFT_DETECTED;
    }
    break;
  case Phase::IN_PROGRESS:
    // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
    if(latest_record_version < thread_ctx().version) {
      // Will create new record or update existing record to new version (v+1).
      if(!lock_guard.try_lock_new()) {
        pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_PENDING:
    // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
    if(latest_record_version < thread_ctx().version) {
      if(lock_guard.old_locked()) {
        pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
        return OperationStatus::RETRY_LATER;
      } else {
        // Update to new version (v+1) requires RCU.
        goto create_record;
      }
    }
    break;
  case Phase::WAIT_FLUSH:
    // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
    if(latest_record_version < thread_ctx().version) {
      goto create_record;
    }
    break;
  default:
    break;
  }

  // Mutable Region: Update the record in-place
  if(address >= read_only_address) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    // If the record is the head of the hash chain, try to update the hash chain and completely
    // elide record only if the previous address points to invalid address
    if(expected_entry.address() == address) {
      Address previous_address = record->header.previous_address();
      if (previous_address < begin_address) {
        atomic_entry->compare_exchange_strong(expected_entry, HashBucketEntry::kInvalidEntry);
      }
    }
    record->header.tombstone = true;
    return OperationStatus::SUCCESS;
  }

create_record:
  uint32_t record_size = record_t::size(key, pending_context.value_size());
  Address new_address = BlockAllocate(record_size);
  record_t* record = reinterpret_cast<record_t*>(hlog.Get(new_address));
  new(record) record_t{
    RecordInfo{
      static_cast<uint16_t>(thread_ctx().version), true, true, false,
      expected_entry.address() },
    key };

  HashBucketEntry updated_entry{ new_address, hash.tag(), false };

  if(atomic_entry->compare_exchange_strong(expected_entry, updated_entry)) {
    // Installed the new record in the hash table.
    return OperationStatus::SUCCESS;
  } else {
    // Try again.
    record->header.invalid = true;
    return OperationStatus::RETRY_NOW;
  }
}

template <class K, class V, class D>
inline OperationStatus FasterKv<K, V, D>::InternalRetryPendingRmw(
  async_pending_rmw_context_t& pending_context) {
  OperationStatus status = InternalRmw(pending_context, true);
  if(status == OperationStatus::SUCCESS && pending_context.version != thread_ctx().version) {
    status = OperationStatus::SUCCESS_UNMARK;
  }
  return status;
}

template <class K, class V, class D>
inline Address FasterKv<K, V, D>::TraceBackForKeyMatch(const key_t& key, Address from_address,
    Address min_offset) const {
  while(from_address >= min_offset) {
    // Stop iterating down the chain the moment we hit an indirection record.
    auto hdr = reinterpret_cast<const RecordInfo*>(hlog.Get(from_address));
    if (hdr->IsIndirection()) return from_address;

    const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(from_address));
    if(key == record->key()) {
      return from_address;
    } else {
      from_address = record->header.previous_address();
      continue;
    }
  }
  return from_address;
}

template <class K, class V, class D>
inline Status FasterKv<K, V, D>::HandleOperationStatus(ExecutionContext& ctx,
    pending_context_t& pending_context, OperationStatus internal_status, bool& async) {
  async = false;
  switch(internal_status) {
  case OperationStatus::RETRY_NOW:
    switch(pending_context.type) {
    case OperationType::Read: {
      async_pending_read_context_t& read_context =
        *static_cast<async_pending_read_context_t*>(&pending_context);
      internal_status = InternalRead(read_context);
      break;
    }
    case OperationType::Upsert: {
      async_pending_upsert_context_t& upsert_context =
        *static_cast<async_pending_upsert_context_t*>(&pending_context);
      internal_status = InternalUpsert(upsert_context);
      break;
    }
    case OperationType::RMW: {
      async_pending_rmw_context_t& rmw_context =
        *static_cast<async_pending_rmw_context_t*>(&pending_context);
      internal_status = InternalRmw(rmw_context, false);
      break;
    }
    case OperationType::Delete: {
      async_pending_delete_context_t& delete_context =
        *static_cast<async_pending_delete_context_t*>(&pending_context);
      internal_status = InternalDelete(delete_context);
      break;
    }
    }

    if(internal_status == OperationStatus::SUCCESS) {
      return Status::Ok;
    } else if(internal_status == OperationStatus::NOT_FOUND) {
      return Status::NotFound;
    } else {
      return HandleOperationStatus(ctx, pending_context, internal_status, async);
    }
  case OperationStatus::RETRY_LATER:
    if(thread_ctx().phase == Phase::PREPARE) {
      assert(pending_context.type == OperationType::RMW);
      // Can I be marking an operation again and again?
      if(!checkpoint_locks_.get_lock(pending_context.key().GetHash()).try_lock_old()) {
        return PivotAndRetry(ctx, pending_context, async);
      }
    }
    return RetryLater(ctx, pending_context, async);
  case OperationStatus::RECORD_ON_DISK:
    if(thread_ctx().phase == Phase::PREPARE) {
      assert(pending_context.type == OperationType::Read ||
             pending_context.type == OperationType::RMW);
      // Can I be marking an operation again and again?
      if(!checkpoint_locks_.get_lock(pending_context.key().GetHash()).try_lock_old()) {
        return PivotAndRetry(ctx, pending_context, async);
      }
    }
    return IssueAsyncIoRequest(ctx, pending_context, async);
  case OperationStatus::SUCCESS_UNMARK:
    checkpoint_locks_.get_lock(pending_context.key().GetHash()).unlock_old();
    return Status::Ok;
  case OperationStatus::NOT_FOUND_UNMARK:
    checkpoint_locks_.get_lock(pending_context.key().GetHash()).unlock_old();
    return Status::NotFound;
  case OperationStatus::CPR_SHIFT_DETECTED:
    return PivotAndRetry(ctx, pending_context, async);
  }
  // not reached
  assert(false);
  return Status::Corruption;
}

template <class K, class V, class D>
inline Status FasterKv<K, V, D>::PivotAndRetry(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  // Some invariants
  assert(ctx.version == thread_ctx().version);
  assert(thread_ctx().phase == Phase::PREPARE);
  Refresh();
  // thread must have moved to IN_PROGRESS phase
  assert(thread_ctx().version == ctx.version + 1);
  // retry with new contexts
  pending_context.phase = thread_ctx().phase;
  pending_context.version = thread_ctx().version;
  return HandleOperationStatus(thread_ctx(), pending_context, OperationStatus::RETRY_NOW, async);
}

template <class K, class V, class D>
inline Status FasterKv<K, V, D>::RetryLater(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  IAsyncContext* context_copy;
  Status result = pending_context.DeepCopy(context_copy);
  if(result == Status::Ok) {
    async = true;
    ctx.retry_requests.push_back(context_copy);
    return Status::Pending;
  } else {
    async = false;
    return result;
  }
}

template <class K, class V, class D>
inline constexpr uint32_t FasterKv<K, V, D>::MinIoRequestSize() const {
  return static_cast<uint32_t>(
           sizeof(value_t) + pad_alignment(record_t::min_disk_key_size(),
               alignof(value_t)));
}

template <class K, class V, class D>
inline Status FasterKv<K, V, D>::IssueAsyncIoRequest(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  // Issue asynchronous I/O request
  uint64_t io_id = thread_ctx().io_id++;
  thread_ctx().pending_ios.insert({ io_id, pending_context.key().GetHash() });
  async = true;
  AsyncIOContext io_request{ this, pending_context.address, &pending_context,
                             &thread_ctx().io_responses, io_id };
  AsyncGetFromDisk(pending_context.address, MinIoRequestSize(), AsyncGetFromDiskCallback,
                   io_request);
  return Status::Pending;
}

template <class K, class V, class D>
inline Address FasterKv<K, V, D>::BlockAllocate(uint32_t record_size) {
  uint32_t page;
  Address retval = hlog.Allocate(record_size, page);
  while(retval < hlog.read_only_address.load()) {
    Refresh();
    // Don't overrun the hlog's tail offset.
    bool page_closed = (retval == Address::kInvalidAddress);
    while(page_closed) {
      page_closed = !hlog.NewPage(page);
      Refresh();
    }
    retval = hlog.Allocate(record_size, page);
  }
  return retval;
}

template <class K, class V, class D>
void FasterKv<K, V, D>::AsyncGetFromDisk(Address address, uint32_t num_records,
    AsyncIOCallback callback, AsyncIOContext& context) {
  if(epoch_.IsProtected()) {
    /// Throttling. (Thread pool, unprotected threads are not throttled.)
    /// Avoid throttling reads on indirection records to retain perf after
    /// receiving ownership of a hash range.
    bool indirection = (context.address.control() >> Address::kAddressBits) > 0;
    while(num_pending_ios.load() > 120 && !indirection) {
      disk.TryComplete();
      epoch_.ProtectAndDrain();
    }
  }
  ++num_pending_ios;
  hlog.AsyncGetFromDisk(address, num_records, callback, context);
}

template <class K, class V, class D>
void FasterKv<K, V, D>::AsyncGetFromDiskCallback(IAsyncContext* ctxt, Status result,
    size_t bytes_transferred) {
  CallbackContext<AsyncIOContext> context{ ctxt };
  faster_t* faster = reinterpret_cast<faster_t*>(context->faster);
  /// Context stack is: AsyncIOContext, PendingContext.
  pending_context_t* pending_context = static_cast<pending_context_t*>(context->caller_context);

  /// This I/O is finished.
  --faster->num_pending_ios;
  /// Always "goes async": context is freed by the issuing thread, when processing thread I/O
  /// responses.
  context.async = true;

  /// Bitmask to fetch a record's DFS identifier given its address.
  uint64_t mask = ((~0UL) >> Address::kAddressBits) << Address::kAddressBits;

  pending_context->result = result;
  if(result == Status::Ok) {
    // Check if we hit an indirection record. If we did, then we might have to
    // issue another IO request, potentially to remote storage.
    auto hdr = reinterpret_cast<RecordInfo*>(context->record.GetValidPointer());
    if (hdr->IsIndirection()) {
      auto rec = reinterpret_cast<IndirectionRecord*>(
                            context->record.GetValidPointer());

      // If the key we're looking for falls inside a migrated hash range, then
      // follow the address within the indirection record and issue another IO.
      auto hash = pending_context->key().GetHash();
      if (hash >= std::get<0>(rec->hashRange) &&
          hash <= std::get<1>(rec->hashRange))
      {
        // If this is a bounded IO, then return the moment we hit an indirection
        // record whose hash range this key falls within.
        if (pending_context->type == OperationType::CondUpsert) return;

        context->address = rec->address;
        faster->AsyncGetFromDisk(context->address, faster->MinIoRequestSize(),
                                 AsyncGetFromDiskCallback, *context.get());
        context.async = true;
        return;
      }

      // Issue another async IO on the same faster instance because the key
      // did not fall inside a migrated hash range. Make sure the address the
      // IO is being issued on is a valid address first.
      context->address = Address((context->address.control() & mask) |
                                  hdr->previous_address().control());

      if(context->address >= faster->hlog.begin_address.load()) {
        faster->AsyncGetFromDisk(context->address, faster->MinIoRequestSize(),
                                 AsyncGetFromDiskCallback, *context.get());
        context.async = true;
      } else {
        context->thread_io_responses->push(context.get());
      }

      return;
    }

    record_t* record = reinterpret_cast<record_t*>(context->record.GetValidPointer());
    // Size of the record we read from disk (might not have read the entire record, yet).
    size_t record_size = context->record.available_bytes;
    if(record->min_disk_key_size() > record_size) {
      // Haven't read the full record in yet; I/O is not complete!
      faster->AsyncGetFromDisk(context->address, record->min_disk_key_size(),
                               AsyncGetFromDiskCallback, *context.get());
      context.async = true;
    } else if(record->min_disk_value_size() > record_size) {
      // Haven't read the full record in yet; I/O is not complete!
      faster->AsyncGetFromDisk(context->address, record->min_disk_value_size(),
                               AsyncGetFromDiskCallback, *context.get());
      context.async = true;
    } else if(record->disk_size() > record_size) {
      // Haven't read the full record in yet; I/O is not complete!
      faster->AsyncGetFromDisk(context->address, record->disk_size(),
                               AsyncGetFromDiskCallback, *context.get());
      context.async = true;
    } else if(pending_context->key() == record->key()) {
      //The keys are same, so I/O is complete
      context->thread_io_responses->push(context.get());
    } else {
      //keys are not same. I/O is not complete

      // We issued this IO to a different machine's remote tier, but
      // we've fully walked down it's chain. We need to stop this IO
      // because the record was not found.
      if ((context->address.control() & mask) > 0 &&
          hdr->previous_address().control() == Address::kInvalidAddress) {
        context->thread_io_responses->push(context.get());
        return;
      }

      context->address = Address((context->address.control() & mask) |
                                  hdr->previous_address().control());
      if(context->address >= faster->hlog.begin_address.load()) {
        faster->AsyncGetFromDisk(context->address, faster->MinIoRequestSize(),
                                 AsyncGetFromDiskCallback, *context.get());
        context.async = true;
      } else {
        // Record not found, so I/O is complete.
        context->thread_io_responses->push(context.get());
      }
    }
  }
}

template <class K, class V, class D>
OperationStatus FasterKv<K, V, D>::InternalContinuePendingRead(ExecutionContext& context,
    AsyncIOContext& io_context) {
  // Set a flag determining if we are in the middle of a migration and if the
  // key belongs to the migrating hash range. This way, if the record was not
  // on disk, we can return the correct error code.
  bool migration = false;
  if(thread_ctx().phase == Phase::RECV_DATA_FROM_TARGET) {
    async_pending_read_context_t* ctxt =
        static_cast<async_pending_read_context_t*>(io_context.caller_context);
    migration = migrate_.ShouldMigrate(ctxt->key().GetHash());
  }

  if(io_context.address >= hlog.begin_address.load()) {
    async_pending_read_context_t* pending_context = static_cast<async_pending_read_context_t*>(
          io_context.caller_context);
    record_t* record = reinterpret_cast<record_t*>(io_context.record.GetValidPointer());
    if(record->header.tombstone) {
      return (thread_ctx().version > context.version) ? OperationStatus::NOT_FOUND_UNMARK :
             OperationStatus::NOT_FOUND;
    }
    pending_context->Get(record);
    assert(!kCopyReadsToTail);
    return (thread_ctx().version > context.version) ? OperationStatus::SUCCESS_UNMARK :
           OperationStatus::SUCCESS;
  } else if(migration) {
    return OperationStatus::NOT_READY;
  } else {
    return (thread_ctx().version > context.version) ? OperationStatus::NOT_FOUND_UNMARK :
           OperationStatus::NOT_FOUND;
  }
}

template <class K, class V, class D>
OperationStatus FasterKv<K, V, D>::InternalContinuePendingCondUpsert(
    ExecutionContext& context, AsyncIOContext& io)
{
  auto pending = static_cast<AsyncPendingCondUpsertContext<K>*>(
                             io.caller_context);
  auto h = reinterpret_cast<RecordInfo*>(io.record.GetValidPointer());

  if (h->IsIndirection()) {
    auto r = reinterpret_cast<IndirectionRecord*>(io.record.GetValidPointer());
    auto hash = pending->key().GetHash();
    if (hash >= std::get<0>(r->hashRange) && hash <= std::get<1>(r->hashRange))
    {
      return InternalUpsert(*pending);
    }
  }

  return OperationStatus::SUCCESS;
}

template <class K, class V, class D>
OperationStatus FasterKv<K, V, D>::InternalContinuePendingRmw(ExecutionContext& context,
    AsyncIOContext& io_context) {
  async_pending_rmw_context_t* pending_context = static_cast<async_pending_rmw_context_t*>(
        io_context.caller_context);

  // Set a flag determining if we are in the middle of a migration and if the
  // key belongs to the migrating hash range. This way, if the record was not
  // on disk, we can return the correct error code.
  bool migration = false;
  if(thread_ctx().phase == Phase::RECV_DATA_FROM_TARGET) {
    migration = migrate_.ShouldMigrate(pending_context->key().GetHash());
  }

  // Record not on disk *and* we're in the middle of a migration. Can't proceed
  // until migration completes and we know for certain that the record does not
  // exist on this server's log.
  if(io_context.address < hlog.begin_address.load() && migration) {
    return OperationStatus::NOT_READY;
  }

  // Find a hash bucket entry to store the updated value in.
  const key_t& key = pending_context->key();
  KeyHash hash = key.GetHash();
  HashBucketEntry expected_entry;
  HashBucket* bucket;
  AtomicHashBucketEntry* atomic_entry = FindOrCreateEntry(hash, expected_entry, bucket);

  // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
  Address address = expected_entry.address();
  Address head_address = hlog.head_address.load();

  // Make sure that atomic_entry is OK to update.
  while (address >= head_address) {
    auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) {
      record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
      if(key != record->key()) {
        address = TraceBackForKeyMatch(key, record->header.previous_address(),
                                       expected_entry.address());
      }
    }

    // After the in-memory traversal above check if we hit an indirection
    // record. If we did, then check if the key's hash falls in the range
    // pointed to. If it does, then set address to kInvalidAddress. Also,
    // make sure we're still in memory.
    if (address < head_address) break;
    hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) break;

    auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(address));
    if (hash >= std::get<0>(rec->hashRange) &&
        hash <= std::get<1>(rec->hashRange))
    {
      address = Address::kInvalidAddress;
      break;
    }

    address = hdr->previous_address();
  }

  if(address > pending_context->entry.address()) {
    // We can't trace the current hash bucket entry back to the record we read.
    pending_context->continue_async(address, expected_entry);
    return OperationStatus::RETRY_NOW;
  }
  assert(address < hlog.begin_address.load() ||
         address <= pending_context->entry.address());

  // We have to do copy-on-write/RCU and write the updated value to the tail of the log.
  uint32_t record_size = record_t::size(key, pending_context->value_size());
  Address new_address = BlockAllocate(record_size);
  record_t* new_record = reinterpret_cast<record_t*>(hlog.Get(new_address));

  new(new_record) record_t{
    RecordInfo{
      static_cast<uint16_t>(context.version), true, false, false,
      expected_entry.address() },
    key };
  if(io_context.address < hlog.begin_address.load()) {
    // The on-disk trace back failed to find a key match.
    pending_context->RmwInitial(new_record);
  } else {
    // The record we read from disk.
    const record_t* disk_record = reinterpret_cast<const record_t*>(
                                    io_context.record.GetValidPointer());
    pending_context->RmwCopy(disk_record, new_record);
  }

  HashBucketEntry updated_entry{ new_address, hash.tag(), false };
  if(atomic_entry->compare_exchange_strong(expected_entry, updated_entry)) {
    assert(thread_ctx().version >= context.version);
    return (thread_ctx().version == context.version) ? OperationStatus::SUCCESS :
           OperationStatus::SUCCESS_UNMARK;
  } else {
    // CAS failed; try again.
    new_record->header.invalid = true;
    pending_context->continue_async(address, expected_entry);
    return OperationStatus::RETRY_NOW;
  }
}

template <class K, class V, class D>
void FasterKv<K, V, D>::InitializeCheckpointLocks() {
  uint32_t table_version = resize_info_.version;
  uint64_t size = state_[table_version].size();
  checkpoint_locks_.Initialize(size);
}

template <class K, class V, class D>
void FasterKv<K, V, D>::InitializeSamplingLocks() {
  uint32_t table_version = resize_info_.version;
  uint64_t size = state_[table_version].size();
  sampling_locks_.Initialize(size);
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::WriteIndexMetadata() {
  std::string filename = disk.index_checkpoint_path(checkpoint_.index_token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(&checkpoint_.index_metadata, sizeof(checkpoint_.index_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::ReadIndexMetadata(const Guid& token) {
  std::string filename = disk.index_checkpoint_path(token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "rb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fread(&checkpoint_.index_metadata, sizeof(checkpoint_.index_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::WriteCprMetadata() {
  std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::ReadCprMetadata(const Guid& token) {
  std::string filename = disk.cpr_checkpoint_path(token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "rb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fread(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::WriteCprContext() {
  std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token);
  const Guid& guid = prev_thread_ctx().guid;
  filename += guid.ToString();
  filename += ".dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(static_cast<PersistentExecContext*>(&prev_thread_ctx()),
                 sizeof(PersistentExecContext), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::ReadCprContexts(const Guid& token, const Guid* guids) {
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    const Guid& guid = guids[idx];
    if(guid == Guid{}) {
      continue;
    }
    std::string filename = disk.cpr_checkpoint_path(token);
    filename += guid.ToString();
    filename += ".dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE* file = std::fopen(filename.c_str(), "rb");
    if(!file) {
      return Status::IOError;
    }
    PersistentExecContext context{};
    if(std::fread(&context, sizeof(PersistentExecContext), 1, file) != 1) {
      std::fclose(file);
      return Status::IOError;
    }
    if(std::fclose(file) != 0) {
      return Status::IOError;
    }
    auto result = checkpoint_.continue_tokens.insert({ context.guid, context.serial_num });
    assert(result.second);
  }
  if(checkpoint_.continue_tokens.size() != checkpoint_.log_metadata.num_threads) {
    return Status::Corruption;
  } else {
    return Status::Ok;
  }
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::CheckpointFuzzyIndex() {
  uint32_t hash_table_version = resize_info_.version;
  // Checkpoint the main hash table.
  file_t ht_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&disk.handler()));
  RETURN_NOT_OK(state_[hash_table_version].Checkpoint(disk, std::move(ht_file),
                checkpoint_.index_metadata.num_ht_bytes));
  // Checkpoint the hash table's overflow buckets.
  file_t ofb_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                 "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&disk.handler()));
  RETURN_NOT_OK(overflow_buckets_allocator_[hash_table_version].Checkpoint(disk,
                std::move(ofb_file), checkpoint_.index_metadata.num_ofb_bytes));
  checkpoint_.index_checkpoint_started = true;
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::CheckpointFuzzyIndexComplete() {
  if(!checkpoint_.index_checkpoint_started) {
    return Status::Pending;
  }
  uint32_t hash_table_version = resize_info_.version;
  Status result = state_[hash_table_version].CheckpointComplete(false);
  if(result == Status::Pending) {
    return Status::Pending;
  } else if(result != Status::Ok) {
    return result;
  } else {
    return overflow_buckets_allocator_[hash_table_version].CheckpointComplete(false);
  }
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::RecoverFuzzyIndex() {
  uint8_t hash_table_version = resize_info_.version;
  assert(state_[hash_table_version].size() == checkpoint_.index_metadata.table_size);

  // Recover the main hash table.
  file_t ht_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&disk.handler()));
  RETURN_NOT_OK(state_[hash_table_version].Recover(disk, std::move(ht_file),
                checkpoint_.index_metadata.num_ht_bytes));
  // Recover the hash table's overflow buckets.
  file_t ofb_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                 "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&disk.handler()));
  return overflow_buckets_allocator_[hash_table_version].Recover(disk, std::move(ofb_file),
         checkpoint_.index_metadata.num_ofb_bytes, checkpoint_.index_metadata.ofb_count);
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::RecoverFuzzyIndexComplete(bool wait) {
  uint8_t hash_table_version = resize_info_.version;
  Status result = state_[hash_table_version].RecoverComplete(true);
  if(result != Status::Ok) {
    return result;
  }
  result = overflow_buckets_allocator_[hash_table_version].RecoverComplete(true);
  if(result != Status::Ok) {
    return result;
  }

  // Clear all tentative entries.
  for(uint64_t bucket_idx = 0; bucket_idx < state_[hash_table_version].size(); ++bucket_idx) {
    HashBucket* bucket = &state_[hash_table_version].bucket(bucket_idx);
    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        if(bucket->entries[entry_idx].load().tentative()) {
          bucket->entries[entry_idx].store(HashBucketEntry::kInvalidEntry);
        }
      }
      // Go to next bucket in the chain
      HashBucketOverflowEntry entry = bucket->overflow_entry.load();
      if(entry.unused()) {
        // No more buckets in the chain.
        break;
      }
      bucket = &overflow_buckets_allocator_[hash_table_version].Get(entry.address());
      assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
    }
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::RecoverHybridLog() {
  class Context : public IAsyncContext {
   public:
    Context(hlog_t& hlog_, uint32_t page_, RecoveryStatus& recovery_status_)
      : hlog{ &hlog_}
      , page{ page_ }
      , recovery_status{ &recovery_status_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : hlog{ other.hlog }
      , page{ other.page }
      , recovery_status{ other.recovery_status } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    hlog_t* hlog;
    uint32_t page;
    RecoveryStatus* recovery_status;
  };

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<Context> context{ ctxt };
    result = context->hlog->AsyncReadPagesFromLog(context->page, 1, *context->recovery_status);
  };

  Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
  Address to_address = checkpoint_.log_metadata.final_address;

  uint32_t start_page = from_address.page();
  uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
  uint32_t capacity = hlog.buffer_size();
  RecoveryStatus recovery_status{ start_page, end_page };
  // Initially issue read request for all pages that can be held in memory
  uint32_t total_pages_to_read = end_page - start_page;
  uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
  RETURN_NOT_OK(hlog.AsyncReadPagesFromLog(start_page, pages_to_read_first, recovery_status));

  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }

    // handle start and end at non-page boundaries
    RETURN_NOT_OK(RecoverFromPage(page == start_page ? from_address : Address{ page, 0 },
                                  page + 1 == end_page ? to_address :
                                  Address{ page, Address::kMaxOffset }));

    // OS thread flushes current page and issues a read request if necessary
    if(page + capacity < end_page) {
      Context context{ hlog, page + capacity, recovery_status };
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, callback, &context));
    } else {
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, nullptr, nullptr));
    }
  }
  // Wait until all pages have been flushed
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::RecoverHybridLogFromSnapshotFile() {
  class Context : public IAsyncContext {
   public:
    Context(hlog_t& hlog_, file_t& file_, uint32_t file_start_page_, uint32_t page_,
            RecoveryStatus& recovery_status_)
      : hlog{ &hlog_ }
      , file{ &file_ }
      , file_start_page{ file_start_page_ }
      , page{ page_ }
      , recovery_status{ &recovery_status_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : hlog{ other.hlog }
      , file{ other.file }
      , file_start_page{ other.file_start_page }
      , page{ other.page }
      , recovery_status{ other.recovery_status } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    hlog_t* hlog;
    file_t* file;
    uint32_t file_start_page;
    uint32_t page;
    RecoveryStatus* recovery_status;
  };

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<Context> context{ ctxt };
    result = context->hlog->AsyncReadPagesFromSnapshot(*context->file,
             context->file_start_page, context->page, 1, *context->recovery_status);
  };

  Address file_start_address = checkpoint_.log_metadata.flushed_address;
  Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
  Address to_address = checkpoint_.log_metadata.final_address;

  uint32_t start_page = file_start_address.page();
  uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
  uint32_t capacity = hlog.buffer_size();
  RecoveryStatus recovery_status{ start_page, end_page };
  checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
                                checkpoint_.hybrid_log_token) + "snapshot.dat");
  RETURN_NOT_OK(checkpoint_.snapshot_file.Open(&disk.handler()));

  // Initially issue read request for all pages that can be held in memory
  uint32_t total_pages_to_read = end_page - start_page;
  uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
  RETURN_NOT_OK(hlog.AsyncReadPagesFromSnapshot(checkpoint_.snapshot_file, start_page, start_page,
                pages_to_read_first, recovery_status));

  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }

    // Perform recovery if page in fuzzy portion of the log
    if(Address{ page + 1, 0 } > from_address) {
      // handle start and end at non-page boundaries
      RETURN_NOT_OK(RecoverFromPage(page == from_address.page() ? from_address :
                                    Address{ page, 0 },
                                    page + 1 == end_page ? to_address :
                                    Address{ page, Address::kMaxOffset }));
    }

    // OS thread flushes current page and issues a read request if necessary
    if(page + capacity < end_page) {
      Context context{ hlog, checkpoint_.snapshot_file, start_page, page + capacity,
                       recovery_status };
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, callback, &context));
    } else {
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, nullptr, nullptr));
    }
  }
  // Wait until all pages have been flushed
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::RecoverFromPage(Address from_address, Address to_address) {
  assert(from_address.page() == to_address.page());
  for(Address address = from_address; address < to_address;) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(record->header.IsNull()) {
      address += sizeof(record->header);
      continue;
    }
    if(record->header.invalid) {
      address += record->size();
      continue;
    }
    const key_t& key = record->key();
    KeyHash hash = key.GetHash();
    HashBucketEntry expected_entry;
    HashBucket* bucket;
    AtomicHashBucketEntry* atomic_entry = FindOrCreateEntry(hash, expected_entry, bucket);

    if(record->header.checkpoint_version <= checkpoint_.log_metadata.version) {
      HashBucketEntry new_entry{ address, hash.tag(), false };
      atomic_entry->store(new_entry);
    } else {
      record->header.invalid = true;
      if(record->header.previous_address() < checkpoint_.index_metadata.checkpoint_start_address) {
        HashBucketEntry new_entry{ record->header.previous_address(), hash.tag(), false };
        atomic_entry->store(new_entry);
      }
    }
    address += record->size();
  }

  return Status::Ok;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::RestoreHybridLog() {
  Address tail_address = checkpoint_.log_metadata.final_address;
  uint32_t end_page = tail_address.offset() > 0 ? tail_address.page() + 1 : tail_address.page();
  uint32_t capacity = hlog.buffer_size();
  // Restore as much of the log as will fit in memory.
  uint32_t start_page;
  if(end_page < capacity - hlog.kNumHeadPages) {
    start_page = 0;
  } else {
    start_page = end_page - (capacity - hlog.kNumHeadPages);
  }
  RecoveryStatus recovery_status{ start_page, end_page };

  uint32_t num_pages = end_page - start_page;
  RETURN_NOT_OK(hlog.AsyncReadPagesFromLog(start_page, num_pages, recovery_status));

  // Wait until all pages have been read.
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  // Skip the null page.
  Address head_address = start_page == 0 ? Address{ 0, Constants::kCacheLineBytes } :
                         Address{ start_page, 0 };
  hlog.RecoveryReset(checkpoint_.index_metadata.log_begin_address, head_address, tail_address);
  return Status::Ok;
}

template <class K, class V, class D>
void FasterKv<K, V, D>::HeavyEnter() {
  if(thread_ctx().phase == Phase::GC_IO_PENDING || thread_ctx().phase == Phase::GC_IN_PROGRESS) {
    CleanHashTableBuckets();
    return;
  }
  while(thread_ctx().phase == Phase::GROW_PREPARE) {
    // We spin-wait as a simplification
    // Could instead do a "heavy operation" here
    std::this_thread::yield();
    Refresh();
  }
  if(thread_ctx().phase == Phase::GROW_IN_PROGRESS) {
    SplitHashTableBuckets();
  }
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::CleanHashTableBuckets() {
  uint64_t chunk = gc_.next_chunk++;
  if(chunk >= gc_.num_chunks) {
    // No chunk left to clean.
    return false;
  }
  uint8_t version = resize_info_.version;
  Address begin_address = hlog.begin_address.load();
  uint64_t upper_bound;
  if(chunk + 1 < grow_.num_chunks) {
    // All chunks but the last chunk contain kGrowHashTableChunkSize elements.
    upper_bound = kGrowHashTableChunkSize;
  } else {
    // Last chunk might contain more or fewer elements.
    upper_bound = state_[version].size() - (chunk * kGcHashTableChunkSize);
  }
  for(uint64_t idx = 0; idx < upper_bound; ++idx) {
    HashBucket* bucket = &state_[version].bucket(chunk * kGcHashTableChunkSize + idx);
    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        AtomicHashBucketEntry& atomic_entry = bucket->entries[entry_idx];
        HashBucketEntry expected_entry = atomic_entry.load();
        if(!expected_entry.unused() && expected_entry.address() != Address::kInvalidAddress &&
            expected_entry.address() < begin_address) {
          // The record that this entry points to was truncated; try to delete the entry.
          atomic_entry.compare_exchange_strong(expected_entry, HashBucketEntry::kInvalidEntry);
          // If deletion failed, then some other thread must have added a new record to the entry.
        }
      }
      // Go to next bucket in the chain.
      HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
      if(overflow_entry.unused()) {
        // No more buckets in the chain.
        break;
      }
      bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
    }
  }
  // Done with this chunk--did some work.
  return true;
}

template <class K, class V, class D>
void FasterKv<K, V, D>::AddHashEntry(HashBucket*& bucket, uint32_t& next_idx, uint8_t version,
                                     HashBucketEntry entry) {
  if(next_idx == HashBucket::kNumEntries) {
    // Need to allocate a new bucket, first.
    FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
    HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };
    bucket->overflow_entry.store(new_bucket_entry);
    bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
    next_idx = 0;
  }
  bucket->entries[next_idx].store(entry);
  ++next_idx;
}

template <class K, class V, class D>
Address FasterKv<K, V, D>::TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size,
    Address from_address, Address min_address, uint8_t side) {
  assert(side == 0 || side == 1);
  // Search back as far as min_address.
  while(from_address >= min_address) {
    const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(from_address));
    KeyHash hash = record->key().GetHash();
    if((hash.idx(new_size) < old_size) != (side == 0)) {
      // Record's key hashes to the other side.
      return from_address;
    }
    from_address = record->header.previous_address();
  }
  return from_address;
}

template <class K, class V, class D>
void FasterKv<K, V, D>::SplitHashTableBuckets() {
  // This thread won't exit until all hash table buckets have been split.
  Address head_address = hlog.head_address.load();
  Address begin_address = hlog.begin_address.load();
  for(uint64_t chunk = grow_.next_chunk++; chunk < grow_.num_chunks; chunk = grow_.next_chunk++) {
    uint64_t old_size = state_[grow_.old_version].size();
    uint64_t new_size = state_[grow_.new_version].size();
    assert(new_size == old_size * 2);
    // Split this chunk.
    uint64_t upper_bound;
    if(chunk + 1 < grow_.num_chunks) {
      // All chunks but the last chunk contain kGrowHashTableChunkSize elements.
      upper_bound = kGrowHashTableChunkSize;
    } else {
      // Last chunk might contain more or fewer elements.
      upper_bound = old_size - (chunk * kGrowHashTableChunkSize);
    }
    for(uint64_t idx = 0; idx < upper_bound; ++idx) {

      // Split this (chain of) bucket(s).
      HashBucket* old_bucket = &state_[grow_.old_version].bucket(
                                 chunk * kGrowHashTableChunkSize + idx);
      HashBucket* new_bucket0 = &state_[grow_.new_version].bucket(
                                  chunk * kGrowHashTableChunkSize + idx);
      HashBucket* new_bucket1 = &state_[grow_.new_version].bucket(
                                  old_size + chunk * kGrowHashTableChunkSize + idx);
      uint32_t new_entry_idx0 = 0;
      uint32_t new_entry_idx1 = 0;
      while(true) {
        for(uint32_t old_entry_idx = 0; old_entry_idx < HashBucket::kNumEntries; ++old_entry_idx) {
          HashBucketEntry old_entry = old_bucket->entries[old_entry_idx].load();
          if(old_entry.unused()) {
            // Nothing to do.
            continue;
          } else if(old_entry.address() < head_address) {
            // Can't tell which new bucket the entry should go into; put it in both.
            AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version, old_entry);
            AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version, old_entry);
            continue;
          }

          const record_t* record = reinterpret_cast<const record_t*>(hlog.Get(
                                     old_entry.address()));
          KeyHash hash = record->key().GetHash();
          if(hash.idx(new_size) < old_size) {
            // Record's key hashes to the 0 side of the new hash table.
            AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version, old_entry);
            Address other_address = TraceBackForOtherChainStart(old_size, new_size,
                                    record->header.previous_address(), head_address, 0);
            if(other_address >= begin_address) {
              // We found a record that either is on disk or has a key that hashes to the 1 side of
              // the new hash table.
              AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version,
                           HashBucketEntry{ other_address, old_entry.tag(), false });
            }
          } else {
            // Record's key hashes to the 1 side of the new hash table.
            AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version, old_entry);
            Address other_address = TraceBackForOtherChainStart(old_size, new_size,
                                    record->header.previous_address(), head_address, 1);
            if(other_address >= begin_address) {
              // We found a record that either is on disk or has a key that hashes to the 0 side of
              // the new hash table.
              AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version,
                           HashBucketEntry{ other_address, old_entry.tag(), false });
            }
          }
        }
        // Go to next bucket in the chain.
        HashBucketOverflowEntry overflow_entry = old_bucket->overflow_entry.load();
        if(overflow_entry.unused()) {
          // No more buckets in the chain.
          break;
        }
        old_bucket = &overflow_buckets_allocator_[grow_.old_version].Get(overflow_entry.address());
      }
    }
    // Done with this chunk.
    if(--grow_.num_pending_chunks == 0) {
      // Free the old hash table.
      state_[grow_.old_version].Uninitialize();
      overflow_buckets_allocator_[grow_.old_version].Uninitialize();
      break;
    }
  }
  // Thread has finished growing its part of the hash table.
  thread_ctx().phase = Phase::REST;
  // Thread ack that it has finished growing the hash table.
  if(epoch_.FinishThreadPhase(Phase::GROW_IN_PROGRESS)) {
    // Let other threads know that they can use the new hash table now.
    GlobalMoveToNextState(SystemState{ Action::GrowIndex, Phase::GROW_IN_PROGRESS,
                                       thread_ctx().version });
  } else {
    while(system_state_.load().phase == Phase::GROW_IN_PROGRESS) {
      // Spin until all other threads have finished splitting their chunks.
      std::this_thread::yield();
    }
  }
}

/// This method works through a chunk of the store's hash table, collects
/// records belonging to the hash range under migration, and writes them
/// into a passed in buffer. This method assumes that ownership of the hash
/// range has already been transferred out of this FASTER instance.
template <class K, class V, class D>
uint32_t FasterKv<K, V, D>::Collect(void* opaque, uint8_t* buffer,
                                    uint32_t left)
{
  auto ctxt = reinterpret_cast<MigrationState::MigrationContext*>(opaque);

  uint32_t written = 0;

  // Snapshot the log's head address. Required to determine if an indirection
  // record needs to be sent out.
  Address head = hlog.head_address.load();

  // First, retrieve the position into FASTER's hash table that we need to
  // start or continue collecting records from. If there isn't any work left
  // to do, then try to move the system to the next phase and return.
  uint64_t chunk = 0, bucket = 0, entry = 0, offset = 0;
  if (!ctxt->Continue(&chunk, &bucket, &entry, &offset)) {
    if (!migrate_.GetNextChunk(&chunk)) {
      if(epoch_.FinishThreadPhase(Phase::MOVE_DATA_TO_TARGET)) {
        migrate_.DataMovtStat();
        if (GlobalMoveToNextState(system_state_.load())) {
          migrate_.CompleteMigration();
        }
      }

      // No more work left for this thread. Invoke the cease data movement
      // callback on this thread.
      migrate_.CeaseDataMovt();
      return 0;
    }
  }

  while (true) {
    for(auto idx = bucket; idx < migrate_.BucketsInChunk(chunk); idx++) {
      auto bucketIdx = chunk * MigrationState::kMigrateChunkSize + idx;
      auto b = &state_[resize_info_.version].bucket(bucketIdx);

      // Collect records to be migrated from the bucket. If we ran out of space
      // in the buffer while doing so (as indicated by nextE), then set the
      // context so that we can continue from the correct spot next time. If we
      // scanned through the entire bucket, then reset entry and offset so that
      // the next bucket gets scanned completely.
      int nextE = 0, nextO = 0;
      auto r =
          CollectBucket(b, entry, offset, head, buffer, left, &nextE, &nextO,
                        bucketIdx, ctxt);
      written += r;
      left -= r;
      buffer += r;

      if (nextE != -1) {
        ctxt->Set(chunk, idx, nextE, nextO);
        migrate_.migrated += written;
        return written;
      }

      entry = 0;
      offset = 0;
    }

    // We reached here if we successfully iterated through the chunk without
    // running out of space in the buffer. Try to get a new chunk to work on.
    // If there aren't any chunks, then we're done! Since there is no more
    // work left for this thread, stop iterating and allow the last set of
    // collected records to be returned to the caller.
    if (!migrate_.GetNextChunk(&chunk)) {
      break;
    }

    bucket = 0;
  }

  // If we reached here, then there aren't anymore chunks to migrate. Reset
  // the context and return to the caller.
  *ctxt = MigrationState::MigrationContext();
  migrate_.migrated += written;
  return written;
}

/// This method traverses through a bucket scanning for records to be migrated,
/// and writing them into a passed in buffer. It returns the number of bytes
/// written into the buffer.
///
/// \param bucket
///    Pointer to the bucket to be scanned for records.
/// \param entry
///    The entry within the bucket to start scanning from.
/// \param offset
///    The offset within the above entry's hash chain to start scanning from.
/// \param head
///    Head address of the log. Required to determine when to send out an
///    indirection record.
/// \param buffer
///    Pointer to the buffer into which eligible records must be written.
/// \param left
///    The space in bytes left inside the above buffer.
/// \param[out] nextEntry
///    The entry within the bucket upto which records were scanned and written
///    into the buffer. Set to -1 if the entire bucket was scanned.
/// \param[out] nextOffset
///    The offset within the above entry's chain upto which records were scanned
///    and written into the buffer.
/// \param bucketIdx
///    Bucket index. Required so that we can correctly construct indirection
///    records when we encounter a record on disk.
template <class K, class V, class D>
uint32_t FasterKv<K, V, D>::CollectBucket(HashBucket* bucket, uint64_t entry,
                                          uint64_t offset, Address head,
                                          uint8_t* buffer, uint32_t left,
                                          int* nextEntry, int* nextOffset,
                                          uint64_t bucketIdx,
                                          MigrationState::MigrationContext* s)
{
  uint32_t written = 0;
  *nextEntry = entry;

  while (true) {
    // Check if we need to continue from an overflow bucket that is pointed to
    // by this bucket. If so, update bucket to point to the overflow.
    while (entry > HashBucket::kNumEntries) {
      auto o = bucket->overflow_entry.load();
      bucket =
        &overflow_buckets_allocator_[resize_info_.version].Get(o.address());
      entry -= HashBucket::kNumEntries;
    }

    for (uint32_t eIdx = entry; eIdx < HashBucket::kNumEntries; eIdx++) {
      auto e = bucket->entries[eIdx].load();

      // If this entry is unused then continue to the next one!
      if (e.unused()) continue;

      // If we're at the beginning of the entry's chain, try adding in an
      // indirection record. We first check for sufficient space in the
      // buffer before doing so; helps avoid a corner case where we skip
      // the indirection record but still add in records on the chain.
      if (offset == 0 && !issueIO) {
        auto ret = CollectIndirection(bucketIdx, e.tag(), buffer,
                                      e.address(), head, left);
        if (ret == -1) return written;

        left -= ret;
        written += ret;
        buffer += ret;
      }

      // Iterate down the chain that the entry points to. If we fail to iterate
      // down the entire chain (as indicated by nextOffset), then return. If we
      // did iterate down the entire chain, then reset offset to zero so that we
      // start at the beginning of the next entry's chain.
      auto ret = CollectChain(e.address(), offset, head, buffer, left,
                              nextOffset, bucketIdx, s);
      left -= ret;
      written += ret;
      buffer += ret;

      if (*nextOffset != -1) {
        return written;
      }

      (*nextEntry)++;
      offset = 0;
    }

    // Check for an overflow bucket. If there is one, then scan through entries
    // within it. Reset entry to 0 so that we start scanning from the first
    // entry of this overflow bucket.
    auto overfl = bucket->overflow_entry.load();
    if (overfl.unused()) break;

    bucket =
      &overflow_buckets_allocator_[resize_info_.version].Get(overfl.address());
    entry = 0;
  }

  // If we reached here, then it means that we fully scanned through the bucket
  // and all it's overflow buckets without running out of space. Set nextEntry
  // to -1 indicating so and then return.
  *nextEntry = -1;
  return written;
}

/// This method traverses down a linked list of records collecting those that
/// belong to the hash range under migration into a passed in buffer. It returns
/// the number of bytes that were written into the buffer.
///
/// \param curr
///    Start address of the linked list.
/// \param offset
///    Offset within the linked list to start collecting records from.
/// \param head
///    Head address of the log. Required to determine when to send out an
///    indirection record.
/// \param buffer
///    Pointer to the buffer into which eligible records must be written.
/// \param left
///    The space in bytes left inside the above buffer.
/// \param[out] nextOff
///    The offset within the chain upto which records were scanned and written
///    into the buffer. Set to -1 if the entire chain was scanned.
/// \param bucket
///    Index of the bucket this chain belongs. Required so that we migrate
///    records within a chain only once.
template <class K, class V, class D>
uint32_t FasterKv<K, V, D>::CollectChain(Address curr, uint64_t offset,
                                         Address head, uint8_t* buffer,
                                         uint32_t left, int* nextOff,
                                         uint32_t bucket,
                                         MigrationState::MigrationContext* state)
{
  uint32_t written = 0;
  *nextOff = offset;

  do {
    // We have reached the end of the linked list, no more work for this chain.
    if (curr == Address::kInvalidAddress) {
      *nextOff = -1;
      return written;
    }

    // Record not in memory. An indirection record was already added if
    // `issueIO` is true.
    if (curr < head) {
      /*
       * This code issues a random IO down the chain. It was replaced with
       * a sequential scan using Compact().
      if (issueIO) {
        (state->migrationIOs)++;
        MigrationIOContext context(migrate_.firstHash, migrate_.finalHash,
                                   &(state->onDisk), &(state->migrationIOs));
        AsyncIOContext io(this, curr, &context,
                          &thread_ctx().io_responses, 0);
        AsyncGetFromDisk(curr, MinIoRequestSize(),
                         MigrationDiskCallback<faster_t>, io);

        // We don't want HOB because of this IO, so reduce the count of the
        // number of pending IOs that was just incremented by the above call.
        --num_pending_ios;
      }
      **/

      *nextOff = -1;
      return written;
    }

    auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(curr));
    if (offset--) {
      curr = hdr->previous_address();
      continue;
    }

    // Skip indirection records. CollectBucket() has already added them in.
    if (hdr->IsIndirection()) {
      curr = hdr->previous_address();
      *nextOff += 1;
      continue;
    }

    // If we're coming from a bucket this record does not belong to, then
    // we skip to the next record even if it falls into the hash range.
    auto record = reinterpret_cast<const record_t*>(hlog.Get(curr));
    uint8_t current_version = resize_info_.version;
    uint64_t htSize = state_[current_version].size();
    bool skip = (record->key().GetHash().idx(htSize) != bucket);

    // Check if this record should be migrated. If yes, then copy it into the
    // buffer if possible. Need not worry about mutable region since ownership
    // of the record has already been transferred out.
    if (!skip && migrate_.ShouldMigrate(record->key().GetHash())) {
      // Check if there is enough space in the buffer for the record.
      auto bytes = record->size();
      if (left < bytes) return written;

      memcpy((void*) buffer, (void*) record, bytes);
      buffer += bytes;
      left -= bytes;
      written += bytes;
    }

    // Move back the chain to the previous record and repeat the set of steps.
    curr = record->header.previous_address();
    *nextOff += 1;
  } while (true);

  // This part of the code is never reached. Either reach the end of the chain
  // or reach an indirection record and return.
  *nextOff = -1;
  return written;
}

/// Iterates down a hash chain adding in an indirection record if required.
/// Assumes that there is enough space within the buffer for the record.
///
/// \param bucket
///    Index of the bucket we're currently scanning through.
/// \param tag
///    Tag of the hash table entry we're currently scanning through.
/// \param buffer
///    Pointer to the buffer into which eligible records must be written.
/// \param curr
///    Start address of the linked list.
/// \param head
///    Head address of the log. Required to determine when to send out an
///    indirection record.
template <class K, class V, class D>
int FasterKv<K, V, D>::CollectIndirection(uint64_t bucket, uint64_t tag,
                                          uint8_t* buffer, Address curr,
                                          Address head, uint32_t left)
{
  int written = 0;
  std::vector<IndirectionRecord> indirs;

  uint8_t current_version = resize_info_.version;
  uint64_t htSize = state_[current_version].size();

  do {
    // Looks like this chain does not point to the persistent layer. No
    // indirection record required.
    if (curr == Address::kInvalidAddress) break;

    // Chain falls-off memory. Add an indirection record. We don't need
    // to iterate down anymore.
    if (curr < head) {
      indirs.emplace_back(KeyHash((tag << 48) | bucket),
                          migrate_.firstHash, migrate_.finalHash,
                          migrate_.dfsId, curr, htSize);
      written += IndirectionRecord::size();
      break;
    }

    // If we hit an indirection record, then check if it's hash range falls
    // within the migrating hash range. If it does, need to send out another
    // indirection record with a normalized hash range.
    auto hdr = reinterpret_cast<const RecordInfo*>(hlog.Get(curr));
    if (hdr->IsIndirection()) {
      auto r = reinterpret_cast<const IndirectionRecord*>(hlog.Get(curr));
      KeyHash lH = std::max(std::get<0>(r->hashRange), migrate_.firstHash);
      KeyHash rH = std::min(std::get<1>(r->hashRange), migrate_.finalHash);

      if (lH < rH) {
        indirs.emplace_back(KeyHash((tag << 48) | bucket),
                            lH, rH, r->dfsLogId, r->logicalAddr, htSize);
        written += IndirectionRecord::size();
      }
    }

    // Continue iterating down the chain.
    curr = hdr->previous_address();
  } while (true);

  // No space for these indirection records. Return -1 indicating this to
  // the caller who should eventually retry with a new buffer.
  if (left < written) return -1;

  // Write all the indirection records into the passed in buffer.
  while (!indirs.empty()) {
    auto bytes = IndirectionRecord::size();
    memcpy((void*) buffer, (void*) &(indirs.back()), bytes);

    indirs.pop_back();
    buffer += bytes;
  }

  return written;
}

/// Collects 'size' bytes worth of previously sampled records into a buffer.
template <class K, class V, class D>
uint32_t FasterKv<K, V, D>::CollectSamples(uint8_t* buff, uint32_t size) {
  uint32_t startSize = size;

  // Determine where our samples start from. The head could have advanced since
  // we sampled, so make sure we look only at the in-memory region.
  Address head = hlog.head_address.load();
  Address tail = hlog.GetTailAddress();
  Address curr = migrate_.tailStart > head ? migrate_.tailStart : head;

  logMessage(Lvl::INFO,
             "%.2f KB of data was sampled in range [%lu, %lu]",
             ((double) (tail.control() - curr.control())) / 1024,
             migrate_.firstHash.control(),
             migrate_.finalHash.control());

  // If there aren't any samples, then nothing to do! Just return zero.
  if (curr == tail) return 0;

  uint32_t recordSize = reinterpret_cast<record_t*>(hlog.Get(curr))->size();

  // Iterate through the log as long as we haven't overshot the tail and as
  // long as we have space in our buffer for a sampled record. Need not worry
  // about indirection records at a log offset after migration started.
  for (; curr < tail && size > recordSize; curr += recordSize) {
    auto record = reinterpret_cast<record_t*>(hlog.Get(curr));
    auto orgKey = record->key();

    // First, check if this record belongs to the migrating hash range. If it
    // does not, then just continue over to the next record.
    auto hash = orgKey.GetHash();
    if(!migrate_.ShouldMigrate(hash)) continue;

    // Lookup the key at the hash table, and follow down the relevant chain
    // searching for the first entry with a matching key.
    HashBucketEntry _entry;
    AtomicHashBucketEntry* atomicE = FindEntry(hash, _entry);
    HashBucketEntry expected = atomicE->load();
    Address expectedAddr = expected.address();

    while (expectedAddr >= head) {
      auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(expectedAddr));
      if (!hdr->IsIndirection()) {
        record = reinterpret_cast<record_t*>(hlog.Get(expectedAddr));
        if (orgKey != record->key()) {
          expectedAddr = TraceBackForKeyMatch(orgKey,
                                              record->header.previous_address(),
                                              head);
        }
      }

      // After the in-memory traversal above check if we hit an indirection
      // record. If we did, then check if the key's hash falls in the range
      // pointed to. If it does, then set address to kInvalidAddress. Also,
      // make sure we're still in memory.
      if (expectedAddr < head) break;
      hdr = reinterpret_cast<RecordInfo*>(hlog.Get(expectedAddr));
      if (!hdr->IsIndirection()) break;

      auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(expectedAddr));
      if (hash >= std::get<0>(rec->hashRange) &&
          hash <= std::get<1>(rec->hashRange))
      {
        expectedAddr = Address::kInvalidAddress;
        break;
      }

      expectedAddr = hdr->previous_address();
    }

    // We stopped behind the current address. This means that this record is no
    // longer indexed by the hash table. Move on to the next record.
    if (expectedAddr < curr) continue;

    // We stopped at the current address or ahead of it. Check if keys match. If
    // they do, then write into the buffer.
    record = reinterpret_cast<record_t*>(hlog.Get(curr));
    if (orgKey == record->key()) {
      auto bytes = record->size();
      memcpy((void*) buff, (void*) record, bytes);

      buff += bytes;
      size -= bytes;
    }
  }

  return startSize - size;
}

/// Inserts an indirection record at address `addr` given a hash table bucket
/// index (`bucketIdx`) and a `tag`.
template <class K, class V, class D>
void FasterKv<K, V, D>::InsertIndirectionBucket(uint64_t bucketIdx, uint64_t tag,
                                                IndirectionRecord* migrated,
                                                Address addr)
{
  while (true) {
    KeyHash hash((tag << 48) | bucketIdx);
    HashBucketEntry expected;
    HashBucket* bucket;
    AtomicHashBucketEntry* atomic = FindOrCreateEntry(hash, expected, bucket);

    auto record = reinterpret_cast<IndirectionRecord*>(hlog.Get(addr));
    new((void*) record) IndirectionRecord(migrated->partialHash,
                                          std::get<0>(migrated->hashRange),
                                          std::get<1>(migrated->hashRange),
                                          migrated->dfsLogId,
                                          migrated->logicalAddr,
                                          migrated->htSize,
                                          expected.address());
    record->header.checkpoint_version = thread_ctx().version;

    HashBucketEntry updated(addr, hash.tag(), false);
    if(atomic->compare_exchange_strong(expected, updated)) break;
  }
}

/// Inserts an indirection record into FASTER. Figures out the set of
/// buckets to perform the inserts into. Starts inserting at `addr`,
/// returns the number of bytes that were inserted.
template <class K, class V, class D>
uint32_t FasterKv<K, V, D>::InsertIndirection(IndirectionRecord* migrated,
                                              Address addr)
{
  uint32_t written = 0;

  uint8_t current_version = resize_info_.version;
  uint64_t htSize = state_[current_version].size();

  // The source's hash table was larger than the target's at the time of
  // migration. As a result, we need to insert only one indirection record.
  if (migrated->htSize >= htSize) {
    auto pHash = migrated->partialHash;
    InsertIndirectionBucket(pHash.idx(htSize), pHash.tag(), migrated, addr);
    return IndirectionRecord::size();
  }

  // Looks like the target's hash table is larger. Need to insert an indirection
  // record at every bucket whose hash range could encompass the record.
  for (auto size = 0; size < htSize; size += migrated->htSize) {
    auto pHash = KeyHash(migrated->partialHash.control() + size);
    InsertIndirectionBucket(pHash.idx(htSize), pHash.tag(), migrated, addr);
    written += IndirectionRecord::size();
  }

  return written;
}

/// Inserts a batch of `size` bytes worth of records from a passed in buffer
/// `buff` into the hybrid log.
template <class K, class V, class D>
void FasterKv<K, V, D>::InsertBatch(uint8_t* buff, uint32_t size) {
  // Allocate space at the tail of the log for these records.
  Address next = BlockAllocate(size);

  while (size) {
    auto hdr = reinterpret_cast<RecordInfo*>(buff);
    if (hdr->IsIndirection()) {
      auto rec = reinterpret_cast<IndirectionRecord*>(buff);
      next += InsertIndirection(rec, next);

      buff += IndirectionRecord::size();
      size -= IndirectionRecord::size();
      continue;
    }

    auto rec = reinterpret_cast<record_t*>(buff);
    auto key = rec->key();
    auto val = rec->value();

    // First, retrieve the hash table entry for this key. Retrieve the address
    // this hash table entry points to.
    KeyHash hash = key.GetHash();
    HashBucketEntry expected;
    HashBucket* bucket;
    AtomicHashBucketEntry* atomic = FindOrCreateEntry(hash, expected, bucket);

    Address addr = expected.address();
    Address head = hlog.head_address.load();

    // Next, walk down the linked list pointed to by this entry in search
    // of a record with a matching key. Do so only if the address falls in
    // the main-memory region of the log.
    while (addr >= head) {
      auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(addr));
      if (!hdr->IsIndirection()) {
        record_t* record = reinterpret_cast<record_t*>(hlog.Get(addr));
        if(key != record->key()) {
          addr = TraceBackForKeyMatch(key, record->header.previous_address(),
                                      head);
        }
      }

      // After the in-memory traversal above check if we hit an indirection
      // record. If we did, then check if the key's hash falls in the range
      // pointed to. If it does, then set address to kInvalidAddress. Also,
      // make sure we're still in memory.
      if (addr < head) break;
      hdr = reinterpret_cast<RecordInfo*>(hlog.Get(addr));
      if (!hdr->IsIndirection()) break;

      auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(addr));
      if (hash >= std::get<0>(rec->hashRange) &&
          hash <= std::get<1>(rec->hashRange))
      {
        // Looks like we're migrating a record for which we already have an
        // indirection record. Go ahead and insert it because it is guaranteed
        // to be of a newer version than that pointed to.
        addr = Address::kInvalidAddress;
        break;
      }

      addr = hdr->previous_address();
    }

    // The address is either for an object that existed before ownership
    // transfer, or does not exist. In this case, it is safe to insert the
    // record into the target's log.
    if (addr < migrate_.tailStart || addr == Address::kInvalidAddress) {
      // Setup this record over the allocated region, and copy in the value.
      record_t* record = reinterpret_cast<record_t*>(hlog.Get(next));
      new(record) record_t(
        RecordInfo(static_cast<uint16_t>(thread_ctx().version), true,
                   rec->header.tombstone,
                   false, expected.address()),
        key);
      memcpy((void*) &(record->value()), (void*) &val, V::size());

      // Try to update the hash bucket entry. If this fails, then retry from
      // this record again. If this succeeds, then move to the next record.
      HashBucketEntry updated(next, hash.tag(), false);
      if(!atomic->compare_exchange_strong(expected, updated)) continue;

      next += record->size();
    }

    // The address is for an object in the main-memory region of the log, and
    // was inserted after ownership was transferred. Do nothing here. The record
    // was either modified by the target or was sampled and shipped early.
    if (addr >= head && addr >= migrate_.tailStart) {}

    // The address is for an object on disk that was inserted after ownership
    // transfer but is on disk right now. Issue an asynchronous IO.
    if (addr < head && addr >= migrate_.tailStart) {
      logMessage(Lvl::INFO, "Need to head down to disk to complete insert");
    }

    size -= rec->size();
    buff += rec->size();
  }
}

/// Tries to push the target past the prepare phases of migration. We need
/// this because we might receive a TransferOwnership() and a PushHashes()
/// concurrently. Need to make sure that both calls can push the system forward
/// and only then do the actual work.
template <class K, class V, class D>
void FasterKv<K, V, D>::PushTarget(KeyHash lHash, KeyHash rHash,
                                   ownership_cb_t o, migration_cb_t d,
                                   cntrlpath_cb_t c, iopath_cb_t io)
{
  // First, refresh this thread's view of the system before proceeding.
  Refresh();

  // If the target is in REST, add a migration and wait until this thread
  // moves forward. While waiting, keep refreshing the epoch.
  if (thread_ctx().phase == Phase::REST) {
    Migrate(lHash, rHash, false, o, d, c, io, "", 0);
    do { Refresh(); } while (thread_ctx().phase == Phase::REST);
  }

  // If the target is in PREPARE_FOR_RECEIVE, try to move it to
  // RECV_DATA_FROM_TARGET. Keep trying to do so until either this thread
  // succeeds in doing so, or another thread does. Refresh epoch on every try.
  while (thread_ctx().phase == Phase::PREPARE_FOR_RECEIVE) {
    if (GlobalMoveToNextState(SystemState{ Action::MigrationTarget,
                                           Phase::PREPARE_FOR_RECEIVE,
                                           thread_ctx().version }))
    {
      Refresh();
      break;
    }

    Refresh();
  }
}

/// Receives a buffer of records and inserts them into FASTER. Pushes the system
/// to RECV_DATA_FROM_TARGET if required. Can be invoked by multiple threads.
/// Refer to Migrate() for parameter documentation.
template <class K, class V, class D>
bool FasterKv<K, V, D>::Receive(uint8_t* buffer, uint32_t size, KeyHash lHash,
                                KeyHash rHash, ownership_cb_t o,
                                migration_cb_t d, cntrlpath_cb_t c,
                                iopath_cb_t io)
{
  // Make sure the target is moved through the prepare phases of migration
  // before we proceed with inserting records. At the end of migration we
  // might get an empty RPC with no records after a completion RPC. If that
  // happens, we avoid pushing the target through its state machine again.
  if (size) PushTarget(lHash, rHash, o, d, c, io);

  // Insert the set of received records if there are any.
  if (size) InsertBatch(buffer, size);

  return true;
}

/// Receives a buffer of sampled bytes and inserts them into FASTER. Pushes
/// the system to RECV_DATA_FROM_TARGET. Can be invoked only by a single thread.
/// Refer to Migrate() for parameter documentation.
template <class K, class V, class D>
bool FasterKv<K, V, D>::ReceiveSampled(uint8_t* buffer, uint32_t size,
                                       KeyHash lHash, KeyHash rHash,
                                       ownership_cb_t o, migration_cb_t d,
                                       cntrlpath_cb_t c, iopath_cb_t io)
{
  // Make sure the target is moved through the prepare phases of migration
  // before we proceed with inserting records.
  PushTarget(lHash, rHash, o, d, c, io);

  // Insert sampled records if there are any.
  if (size) InsertBatch(buffer, size);

  return true;
}

/// Pushes the system to REST from RECV_DATA_FROM_TARGET. Should be invoked
/// by only a single thread.
template <class K, class V, class D>
bool FasterKv<K, V, D>::CompleteMigration(KeyHash lHash, KeyHash rHash)
{
  // First, refresh this thread's view of the system before proceeding.
  Refresh();

  // Make sure we're within a migration to begin with.
  if (thread_ctx().phase != Phase::RECV_DATA_FROM_TARGET) {
    logMessage(Lvl::ERROR,
      "Asked to complete migration, but not involved in one to begin with");
    return false;
  }

  // Make sure that we're completing the correct migration.
  bool match = migrate_.firstHash == lHash && migrate_.finalHash == rHash;
  if (!match) {
    logMessage(Lvl::ERROR,
      (std::string("Asked to complete migration for mismatched hash ") +
      "range [%lu, %lu] (Mine: [%lu, %lu])").c_str(),
      lHash, rHash,
      migrate_.firstHash, migrate_.finalHash);
    return false;
  }

  // Try updating the system's state to complete migration.
  if (!GlobalMoveToNextState(SystemState{ Action::MigrationTarget,
                                          Phase::RECV_DATA_FROM_TARGET,
                                          thread_ctx().version }))
  {
    logMessage(Lvl::ERROR,
      "Failed to complete migration of [%lu, %lu] at the target",
      migrate_.firstHash, migrate_.finalHash);
    return false;
  }

  return true;
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::GlobalMoveToNextState(SystemState current_state) {
  SystemState next_state = current_state.GetNextState();
  if(!system_state_.compare_exchange_strong(current_state, next_state)) {
    return false;
  }

  switch(next_state.action) {
  case Action::CheckpointFull:
  case Action::CheckpointIndex:
  case Action::CheckpointHybridLog:
    switch(next_state.phase) {
    case Phase::PREP_INDEX_CHKPT:
      // This case is handled directly inside Checkpoint[Index]().
      assert(false);
      break;
    case Phase::INDEX_CHKPT:
      assert(next_state.action != Action::CheckpointHybridLog);
      // Issue async request for fuzzy checkpoint
      assert(!checkpoint_.failed);
      if(CheckpointFuzzyIndex() != Status::Ok) {
        checkpoint_.failed = true;
      }
      break;
    case Phase::PREPARE:
      // Index checkpoint will never reach this state; and CheckpointHybridLog() will handle this
      // case directly.
      assert(next_state.action == Action::CheckpointFull);
      // INDEX_CHKPT -> PREPARE
      // Get an overestimate for the ofb's tail, after we've finished fuzzy-checkpointing the ofb.
      // (Ensures that recovery won't accidentally reallocate from the ofb.)
      checkpoint_.index_metadata.ofb_count =
        overflow_buckets_allocator_[resize_info_.version].count();
      // Write index meta data on disk
      if(WriteIndexMetadata() != Status::Ok) {
        checkpoint_.failed = true;
      }
      if(checkpoint_.index_persistence_callback) {
        // Notify the host that the index checkpoint has completed.
        checkpoint_.index_persistence_callback(Status::Ok);
      }
      break;
    case Phase::IN_PROGRESS: {
      assert(next_state.action != Action::CheckpointIndex);
      // PREPARE -> IN_PROGRESS
      // Do nothing
      break;
    }
    case Phase::WAIT_PENDING:
      assert(next_state.action != Action::CheckpointIndex);
      // IN_PROGRESS -> WAIT_PENDING
      // Do nothing
      break;
    case Phase::WAIT_FLUSH:
      assert(next_state.action != Action::CheckpointIndex);
      // WAIT_PENDING -> WAIT_FLUSH
      if(fold_over_snapshot) {
        // Move read-only to tail
        Address tail_address = hlog.ShiftReadOnlyToTail();
        // Get final address for CPR
        checkpoint_.log_metadata.final_address = tail_address;
      } else {
        Address tail_address = hlog.GetTailAddress();
        // Get final address for CPR
        checkpoint_.log_metadata.final_address = tail_address;
        checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
                                      checkpoint_.hybrid_log_token) + "snapshot.dat");
        if(checkpoint_.snapshot_file.Open(&disk.handler()) != Status::Ok) {
          checkpoint_.failed = true;
        }
        // Flush the log to a snapshot.
        hlog.AsyncFlushPagesToFile(checkpoint_.log_metadata.flushed_address.page(),
                                   checkpoint_.log_metadata.final_address, checkpoint_.snapshot_file,
                                   checkpoint_.flush_pending);
      }
      // Write CPR meta data file
      if(WriteCprMetadata() != Status::Ok) {
        checkpoint_.failed = true;
      }
      break;
    case Phase::PERSISTENCE_CALLBACK:
      assert(next_state.action != Action::CheckpointIndex);
      // WAIT_FLUSH -> PERSISTENCE_CALLBACK
      break;
    case Phase::REST:
      // PERSISTENCE_CALLBACK -> REST or INDEX_CHKPT -> REST
      if(next_state.action != Action::CheckpointIndex) {
        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
        // another checkpoint can be started.)
        checkpoint_.CheckpointDone();
        // Free checkpoint locks!
        checkpoint_locks_.Free();
        // Checkpoint is done--no more work for threads to do.
        system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      } else {
        // Get an overestimate for the ofb's tail, after we've finished fuzzy-checkpointing the
        // ofb. (Ensures that recovery won't accidentally reallocate from the ofb.)
        checkpoint_.index_metadata.ofb_count =
          overflow_buckets_allocator_[resize_info_.version].count();
        // Write index meta data on disk
        if(WriteIndexMetadata() != Status::Ok) {
          checkpoint_.failed = true;
        }
        auto index_persistence_callback = checkpoint_.index_persistence_callback;
        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
        // another checkpoint can be started.)
        checkpoint_.CheckpointDone();
        // Checkpoint is done--no more work for threads to do.
        system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
        if(index_persistence_callback) {
          // Notify the host that the index checkpoint has completed.
          index_persistence_callback(Status::Ok);
        }
      }
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;

  case Action::GC:
    switch(next_state.phase) {
    case Phase::GC_IO_PENDING:
      // This case is handled directly inside ShiftBeginAddress().
      assert(false);
      break;
    case Phase::GC_IN_PROGRESS:
      // GC_IO_PENDING -> GC_IN_PROGRESS
      // Tell the disk to truncate the log.
      hlog.Truncate(gc_.truncate_callback);
      break;
    case Phase::REST:
      // GC_IN_PROGRESS -> REST
      // GC is done--no more work for threads to do.
      if(gc_.complete_callback) {
        gc_.complete_callback();
      }
      system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;

  case Action::GrowIndex:
    switch(next_state.phase) {
    case Phase::GROW_PREPARE:
      // This case is handled directly inside GrowIndex().
      assert(false);
      break;
    case Phase::GROW_IN_PROGRESS:
      // Swap hash table versions so that all threads will use the new version after populating it.
      resize_info_.version = grow_.new_version;
      break;
    case Phase::REST:
      if(grow_.callback) {
        grow_.callback(state_[grow_.new_version].size());
      }
      system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;

  case Action::MigrationSource:
    switch(next_state.phase) {
    // Use trigger actions to move through sampling phases at the source
    // of a data migration.
    case Phase::PREPARE_TO_SAMPLE:
      logMessage(Lvl::INFO,
                 "Source entering PREPARE_TO_SAMPLE phase of migration");
    case Phase::SAMPLING: {
      if (next_state.phase == Phase::SAMPLING) {
        logMessage(Lvl::INFO,
                   "Source entering SAMPLING phase of migration");
      }

      class Context : public IAsyncContext {
       public:
        Context(faster_t* faster_, SystemState current_state_)
          : faster{ faster_ }
          , current_state{ current_state_ }
        {}

        Context(Context& other)
          : faster{ other.faster }
          , current_state{ other.current_state }
        {}

       protected:
        Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
          return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

       public:
        faster_t* faster;
        SystemState current_state;
      };

      auto callback = [](IAsyncContext* ctxt) {
        CallbackContext<Context> context{ ctxt };
        context->faster->GlobalMoveToNextState(context->current_state);
      };

      // Add trigger action to shift global state
      Context context{ this, next_state };
      IAsyncContext* context_copy;
      Status result = context.DeepCopy(context_copy);
      assert(result == Status::Ok);
      epoch_.BumpCurrentEpoch(callback, context_copy);
      break;
    }
    // Nothing to be done for these cases. They are handled in
    // HandleSpecialPhases() & GetNextState().
    case Phase::PREPARE_FOR_TRANSFER:
      logMessage(Lvl::INFO,
                 "Source entering PREPARE_FOR_TRANSFER phase of migration");
      break;
    case Phase::TRANSFERING_OWNERSHIP:
      logMessage(Lvl::INFO,
                 "Source entering TRANSFERING_OWNERSHIP phase of migration");
      break;
    case Phase::WAIT_FOR_PENDING:
      logMessage(Lvl::INFO,
                 "Source entering WAIT_FOR_PENDING phase of migration");
      break;
    case Phase::MOVE_DATA_TO_TARGET:
      logMessage(Lvl::INFO,
                 "Source entering MOVE_DATA_TO_TARGET phase of migration");
      break;
    case Phase::REST: {
      // Free up locks that were used for sampling.
      InitializeSamplingLocks();

      // Migrate records on disk if required.
      if (issueIO) {
        logMessage(Lvl::INFO,
                   "Source migrating records not in memory");
        Compact(hlog.head_address.control(), migrate_.ioP, false);
      }

      // Set the system action to None. Migration has completed.
      SystemState update{ Action::None, Phase::REST, next_state.version };
      system_state_.store(update);
      logMessage(Lvl::INFO,
                 "Source completed its role in migration. Returning to REST");
      break;
    }
    default:
      // not reached
      assert(false);
      break;
    }
    break;

  case Action::MigrationTarget:
    switch(next_state.phase) {
    case Phase::PREPARE_FOR_RECEIVE:
      logMessage(Lvl::INFO,
                 "Target entering PREPARE_FOR_RECEIVE phase of migration");
      break;
    case Phase::RECV_DATA_FROM_TARGET:
      // Update the tail address at which ownership was received. This will
      // allow us to safely start servicing Upserts at the target immediately.
      migrate_.tailStart = hlog.GetTailAddress().control();
      logMessage(Lvl::INFO,
                 "Target entering RECV_DATA_FROM_TARGET phase of migration");
      break;
    case Phase::REST: {
      // Set the system action to None. Migration has completed.
      SystemState update{ Action::None, Phase::REST, next_state.version };
      system_state_.store(update);
      logMessage(Lvl::INFO,
                 "Target completed its role in migration. Returning to REST");
      break;
    }
    default:
      // not reached
      assert(false);
      break;
    }
    break;

  default:
    // not reached
    assert(false);
    break;
  }
  return true;
}

template <class K, class V, class D>
void FasterKv<K, V, D>::MarkAllPendingRequests() {
  uint32_t table_version = resize_info_.version;
  uint64_t table_size = state_[table_version].size();

  for(const IAsyncContext* ctxt : thread_ctx().retry_requests) {
    const pending_context_t* context = static_cast<const pending_context_t*>(ctxt);
    // We will succeed, since no other thread can currently advance the entry's version, since this
    // thread hasn't acked "PENDING" phase completion yet.
    bool result = checkpoint_locks_.get_lock(context->key().GetHash()).try_lock_old();
    assert(result);
  }
  for(const auto& pending_io : thread_ctx().pending_ios) {
    // We will succeed, since no other thread can currently advance the entry's version, since this
    // thread hasn't acked "PENDING" phase completion yet.
    bool result = checkpoint_locks_.get_lock(pending_io.second).try_lock_old();
    assert(result);
  }
}

template <class K, class V, class D>
void FasterKv<K, V, D>::HandleSpecialPhases() {
  SystemState final_state = system_state_.load();
  if(final_state.phase == Phase::REST) {
    // Nothing to do; just reset thread context.
    thread_ctx().phase = Phase::REST;
    thread_ctx().version = final_state.version;
    return;
  }

  SystemState previous_state{ final_state.action, thread_ctx().phase,
                              thread_ctx().version };
  do {
    // Identify the transition (currentState -> nextState)
    SystemState current_state = (previous_state == final_state) ? final_state :
                                previous_state.GetNextState();
    switch(current_state.action) {
    // Handle phase transitions during checkpointing.
    case Action::CheckpointFull:
    case Action::CheckpointIndex:
    case Action::CheckpointHybridLog:
      switch(current_state.phase) {
      case Phase::PREP_INDEX_CHKPT:
        assert(current_state.action != Action::CheckpointHybridLog);
        // Both from REST -> PREP_INDEX_CHKPT and PREP_INDEX_CHKPT -> PREP_INDEX_CHKPT
        if(previous_state.phase == Phase::REST) {
          // Thread ack that we're performing a checkpoint.
          if(epoch_.FinishThreadPhase(Phase::PREP_INDEX_CHKPT)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::INDEX_CHKPT: {
        assert(current_state.action != Action::CheckpointHybridLog);
        // Both from PREP_INDEX_CHKPT -> INDEX_CHKPT and INDEX_CHKPT -> INDEX_CHKPT
        Status result = CheckpointFuzzyIndexComplete();
        if(result != Status::Pending && result != Status::Ok) {
          checkpoint_.failed = true;
        }
        if(result != Status::Pending) {
          if(current_state.action == Action::CheckpointIndex) {
            // This thread is done now.
            thread_ctx().phase = Phase::REST;
            // Thread ack that it is done.
            if(epoch_.FinishThreadPhase(Phase::INDEX_CHKPT)) {
              GlobalMoveToNextState(current_state);
            }
          } else {
            // Index checkpoint is done; move on to PREPARE phase.
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      }
      case Phase::PREPARE:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle (INDEX_CHKPT -> PREPARE or REST -> PREPARE) and PREPARE -> PREPARE
        if(previous_state.phase != Phase::PREPARE) {
          // mark pending requests
          MarkAllPendingRequests();
          // keep a count of number of threads
          ++checkpoint_.log_metadata.num_threads;
          // set the thread index
          checkpoint_.log_metadata.guids[Thread::id()] = thread_ctx().guid;
          // Thread ack that it has finished marking its pending requests.
          if(epoch_.FinishThreadPhase(Phase::PREPARE)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::IN_PROGRESS:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle PREPARE -> IN_PROGRESS and IN_PROGRESS -> IN_PROGRESS
        if(previous_state.phase == Phase::PREPARE) {
          assert(prev_thread_ctx().retry_requests.empty());
          assert(prev_thread_ctx().pending_ios.empty());
          assert(prev_thread_ctx().io_responses.empty());

          // Get a new thread context; keep track of the old one as "previous."
          thread_contexts_[Thread::id()].swap();
          // initialize a new local context
          thread_ctx().Initialize(Phase::IN_PROGRESS, current_state.version,
                                  prev_thread_ctx().guid, prev_thread_ctx().serial_num);
          // Thread ack that it has swapped contexts.
          if(epoch_.FinishThreadPhase(Phase::IN_PROGRESS)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::WAIT_PENDING:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle IN_PROGRESS -> WAIT_PENDING and WAIT_PENDING -> WAIT_PENDING
        if(!epoch_.HasThreadFinishedPhase(Phase::WAIT_PENDING)) {
          if(prev_thread_ctx().pending_ios.empty() &&
              prev_thread_ctx().retry_requests.empty()) {
            // Thread ack that it has completed its pending I/Os.
            if(epoch_.FinishThreadPhase(Phase::WAIT_PENDING)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::WAIT_FLUSH:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle WAIT_PENDING -> WAIT_FLUSH and WAIT_FLUSH -> WAIT_FLUSH
        if(!epoch_.HasThreadFinishedPhase(Phase::WAIT_FLUSH)) {
          bool flushed;
          if(fold_over_snapshot) {
            flushed = hlog.flushed_until_address.load() >= checkpoint_.log_metadata.final_address;
          } else {
            flushed = checkpoint_.flush_pending.load() == 0;
          }
          if(flushed) {
            // write context info
            WriteCprContext();
            // Thread ack that it has written its CPU context.
            if(epoch_.FinishThreadPhase(Phase::WAIT_FLUSH)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::PERSISTENCE_CALLBACK:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle WAIT_FLUSH -> PERSISTENCE_CALLBACK and PERSISTENCE_CALLBACK -> PERSISTENCE_CALLBACK
        if(previous_state.phase == Phase::WAIT_FLUSH) {
          // Persistence callback
          if(checkpoint_.hybrid_log_persistence_callback) {
            checkpoint_.hybrid_log_persistence_callback(Status::Ok, prev_thread_ctx().serial_num);
          }
          // Thread has finished checkpointing.
          thread_ctx().phase = Phase::REST;
          // Thread ack that it has finished checkpointing.
          if(epoch_.FinishThreadPhase(Phase::PERSISTENCE_CALLBACK)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      default:
        // nothing to do.
        break;
      }
      break;

    // Handle phase transitions during garbage collection.
    case Action::GC:
      switch(current_state.phase) {
      case Phase::GC_IO_PENDING:
        // Handle REST -> GC_IO_PENDING and GC_IO_PENDING -> GC_IO_PENDING.
        if(previous_state.phase == Phase::REST) {
          assert(prev_thread_ctx().retry_requests.empty());
          assert(prev_thread_ctx().pending_ios.empty());
          assert(prev_thread_ctx().io_responses.empty());
          // Get a new thread context; keep track of the old one as "previous."
          thread_contexts_[Thread::id()].swap();
          // initialize a new local context
          thread_ctx().Initialize(Phase::GC_IO_PENDING, current_state.version,
                                  prev_thread_ctx().guid, prev_thread_ctx().serial_num);
        }

        // See if the old thread context has completed its pending I/Os.
        if(!epoch_.HasThreadFinishedPhase(Phase::GC_IO_PENDING)) {
          if(prev_thread_ctx().pending_ios.empty() &&
              prev_thread_ctx().retry_requests.empty()) {
            // Thread ack that it has completed its pending I/Os.
            if(epoch_.FinishThreadPhase(Phase::GC_IO_PENDING)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::GC_IN_PROGRESS:
        // Handle GC_IO_PENDING -> GC_IN_PROGRESS and GC_IN_PROGRESS -> GC_IN_PROGRESS.
        if(!epoch_.HasThreadFinishedPhase(Phase::GC_IN_PROGRESS)) {
          if(!CleanHashTableBuckets()) {
            // No more buckets for this thread to clean; thread has finished GC.
            thread_ctx().phase = Phase::REST;
            // Thread ack that it has finished GC.
            if(epoch_.FinishThreadPhase(Phase::GC_IN_PROGRESS)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      default:
        assert(false); // not reached
        break;
      }
      break;

    // Handle phase transitions during resizing.
    case Action::GrowIndex:
      switch(current_state.phase) {
      case Phase::GROW_PREPARE:
        if(previous_state.phase == Phase::REST) {
          // Thread ack that we're going to grow the hash table.
          if(epoch_.FinishThreadPhase(Phase::GROW_PREPARE)) {
            GlobalMoveToNextState(current_state);
          }
        } else {
          // Wait for all other threads to finish their outstanding (synchronous) hash table
          // operations.
          std::this_thread::yield();
        }
        break;
      case Phase::GROW_IN_PROGRESS:
        SplitHashTableBuckets();
        break;
      }
      break;

    // Handle phase transitions during migration on the source.
    case Action::MigrationSource:
      switch(current_state.phase) {
      // Nothing to be done for these two phases. Epoch trigger actions take
      // care of moving the system forward. These actions are enqueued in
      // GlobalMoveToNextState().
      case Phase::PREPARE_TO_SAMPLE:
        break;
      case Phase::SAMPLING:
        break;
      case Phase::PREPARE_FOR_TRANSFER:
        if(previous_state.phase == Phase::SAMPLING) {
          // Once all threads have entered PREPARE_FOR_TRANSFER, move the
          // migration to the next phase.
          if(epoch_.FinishThreadPhase(Phase::PREPARE_FOR_TRANSFER)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::TRANSFERING_OWNERSHIP:
        if(previous_state.phase == Phase::PREPARE_FOR_TRANSFER) {
          // The system has moved over to a new version. Swap in a
          // new thread context.
          thread_contexts_[Thread::id()].swap();
          thread_ctx().Initialize(Phase::TRANSFERING_OWNERSHIP,
                                  current_state.version,
                                  prev_thread_ctx().guid,
                                  prev_thread_ctx().serial_num);

          // Issue the callback informing clients of when this thread
          // observed an ownership transfer.
          migrate_.ChangeView();

          // Once all threads have entered TRANSFERING_OWNERSHIP, move the
          // migration to the next phase.
          if(epoch_.FinishThreadPhase(Phase::TRANSFERING_OWNERSHIP)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::WAIT_FOR_PENDING:
        // Check if all pending requests (IO and retries) have completed.
        // Only then can we move the migration forward.
        if(!epoch_.HasThreadFinishedPhase(Phase::WAIT_FOR_PENDING)) {
          if(prev_thread_ctx().pending_ios.empty() &&
              prev_thread_ctx().retry_requests.empty()) {
            // Move the system to the next phase once all threads have
            // completed all pending operations on the previous version.
            if(epoch_.FinishThreadPhase(Phase::WAIT_FOR_PENDING)) {
              // Since all pending operations have completed, we can now
              // safely transfer ownership to the target. Only one thread
              // can send out this RPC (the thread that moves the system
              // to the next migration phase.
              if (GlobalMoveToNextState(current_state)) {
                migrate_.TransferOwnership();
              }
            }
          }
        }
        break;
      case Phase::MOVE_DATA_TO_TARGET:
        // Invoke the callback to start data movement on this thread.
        if(previous_state.phase == Phase::WAIT_FOR_PENDING) {
          migrate_.StartDataMovt();
        }
        break;
      default:
        assert(false);
        break;
      }
      break;

    // Handle phase transitions during migration on the target.
    case Action::MigrationTarget:
      switch(current_state.phase) {
      case Phase::PREPARE_FOR_RECEIVE:
        break;
      case Phase::RECV_DATA_FROM_TARGET:
        // Invoke the callback associated with the start of data movement
        // on this thread.
        if(previous_state.phase == Phase::PREPARE_FOR_RECEIVE) {
          migrate_.StartDataMovt();
        }
        break;
      default:
        assert(false);
        break;
      }
      break;
    }
    thread_ctx().phase = current_state.phase;
    thread_ctx().version = current_state.version;
    previous_state = current_state;
  } while(previous_state != final_state);
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::Checkpoint(void(*index_persistence_callback)(Status result),
                                   void(*hybrid_log_persistence_callback)(Status result,
                                       uint64_t persistent_serial_num), Guid& token) {
  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointFull, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateIndexCheckpointDirectory(token);
  disk.CreateCprCheckpointDirectory(token);
  // Obtain tail address for fuzzy index checkpoint
  if(!fold_over_snapshot) {
    checkpoint_.InitializeCheckpoint(token, desired.version, state_[resize_info_.version].size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), true,
                                     hlog.flushed_until_address.load(),
                                     index_persistence_callback,
                                     hybrid_log_persistence_callback);
  } else {
    checkpoint_.InitializeCheckpoint(token, desired.version, state_[resize_info_.version].size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), false,
                                     Address::kInvalidAddress, index_persistence_callback,
                                     hybrid_log_persistence_callback);

  }
  InitializeCheckpointLocks();
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::CheckpointIndex(void(*index_persistence_callback)(Status result),
                                        Guid& token) {
  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointIndex, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateIndexCheckpointDirectory(token);
  checkpoint_.InitializeIndexCheckpoint(token, desired.version,
                                        state_[resize_info_.version].size(),
                                        hlog.begin_address.load(), hlog.GetTailAddress(),
                                        index_persistence_callback);
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::CheckpointHybridLog(void(*hybrid_log_persistence_callback)(Status result,
    uint64_t persistent_serial_num), Guid& token) {
  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointHybridLog, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateCprCheckpointDirectory(token);
  // Obtain tail address for fuzzy index checkpoint
  if(!fold_over_snapshot) {
    checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, true,
        hlog.flushed_until_address.load(), hybrid_log_persistence_callback);
  } else {
    checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, false,
        Address::kInvalidAddress, hybrid_log_persistence_callback);
  }
  InitializeCheckpointLocks();
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class K, class V, class D>
Status FasterKv<K, V, D>::Recover(const Guid& index_token, const Guid& hybrid_log_token,
                                  uint32_t& version,
                                  std::vector<Guid>& session_ids) {
  version = 0;
  session_ids.clear();
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::Recover, Phase::REST, expected.version })) {
    return Status::Aborted;
  }
  checkpoint_.InitializeRecover(index_token, hybrid_log_token);
  Status status;
#define BREAK_NOT_OK(s) \
    status = (s); \
    if (status != Status::Ok) break

  do {
    // Index and log metadata.
    BREAK_NOT_OK(ReadIndexMetadata(index_token));
    BREAK_NOT_OK(ReadCprMetadata(hybrid_log_token));
    if(checkpoint_.index_metadata.version != checkpoint_.log_metadata.version) {
      // Index and hybrid-log checkpoints should have the same version.
      status = Status::Corruption;
      break;
    }

    system_state_.store(SystemState{ Action::Recover, Phase::REST,
                                     checkpoint_.log_metadata.version + 1 });

    BREAK_NOT_OK(ReadCprContexts(hybrid_log_token, checkpoint_.log_metadata.guids));
    // The index itself (including overflow buckets).
    BREAK_NOT_OK(RecoverFuzzyIndex());
    BREAK_NOT_OK(RecoverFuzzyIndexComplete(true));
    // Any changes made to the log while the index was being fuzzy-checkpointed.
    if(fold_over_snapshot) {
      BREAK_NOT_OK(RecoverHybridLog());
    } else {
      BREAK_NOT_OK(RecoverHybridLogFromSnapshotFile());
    }
    BREAK_NOT_OK(RestoreHybridLog());
  } while(false);
  if(status == Status::Ok) {
    for(const auto& token : checkpoint_.continue_tokens) {
      session_ids.push_back(token.first);
    }
    version = checkpoint_.log_metadata.version;
  }
  checkpoint_.RecoverDone();
  system_state_.store(SystemState{ Action::None, Phase::REST,
                                   checkpoint_.log_metadata.version + 1 });
  return status;
#undef BREAK_NOT_OK
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::ShiftBeginAddress(Address address,
    GcState::truncate_callback_t truncate_callback,
    GcState::complete_callback_t complete_callback) {
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::GC, Phase::REST, expected.version })) {
    // Can't start a GC while an action is already in progress.
    return false;
  }
  hlog.begin_address.store(address);
  // Each active thread will notify the epoch when all pending I/Os have completed.
  epoch_.ResetPhaseFinished();
  uint64_t num_chunks = std::max(state_[resize_info_.version].size() / kGcHashTableChunkSize,
                                 (uint64_t)1);
  gc_.Initialize(truncate_callback, complete_callback, num_chunks);
  // Let other threads know to complete their pending I/Os, so that the log can be truncated.
  system_state_.store(SystemState{ Action::GC, Phase::GC_IO_PENDING, expected.version });
  return true;
}

template <class K, class V, class D>
bool FasterKv<K, V, D>::GrowIndex(GrowState::callback_t caller_callback) {
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::GrowIndex, Phase::REST, expected.version })) {
    // An action is already in progress.
    return false;
  }
  epoch_.ResetPhaseFinished();
  uint8_t current_version = resize_info_.version;
  assert(current_version == 0 || current_version == 1);
  uint8_t next_version = 1 - current_version;
  uint64_t num_chunks = std::max(state_[current_version].size() / kGrowHashTableChunkSize,
                                 (uint64_t)1);
  grow_.Initialize(caller_callback, current_version, num_chunks);
  // Initialize the next version of our hash table to be twice the size of the current version.
  state_[next_version].Initialize(state_[current_version].size() * 2, disk.log().alignment());
  overflow_buckets_allocator_[next_version].Initialize(disk.log().alignment(), epoch_);

  SystemState next = SystemState{ Action::GrowIndex, Phase::GROW_PREPARE, expected.version };
  system_state_.store(next);

  // Let this thread know it should be growing the index.
  Refresh();
  return true;
}

/// Initiates a migration on the range [start, end].
///
/// \param start
///    The first hash in the range to be migrated (inclusive).
/// \param end
///    The final hash in the range to be migrated (exclusive).
/// \param src
///    If true, this FASTER instance is the source of the migration.
///    If false, then it is the target of the migration.
/// \param ownerTx
///    Tuple with all state and callbacks required to initiate
///    ownership transfer to the target.
/// \param dataMovt
///    Tuple with all state and callbacks required to initiate and
///    terminate data transfer to the target.
/// \param control
///    Tuple with all state and callbacks required to issue control
///    RPCs to the target.
/// \param ip
///    Ip address of the target machine.
/// \param port
///    Base port on the target on which workers are listening for connections.
///
/// \return
///    True if the migration started. False otherwise.
template <class K, class V, class D>
bool FasterKv<K, V, D>::Migrate(KeyHash start, KeyHash end, bool src,
                                ownership_cb_t ownerTx, migration_cb_t dataMovt,
                                cntrlpath_cb_t control, iopath_cb_t io,
                                const std::string& ip, uint16_t port)
{
  auto ver = system_state_.load().version;
  SystemState curr{ Action::None, Phase::REST, ver };
  SystemState next{ src ? Action::MigrationSource : Action::MigrationTarget,
                    Phase::REST, ver };

  // Try to set the system action to MigrationSource/MigrationTarget.
  if (!system_state_.compare_exchange_strong(curr, next)) {
    return false;
  }

  // Calculate the number of buckets in the hash table.
  uint8_t current_version = resize_info_.version;
  uint64_t buckets = state_[current_version].size();

  // Initialize all migration metadata.
  Address tail = hlog.GetTailAddress().control();
  migrate_.AddMigration(start, end, src, tail, ownerTx, dataMovt, control,
                        io, buckets, ip, port);

  // Prepare the target if this is the source machine.
  if (src) migrate_.PrepareTarget();

  // Move the system to the next Migration phase (REST -> PREPARE_TO_SAMPLE).
  GlobalMoveToNextState(next);

  return true;
}

/// When invoked, compacts the hybrid-log between the begin address and a
/// passed in offset (`untilAddress`). The `compact` callback is invoked
/// to decide whether to append a record to the tail of the log.
template <class K, class V, class D>
bool FasterKv<K, V, D>::Compact(uint64_t untilAddress, compaction_cb_t compact,
                                bool pace)
{
  // If untilAddress is zero, assume we're compacting everything on disk.
  if (untilAddress == 0) untilAddress = hlog.head_address.control();

  // First, initialize a mini FASTER that will store all live records in
  // the range [beginAddress, untilAddress).
  Address begin = hlog.begin_address.load();
  if (untilAddress < begin.control()) return false;

  // Size needs to be aligned to 32 MB and needs to large enough to hold all
  // records in the range we're going to compact.
  uint64_t size = 2 * (untilAddress - begin.control());
  uint64_t norm = 32 * 1024 * 1024;
  size = (size / norm) * norm;

  tempkv_t tempKv(min_table_size_, size, "", 0, "", 1.0);
  tempKv.StartSession();

  logMessage(Lvl::INFO, "Compacting HybridLog between Addresses %lu to %lu",
             begin.control(), untilAddress);

  // In the first phase of compaction, scan the hybrid-log between addresses
  // [beginAddress, untilAddress), adding all live records to the mini FASTER.
  // On encountering a tombstone, we try to delete the record from the mini
  // instance of FASTER.
  int numOps = 0;
  ScanIterator<faster_t> iter(&hlog, Buffering::DOUBLE_PAGE, begin,
                              Address(untilAddress), &disk);
  while (true) {
    auto r = iter.GetNext();
    if (r == nullptr) break;

    if (!r->header.tombstone) {
      CompactionUpsert<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionUpsert<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };
      tempKv.Upsert(ctxt, cb, 0);
    } else {
      CompactionDelete<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionDelete<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };
      tempKv.Delete(ctxt, cb, 0);
    }

    if (++numOps % 1000 == 0) {
      tempKv.Refresh();
      Refresh();
    }
  }

  // Scan the remainder of the hybrid log, deleting all encountered records
  // from the temporary/mini FASTER instance.
  auto upto = LogScanForValidity(Address(untilAddress), &tempKv);

  // Finally, scan through all records within the temporary FASTER instance,
  // inserting those that don't already exist within FASTER's mutable region.
  // Before insertion, also invoke the `compact` callback on the record. In a
  // distributed setting, this can help validate if we own the record.
  numOps = 0;
  Address before = hlog.GetTailAddress();
  ScanIterator<tempkv_t> iter2(&tempKv.hlog, Buffering::DOUBLE_PAGE,
                               tempKv.hlog.begin_address.load(),
                               tempKv.hlog.GetTailAddress(), &tempKv.disk);
  while (true) {
    auto r = iter2.GetNext();
    if (r == nullptr) break;

    if (!r->header.tombstone && !ContainsKeyInMemory(r->key(), upto)) {
      CompactionUpsert<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionUpsert<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };

      auto cookie = std::get<0>(compact);
      auto cFunct = std::get<1>(compact);
      auto rawRec = reinterpret_cast<uint8_t*>(r);
      if (cFunct(cookie, rawRec)) Upsert(ctxt, cb, 0);
    }

    // Pace this phase to avoid overwhelming the system with too many upserts.
    if (++numOps % 1000 == 0) {
      tempKv.Refresh();
      Refresh();
      if (pace) std::this_thread::sleep_for(milliseconds(1));
    }

    // The safe-read-only region might have moved forward since the previous
    // log scan. If it has, perform another validity scan over the delta.
    if (upto < hlog.safe_read_only_address.load()) {
      upto = LogScanForValidity(upto, &tempKv);
    }
  }
  Address after = hlog.GetTailAddress();

  logMessage(Lvl::INFO,
    "Finished compaction between Addresses %lu and %lu. Log grew by %lu MB",
    begin.control(), untilAddress,
    (after.control() - before.control()) / (1024 * 1024));

  tempKv.StopSession();
  return true;
}

/// Scans the hybrid log starting at `from` until the safe-read-only address,
/// deleting all encountered records from a passed in temporary FASTER instance.
/// Useful for log compaction where the temporary instance contains potentially
/// live records that were found before `from` on the log. This method will then
/// delete all records within that instance that are now dead.
///
/// Returns the address upto which the scan was performed.
template <class K, class V, class D>
Address FasterKv<K, V, D>::LogScanForValidity(Address from, tempkv_t* temp)
{
  // Scan upto the safe read only region of the log, deleting all encountered
  // records from the temporary instance of FASTER. Since the safe-read-only
  // offset can advance while we're scanning, we repeat this operation until
  // we converge.
  Address sRO = hlog.safe_read_only_address.load();
  while (from < sRO) {
    int numOps = 0;
    ScanIterator<faster_t> iter(&hlog, Buffering::DOUBLE_PAGE, from,
                                sRO, &disk);
    while (true) {
      auto r = iter.GetNext();
      if (r == nullptr) break;

      CompactionDelete<K, V> ctxt(r);
      auto cb = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<CompactionDelete<K, V>> context(ctxt);
        assert(result == Status::Ok);
      };
      temp->Delete(ctxt, cb, 0);

      if (++numOps % 1000 == 0) {
        temp->Refresh();
        Refresh();
      }
    }

    // Refresh Faster, updating our start and end addresses for the convergence
    // check in the while loop above.
    Refresh();
    from = sRO;
    sRO = hlog.safe_read_only_address.load();
  }

  return sRO;
}

/// Checks if a key exists between a passed in address (`offset`) and the
/// current tail of the hybrid log.
template <class K, class V, class D>
bool FasterKv<K, V, D>::ContainsKeyInMemory(key_t key, Address offset)
{
  // First, retrieve the hash table entry corresponding to this key.
  KeyHash hash = key.GetHash();
  HashBucketEntry _entry;
  AtomicHashBucketEntry* atomic_entry = FindEntry(hash, _entry);
  if (!atomic_entry) return false;

  HashBucketEntry entry = atomic_entry->load();
  Address address = entry.address();

  while (address >= offset) {
    // Look through the in-memory portion of the log, to find the first record
    // (if any) whose key matches. First make sure we haven't hit an indirection
    // record. If we have, then skip the in-memory lookup.
    auto hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) {
      const record_t* record =
              reinterpret_cast<const record_t*>(hlog.Get(address));
      if(key != record->key()) {
        address = TraceBackForKeyMatch(key, record->header.previous_address(),
                                       offset);
      }
    }

    // After the in-memory traversal above check if we hit an indirection
    // record. If we did, then check if the key's hash falls in the range
    // pointed to. If it does, then return false. Otherwise, continue traversing
    // down the in-memory hash chain. Also, make sure we're within the offset.
    if (address < offset) break;
    hdr = reinterpret_cast<RecordInfo*>(hlog.Get(address));
    if (!hdr->IsIndirection()) break;

    auto rec = reinterpret_cast<IndirectionRecord*>(hlog.Get(address));
    if (hash >= std::get<0>(rec->hashRange) &&
        hash <= std::get<1>(rec->hashRange))
    {
      return false;
    }

    address = hdr->previous_address();
  }

  // If we found a record after the passed in address then we succeeded.
  // Otherwise, we failed and so return false.
  if (address >= offset) return true;
  return false;
}

}
} // namespace FASTER::core
