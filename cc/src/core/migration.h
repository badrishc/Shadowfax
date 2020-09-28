// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <time.h>
#include <stdio.h>
#include <stdarg.h>

#include <chrono>

#include "faster.h"
#include "address.h"
#include "key_hash.h"

#include "common/log.h"

/// Uncomment the below line to disable sampling during migration.
/// #define DISABLE_SAMPLING 1

/// If true, issues IOs to disk/DFS instead of indirection records.
bool issueIO = false;

namespace FASTER {
namespace core {

using namespace std::chrono;

/// Typedef for convenience. Defines the type on the callbacks to be invoked
/// at the start and the end of the data movement phase of migration.
typedef std::tuple<void*,
                   void (*)(void*, KeyHash, KeyHash, std::string&, uint16_t),
                   void (*)(void*)
                  > migration_cb_t;

/// Typedef for convenience. Defines the type on the callback to be invoked
/// when a thread transfers out ownership of a hash range.
typedef std::tuple<void*, void (*)(void*)> ownership_cb_t;

/// Typedef for convenience. Defines the type on the callback to be invoked
/// along the control path of migration.
typedef std::tuple<void*,
                   void (*)(void*, KeyHash, KeyHash, std::string&, uint16_t,
                            int)
                  > cntrlpath_cb_t;

/// Typedef for convenience. Defines the type on the callback to be invoked
/// along the io path of migration (if enabled).
typedef std::tuple<void*, bool (*)(void*, uint8_t*)> iopath_cb_t;

/// Encapsulates all state required to complete a data migration between two
/// FASTER instances.
class MigrationState {
 public:

  /// Default constructor.
  MigrationState()
    : firstHash(0)
    , finalHash(0)
    , isSource(true)
    , tailStart(0)
    , ownerTfr()
    , dataMovt()
    , controlP()
    , ioP()
    , num_chunks(0)
    , next_chunk(0)
    , nBuckets(0)
    , startTs()
    , migrated(0)
    , targetIp()
    , basePort(0)
    , dfsId(0)
  {}

  /// Updates MigrationState.
  ///
  /// \param start
  ///    The starting hash of the range to be migrated (inclusive).
  /// \param last
  ///    The ending hash of the range to be migrated (inclusive).
  /// \param src
  ///    If true, this FASTER instance is the source of the migration.
  ///    If false, then it is the target of the migration.
  /// \param tail
  ///    The tail address of the hybrid log at which migration started.
  /// \param ownership
  ///    The callback to be invoked by a FASTER thread when it transfers
  ///    out ownership of a hash range.
  /// \param dataMovtTuple
  ///    The callback to be invoked by a FASTER thread at the start and
  ///    end of the data movement phase of migration.
  /// \param controlTuple
  ///    The callback to be invoked along the migration control path.
  /// \param buckets
  ///    The total number of buckets in the current version of the hash table.
  /// \param ip
  ///    Ip address of the target machine.
  /// \param port
  ///    Base port on the target on which workers are listening for connections.
  void AddMigration(KeyHash start, KeyHash last, bool src, Address tail,
                    ownership_cb_t ownership,
                    migration_cb_t dataMovtTuple,
                    cntrlpath_cb_t controlTuple,
                    iopath_cb_t ioTuple,
                    uint64_t buckets, const std::string& ip, uint16_t port)
  {
    firstHash = start;
    finalHash = last;
    isSource = src;
    tailStart = tail;
    ownerTfr = ownership;
    dataMovt = dataMovtTuple;
    controlP = controlTuple;
    ioP = ioTuple;
    num_chunks = std::max(buckets / kMigrateChunkSize, 1UL);
    next_chunk = 0;
    nBuckets = buckets;
    migrated = 0;
    targetIp = ip;
    basePort = port;
    startTs = high_resolution_clock::now();

    logMessage(Lvl::INFO,
               "Added a migration for hash range [%lu, %lu] to the server",
               start.control(), last.control());
  }

  /// Resets MigrationState to default values.
  void Reset() {
    firstHash = KeyHash(0);
    finalHash = KeyHash(0);
    isSource = true;
    tailStart = 0;
    num_chunks = 0;
    next_chunk = 0;
    nBuckets = 0;
    migrated = 0;
    targetIp = std::string();
    basePort = 0;

    auto oCtx = std::get<0>(ownerTfr);
    if (oCtx) free(oCtx);
    ownerTfr = ownership_cb_t();

    auto dCtx = std::get<0>(dataMovt);
    if (dCtx) free(dCtx);
    dataMovt = migration_cb_t();

    auto cCtx = std::get<0>(controlP);
    if (cCtx) free(cCtx);
    controlP = cntrlpath_cb_t();

    auto iCtx = std::get<0>(ioP);
    if (iCtx) free(iCtx);
    ioP = iopath_cb_t();
  }

  /// Returns true if an operation on a KeyHash at a given address
  /// in the HybridLog should be converted to an Upsert operation.
  ///
  /// \param hash
  ///    The hash of the key.
  /// \param addr
  ///    The address of the key in the HybridLog.
  inline bool ShouldSample(KeyHash hash, Address addr) {
#ifdef DISABLE_SAMPLING
    return false;
#endif

    bool sample =
            (hash >= firstHash) && (hash <= finalHash) && (addr < tailStart);
    if (sample) sampled++;

    return sample;
  }

  /// Returns true if the hash should be migrated.
  inline bool ShouldMigrate(KeyHash hash) {
    return (hash >= firstHash) && (hash <= finalHash);
  }

  /// Returns true if we should skip migrating a hash table entry with
  /// a given hash `tag` within a given hash table `bucket`.
  inline bool SkipChain(uint64_t bucket, uint64_t tag) {
    // Find bounds over the set of hashes contained within the entry. This
    // will be an over-estimation.
    const KeyHash lower((tag << 48) | bucket);
    KeyHash upper(((tag + 1) << 48) | bucket);

    // Don't send the record if the range covered by this entry does not
    // overlap with that of the migrating hash range.
    bool send = true;
    if (upper <= firstHash || finalHash < lower) send = false;

    return !send;
  }

  /// Invokes the callback associated with ownership transfer on a FASTER
  /// thread.
  inline void ChangeView() {
    auto ctxt = std::get<0>(ownerTfr);
    auto func = std::get<1>(ownerTfr);

    func(ctxt);
  }

  /// Invokes the callback associated with the start of data movement on a
  /// FASTER thread.
  inline void StartDataMovt() {
    auto ctxt = std::get<0>(dataMovt);
    auto func = std::get<1>(dataMovt);

    func(ctxt, firstHash, finalHash, targetIp, basePort);
    if (!Thread::id()) {
      logMessage(Lvl::INFO, "Started moving data on range [%lu, %lu]",
                 firstHash.control(), finalHash.control());
    }
  }

  /// Invokes the callback associated with the end of data movement on a
  /// FASTER thread.
  inline void CeaseDataMovt() {
    auto ctxt = std::get<0>(dataMovt);
    auto func = std::get<2>(dataMovt);

    func(ctxt);
  }

  /// Computes stats for data-movement.
  inline void DataMovtStat() {
    auto end = high_resolution_clock::now();
    auto dur = (end - startTs).count();
    logMessage(Lvl::INFO,
               "Migrated %.2f GB in [%lu, %lu] in %.2f seconds (%.2f MBps)",
               ((double) migrated) / (1 << 30),
               firstHash.control(), finalHash.control(),
               dur / 1e9,
               (migrated * 1e9) / (dur * 1024 * 1024));
  }

  /// Sends a PrepForTransfer() message to the target by invoking the
  /// relevant callback.
  inline void PrepareTarget() {
    auto ctxt = std::get<0>(controlP);
    auto func = std::get<1>(controlP);

    func(ctxt, firstHash, finalHash, targetIp, basePort, 0);
    logMessage(Lvl::DEBUG,
               "Sent out PrepForTransfer() for range [%lu, %lu] to the target",
               firstHash.control(), finalHash.control());
  }

  /// Sends a TakeOwnership() message to the target by invoking the
  /// relevant callback.
  inline void TransferOwnership() {
    auto ctxt = std::get<0>(controlP);
    auto func = std::get<1>(controlP);

    func(ctxt, firstHash, finalHash, targetIp, basePort, 1);
    logMessage(Lvl::DEBUG,
               "Sent out TakeOwnership() for range [%lu, %lu] to the target",
               firstHash.control(), finalHash.control());
  }

  /// Sends a CompleteMigration() message to the target by invoking the
  /// relevant callback.
  inline void CompleteMigration() {
    auto ctxt = std::get<0>(controlP);
    auto func = std::get<1>(controlP);

    func(ctxt, firstHash, finalHash, targetIp, basePort, 2);
    logMessage(Lvl::DEBUG,
               "Sent out CompleteMigration() for range [%lu, %lu] to the target",
               firstHash.control(), finalHash.control());
  }

  /// Tries to set the passed in pointer to the next chunk of the hash table.
  /// Returns false if there aren't any chunks left to be scanned.
  inline bool GetNextChunk(uint64_t* chunk) {
    uint64_t next;
    do {
      next = next_chunk;
      if (next >= num_chunks) return false;
    } while (!next_chunk.compare_exchange_strong(next, next + 1));

    *chunk = next;
    return true;
  }

  /// Returns the number of buckets in the passed in chunk.
  inline uint64_t BucketsInChunk(uint64_t chunk) {
    // All except the last chunk will have kMigrateChunkSize buckets.
    if (chunk + 1 < num_chunks) return kMigrateChunkSize;

    // The last chunk might have fewer buckets. Subtract the number of
    // buckets in the other chunks from the total number of buckets.
    return nBuckets - (num_chunks - 1) * kMigrateChunkSize;
  }

  /// A context containing all state required by a thread to collect records
  /// belonging to the hash range under migration 
  struct MigrationContext {
    /// The chunk of the hash table that the thread should work over, collecting
    /// records to be migrated to the target.
    uint64_t chunk;

    /// The bucket within the above chunk that the thread should start working
    /// over.
    uint64_t bucket;

    /// The hash entry within the above bucket that the thread should start
    /// working over.
    uint64_t entry;

    /// The offset within the above hash entry's linked list that the thread
    /// should start working over.
    uint64_t offset;

    /// All records that are on disk but should be migrated.
    std::vector<uint8_t> onDisk;

    /// The number of in-progress migration IO requests.
    uint64_t migrationIOs;

    /// Default initializer/constructor.
    MigrationContext()
     : chunk(0)
     , bucket(0)
     , entry(0)
     , offset(0)
     , onDisk()
     , migrationIOs(0)
    {}

    /// Sets the chunk (\param c), bucket (\param b), entry (\param e), and
    /// offset (\param o) that a thread should continue collecting records from.
    /// Returns true if these parameters were set, and false if there is nothing
    /// to continue from.
    inline bool Continue(uint64_t* c, uint64_t* b, uint64_t* e, uint64_t* o) {
      // Nothing to continue from. The caller will have to retrieve a
      // chunk from MigrationState.
      if (!chunk && !bucket && !entry && !offset) return false;

      *c = chunk;
      *b = bucket;
      *e = entry;
      *o = offset;

      return true;
    }

    /// Sets the internal state of the context to the chunk (\param c),
    /// bucket (\param b), entry (\param e), and offset (\param o) that a thread
    /// should continue collecting records from next time.
    inline void Set(uint64_t c, uint64_t b, uint64_t e, uint64_t o) {
      chunk = c;
      bucket = b;
      entry = e;
      offset = o;
    }
  };

  /// The first hash in the range to be migrated (inclusive).
  KeyHash firstHash;

  /// The final hash in the range to be migrated (exclusive).
  KeyHash finalHash;

  /// Boolean determining if this FASTER instance is the source
  /// or target of the migration.
  bool isSource;

  /// The tail address of the hybrid log at the moment migration started.
  Address tailStart;

  /// The number of records in the migrating hash range that were sampled
  /// and copied to the end of the log.
  std::atomic<uint64_t> sampled;

  /// Callback to be invoked by a FASTER thread when it transfers ownership.
  ownership_cb_t ownerTfr;

  /// Callback to be invoked by a FASTER thread at the start and end of
  /// the data movement phase of migration.
  migration_cb_t dataMovt;

  /// Callback to be invoked on the control path of migration.
  cntrlpath_cb_t controlP;

  /// Callback to be invoked along the io path of migration (if enabled).
  iopath_cb_t ioP;

  /// Number of chunks the source's hash table should be divided into. Each
  /// thread works over one chunk at a time.
  uint64_t num_chunks;

  /// The number of buckets per hash table chunk.
  static constexpr uint64_t kMigrateChunkSize = 16384;

  /// The next chunk to be worked over at the source.
  std::atomic<uint64_t> next_chunk;

  /// The total number of buckets in the current version of the hash table.
  uint64_t nBuckets;

  /// The time stamp at which migration started.
  time_point<high_resolution_clock> startTs;

  /// Number of bytes migrated to the target so far.
  uint64_t migrated;

  /// The IP address of the target machine.
  std::string targetIp;

  /// The base port on which target threads are listening for connections.
  /// Thread 'i' is assumed to be listening on 'basePort + i'.
  uint16_t basePort;

  /// The identifier of this machine's on-disk/dfs log.
  uint16_t dfsId;
};

/// Context required when we issue IOs at the source during migration instead
/// of sending out indirection records.
class MigrationIOContext : public IAsyncContext {
 public:
  /// Constructs a context encapsulating a migrating hash range and a buffer
  /// that records can be appended to.
  MigrationIOContext(KeyHash left, KeyHash right, std::vector<uint8_t>* buf,
                     uint64_t* cntr)
   : hashRange(left, right)
   , buffer(buf)
   , ioCounter(cntr)
  {}

  /// Copy constructor. Required because the operation goes asynchronous.
  MigrationIOContext(const MigrationIOContext& from)
   : hashRange(from.hashRange)
   , buffer(from.buffer)
   , ioCounter(from.ioCounter)
  {}

 protected:
  /// Copies this context when the operation goes asynchronous inside FASTER.
  Status DeepCopy_Internal(IAsyncContext*& copy) {
    return IAsyncContext::DeepCopy_Internal(*this, copy);
  }

 public:
  /// The hash range under migration. Required so we can determine if the record
  /// should be migrated or not.
  std::pair<KeyHash, KeyHash> hashRange;

  /// If the record is to be migrated it will be enqueued here.
  std::vector<uint8_t>* buffer;

  /// A counter to decrement once the IO has completed.
  uint64_t* ioCounter;
};

/// Callback for when we issue IOs on the migration datapath instead of sending
/// out indirection records to the target.
template <class faster_t>
void MigrationDiskCallback(IAsyncContext* ctxt, Status result, size_t bytes) {
  CallbackContext<AsyncIOContext> context(ctxt);

  faster_t* faster = reinterpret_cast<faster_t*>(context->faster);
  MigrationIOContext* m =
        static_cast<MigrationIOContext*>(context->caller_context);

  // Looks like the IO failed. Log this message and return. Nothing more to do.
  if (result != Status::Ok) {
    logMessage(Lvl::ERROR, "Migration IO on range [%lu, %lu] failed",
               m->hashRange.first.control(), m->hashRange.second.control());
    return;
  }

  // Retrieve the record's hash. If it falls inside our migrating hash range,
  // append to the buffer and continue down to chain to make sure we collect
  // all of our records in the hash range.
  auto record =
      reinterpret_cast<typename faster_t::record_t*>(
      context->record.GetValidPointer());
  KeyHash hash = record->key().GetHash();

  if (hash >= m->hashRange.first && hash <= m->hashRange.second) {
    auto pos = m->buffer->begin() + m->buffer->size();
    uint8_t* s = reinterpret_cast<uint8_t*>(record);
    m->buffer->insert(pos, s, s + record->size());
  }

  // Continue walking down the chain if possible. If we cannot, then this IO
  // is officially complete.
  context->address = record->header.previous_address();
  if (context->address >= faster->hlog.begin_address.load()) {
    context.async = true;
    faster->AsyncGetFromDisk(context->address, faster->MinIoRequestSize(),
                             MigrationDiskCallback<faster_t>, *context.get());

    // We don't want HOB because of this IO, so reduce the count of the number
    // of pending IOs that was just incremented by the above call.
    --faster->num_pending_ios;
  } else {
    *(m->ioCounter)--;
  }

  return;
}

} // namespace core
} // namespace FAST
