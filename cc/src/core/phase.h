// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <thread>

#include "alloc.h"
#include "async.h"
#include "constants.h"
#include "thread.h"
#include "utility.h"

namespace FASTER {
namespace core {

/// Phases, used internally by FASTER to keep track of how far along FASTER has gotten during
/// checkpoint, gc, grow, and migration actions.
enum class Phase : uint8_t {
  /// Checkpoint phases.
  PREP_INDEX_CHKPT,
  INDEX_CHKPT,
  PREPARE,
  IN_PROGRESS,
  WAIT_PENDING,
  WAIT_FLUSH,
  REST,
  PERSISTENCE_CALLBACK,

  /// Garbage-collection phases.
  /// - The log's begin-address has been shifted; finish all outstanding I/Os before trying to
  ///   truncate the log.
  GC_IO_PENDING,
  /// - The log has been truncated, but threads are still cleaning the hash table.
  GC_IN_PROGRESS,

  /// Grow-index phases.
  /// - Each thread waits for all other threads to complete outstanding (synchronous) operations
  ///   against the hash table.
  GROW_PREPARE,
  /// - Each thread copies a chunk of the old hash table into the new hash table.
  GROW_IN_PROGRESS,

  /// Source migration phases.
  /// - Threads on the source start preparing for the sampling phase by taking
  ///   shared latches on mutable records falling in the hash region under
  ///   migration. Records not belonging to this region are processed normally.
  PREPARE_TO_SAMPLE,
  /// - Threads on the source start sampling hot records in the migrating range
  ///   by copying them to the tail of the log. Each record is copied atmost
  ///   once. Exclusive latches are taken on records falling in the mutable
  ///   region, even during reads.
  SAMPLING,
  /// - Threads on the source start preparing for a version change by taking
  ///   shared latches on all records and marking requests that go pending.
  PREPARE_FOR_TRANSFER,
  /// - Threads on the source begin redirecting requests on the migrating hash
  ///   range to the target. Each thread invokes a callback that informs the
  ///   client session that ownership has moved.
  TRANSFERING_OWNERSHIP,
  /// - There might be requests on the hash range that went pending. Wait for
  ///   all of them to complete before moving data over to the target.
  WAIT_FOR_PENDING,
  /// - Move all state over to the target, starting with the sampled records.
  MOVE_DATA_TO_TARGET,

  /// Target migration phases.
  /// - Threads on the target start preparing to receive ownership of the hash
  ///   range under migration. Since a few threads on the source could have
  ///   already undergone a view change, target threads actively check for
  ///   requests belonging to hash range and buffer them.
  PREPARE_FOR_RECEIVE,
  /// - Threads on the target have received ownership of the target and are
  ///   actively receiving data from the source. All previously buffered
  ///   operations can now be executed.
  RECV_DATA_FROM_TARGET,

  INVALID
};

}
} // namespace FASTER::core
