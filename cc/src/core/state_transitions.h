// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include "phase.h"

namespace FASTER {
namespace core {

struct ResizeInfo {
  uint8_t version;
};

/// Each FASTER store can perform only one action at a time (checkpoint, recovery, garbage
/// collect, grow index, or data migration).
enum class Action : uint8_t {
  None = 0,
  CheckpointFull,
  CheckpointIndex,
  CheckpointHybridLog,
  Recover,
  GC,
  GrowIndex,
  MigrationSource,
  MigrationTarget
};

struct SystemState {
  SystemState(Action action_, Phase phase_, uint32_t version_)
    : control_{ 0 }
  {
    action = action_;
    phase = phase_;
    version = version_;
  }

  SystemState(uint64_t control)
    : control_{ control } {
  }

  SystemState(const SystemState& other)
    : control_{ other.control_ } {
  }

  inline SystemState& operator=(const SystemState& other) {
    control_ = other.control_;
    return *this;
  }

  inline bool operator==(const SystemState& other) {
    return control_ == other.control_;
  }

  inline bool operator!=(const SystemState& other) {
    return control_ != other.control_;
  }

  /// State transitions.
  inline SystemState GetNextState() const {
    switch(action) {
    // State machine for a full checkpoint of FASTER.
    case Action::CheckpointFull:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::CheckpointFull, Phase::PREP_INDEX_CHKPT, version };
      case Phase::PREP_INDEX_CHKPT:
        return SystemState{ Action::CheckpointFull, Phase::INDEX_CHKPT, version };
      case Phase::INDEX_CHKPT:
        return SystemState{ Action::CheckpointFull, Phase::PREPARE, version };
      case Phase::PREPARE:
        return SystemState{ Action::CheckpointFull, Phase::IN_PROGRESS, version + 1 };
      case Phase::IN_PROGRESS:
        return SystemState{ Action::CheckpointFull, Phase::WAIT_PENDING, version };
      case Phase::WAIT_PENDING:
        return SystemState{ Action::CheckpointFull, Phase::WAIT_FLUSH, version };
      case Phase::WAIT_FLUSH:
        return SystemState{ Action::CheckpointFull, Phase::PERSISTENCE_CALLBACK, version };
      case Phase::PERSISTENCE_CALLBACK:
        return SystemState{ Action::CheckpointFull, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;

    // State machine for an index (hash table) checkpoint.
    case Action::CheckpointIndex:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::CheckpointIndex, Phase::PREP_INDEX_CHKPT, version };
      case Phase::PREP_INDEX_CHKPT:
        return SystemState{ Action::CheckpointIndex, Phase::INDEX_CHKPT, version };
      case Phase::INDEX_CHKPT:
        return SystemState{ Action::CheckpointIndex, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;

    // State machine for a HybridLog checkpoint.
    case Action::CheckpointHybridLog:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::CheckpointHybridLog, Phase::PREPARE, version };
      case Phase::PREPARE:
        return SystemState{ Action::CheckpointHybridLog, Phase::IN_PROGRESS, version + 1 };
      case Phase::IN_PROGRESS:
        return SystemState{ Action::CheckpointHybridLog, Phase::WAIT_PENDING, version };
      case Phase::WAIT_PENDING:
        return SystemState{ Action::CheckpointHybridLog, Phase::WAIT_FLUSH, version };
      case Phase::WAIT_FLUSH:
        return SystemState{ Action::CheckpointHybridLog, Phase::PERSISTENCE_CALLBACK, version };
      case Phase::PERSISTENCE_CALLBACK:
        return SystemState{ Action::CheckpointHybridLog, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;

    // State machine for garbage collection.
    case Action::GC:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::GC, Phase::GC_IO_PENDING, version };
      case Phase::GC_IO_PENDING:
        return SystemState{ Action::GC, Phase::GC_IN_PROGRESS, version };
      case Phase::GC_IN_PROGRESS:
        return SystemState{ Action::GC, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }
      break;

    // State machine for index resizing.
    case Action::GrowIndex:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::GrowIndex, Phase::GROW_PREPARE, version };
      case Phase::GROW_PREPARE:
        return SystemState{ Action::GrowIndex, Phase::GROW_IN_PROGRESS, version };
      case Phase::GROW_IN_PROGRESS:
        return SystemState{ Action::GrowIndex, Phase::REST, version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }

    // State machine for migration source.
    case Action::MigrationSource:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::MigrationSource,
                            Phase::PREPARE_TO_SAMPLE,
                            version };
      case Phase::PREPARE_TO_SAMPLE:
        return SystemState{ Action::MigrationSource,
                            Phase::SAMPLING,
                            version };
      case Phase::SAMPLING:
        return SystemState{ Action::MigrationSource,
                            Phase::PREPARE_FOR_TRANSFER,
                            version };
      case Phase::PREPARE_FOR_TRANSFER:
        // Move FASTER to a new version.
        return SystemState{ Action::MigrationSource,
                            Phase::TRANSFERING_OWNERSHIP,
                            version };
      case Phase::TRANSFERING_OWNERSHIP:
        return SystemState{ Action::MigrationSource,
                            Phase::WAIT_FOR_PENDING,
                            version };
      case Phase::WAIT_FOR_PENDING:
        return SystemState{ Action::MigrationSource,
                            Phase::MOVE_DATA_TO_TARGET,
                            version };
      case Phase::MOVE_DATA_TO_TARGET:
        return SystemState{ Action::MigrationSource,
                            Phase::REST,
                            version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }

    // State machine for migration target.
    case Action::MigrationTarget:
      switch(phase) {
      case Phase::REST:
        return SystemState{ Action::MigrationTarget,
                            Phase::PREPARE_FOR_RECEIVE,
                            version };
      case Phase::PREPARE_FOR_RECEIVE:
        return SystemState{ Action::MigrationTarget,
                            Phase::RECV_DATA_FROM_TARGET,
                            version };
      case Phase::RECV_DATA_FROM_TARGET:
        return SystemState{ Action::MigrationTarget,
                            Phase::REST,
                            version };
      default:
        // not reached
        assert(false);
        return SystemState(UINT64_MAX);
      }

    // Never reached.
    default:
      assert(false);
      return SystemState(UINT64_MAX);
    }
  }

  union {
      struct {
        /// Action being performed (checkpoint, recover, or gc).
        Action action;
        /// Phase of that action currently being executed.
        Phase phase;
        /// Checkpoint version (used for CPR).
        uint32_t version;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(SystemState) == 8, "sizeof(SystemState) != 8");

class AtomicSystemState {
 public:
  AtomicSystemState(Action action_, Phase phase_, uint32_t version_) {
    SystemState state{ action_, phase_, version_ };
    control_.store(state.control_);
  }

  /// Atomically loads current system state.
  inline SystemState load() const {
    return SystemState{ control_.load() };
  }

  /// Atomically stores a value into system state.
  inline void store(SystemState value) {
    control_.store(value.control_);
  }

  inline bool compare_exchange_strong(SystemState& expected, SystemState desired) {
    uint64_t expected_control = expected.control_;
    bool result = control_.compare_exchange_strong(expected_control, desired.control_);
    expected = SystemState{ expected_control };
    return result;
  }

  /// Returns the current phase of the system.
  inline Phase phase() const {
    return load().phase;
  }

  /// Returns the current version of the system.
  inline uint32_t version() const {
    return load().version;
  }

 private:
  /// Atomic access to the system state.
  std::atomic<uint64_t> control_;
};

}
} // namespace FASTER::core
