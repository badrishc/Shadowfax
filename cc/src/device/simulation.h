// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <tuple>
#include <chrono>

#include "core/guid.h"
#include "core/async.h"
#include "core/status.h"
#include "core/gc_state.h"
#include "environment/file.h"

#include "tbb/concurrent_priority_queue.h"

namespace FASTER {
namespace device {

using namespace tbb;
using namespace core;
using namespace std::chrono;

/// The following are template arguments.
///    - L: The latency of 4KB reads issued to files using this handler in
///         microseconds.
///    - T: The throughput of writes issued to files using this handler
///         in MBps.
template<uint32_t L, uint32_t T>
class SimulationHandler {
 public:
  /// Typedefs for convenience. Define the type signature on an io request
  /// enqueued on this handler.
  typedef time_point<std::chrono::high_resolution_clock> stamp;
  typedef std::tuple<stamp, IAsyncContext*, AsyncIOCallback> io_request_t;

  /// Returns an simulator for an IO handler. The queue has an initial capacity
  /// of 128 IO requests.
  SimulationHandler()
   : ioQueue(128)
  {}

  /// Issues/Enqueues an IO request of size 'size' bytes along with an
  /// associated callback and context. 'read' indicates if the IO request
  /// is for a read or a write.
  void issueIO(IAsyncContext& ctxt, AsyncIOCallback cb,
               uint32_t size, bool read)
  {
    // Add latency based on type of operation. Assuming reads are random,
    // and that writes are sequential.
    auto complete = high_resolution_clock::now();
    uint64_t time = read ? (size / 4096) * L : (size * 1e6) / (T << 20);
    complete += microseconds(time);

    // Enqueue the IO request after deep copying the context onto the heap.
    IAsyncContext* copy = nullptr;
    ctxt.DeepCopy(copy);

    io_request_t io = std::make_tuple(complete, copy, cb);
    ioQueue.push(io);
  }

  /// Tries to complete IOs queued up on this handler. Returns true if some
  /// pending IOs were able to complete when this method was invoked.
  bool TryComplete() {
    // First, check if we have an pending IOs to handle. If we don't then just
    // return immediately. Return false indicating that nothing completed.
    auto numIOs = ioQueue.size();
    if (numIOs == 0) return false;

    // Iterate through the priority queue looking for completed IOs. Since we're
    // sorted on completion time (lesser completion time has higher priority), we
    // can stop the moment we reach an IO request that has not completed yet.
    bool completed = false;
    for(; numIOs > 0; numIOs--) {
      io_request_t io;
      if (!ioQueue.try_pop(io)) break;

      if (high_resolution_clock::now() < std::get<0>(io)) {
        ioQueue.push(io);
        break;
      }

      // This IO request has completed. Invoke the callback with the context.
      // Set completed to true because at least one IO completed.
      auto ctxt = std::get<1>(io);
      auto func = std::get<2>(io);
      func(ctxt, Status::Ok, 0);
      completed = true;
    }

    // Return flag indicating whether any IOs completed during this invocation.
    return completed;
  }

 private:
  /// An IO request has a lower priority if it has a higher completion
  /// time-stamp.
  struct Priority {
    bool operator() (const io_request_t& l, const io_request_t& r) {
      return std::get<0>(l) > std::get<0>(r);
    }
  };

  /// The queue over which IO requests are buffered. This is a priority queue
  /// sorted by the time stamp at which a request is supposed to complete.
  concurrent_priority_queue<io_request_t, Priority> ioQueue;
};

/// A simulated file that asynchronous reads and writes can be issued against.
///    - L: The latency of 4KB reads issued to files under this disk in
///         microseconds.
///    - T: The throughput of writes issued to files under this disk
///         in MBps.
template<uint32_t L, uint32_t T>
class SimulationFile {
 public:
  /// For convenience. Defines the type on the IO handler used by this file.
  typedef SimulationHandler<L, T> handler_t;

  /// Constructors and assignment operators don't really do anything special.

  SimulationFile()
   : ioHandler(nullptr)
  {}

  SimulationFile(const std::string& filename, const environment::FileOptions& o)
   : SimulationFile()
  {}

  SimulationFile(SimulationFile&& other)
   : ioHandler(other.ioHandler)
  {}

  SimulationFile& operator=(SimulationFile&& other) {
    ioHandler = other.ioHandler;
  }

  /// Basic file operations to open, close, delete, and truncate the file.
  /// Mostly no-op, uninteresting methods because we're just a simulation.

  Status Open(handler_t* handler) {
    ioHandler = handler;
    return Status::Ok;
  }

  Status Close() { return Status::Ok; }

  Status Delete() { return Status::Ok; }

  void Truncate(uint64_t begin, GcState::truncate_callback_t cb) {
    cb(begin);
  }

  /// Asynchronously simulates a read of 'length' bytes from
  /// 'source' address. Invokes the passed in callback with the
  /// context when this read IO completes.
  Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                   AsyncIOCallback cb, IAsyncContext& ctxt) const
  {
    // Issue a read IO (rounded to 4KB) to the IO handler. The
    // 'dest' pointer might not point to enough memory after
    // rounding, but this does not matter since we're just a
    // simulation.
    if (length & (4096 - 1)) length = (length + 4096 >> 12) << 12;
    ioHandler->issueIO(ctxt, cb, length, true);

    return Status::Ok;
  }

  /// Asynchronously simulates a write of 'length' bytes from 'source'
  /// into address 'dest'. Invokes the passed in callback with the
  /// context when this write IO completes.
  Status WriteAsync(const void* source, uint64_t dest,
                    uint32_t length, AsyncIOCallback cb,
                    IAsyncContext& ctxt)
  {
    ioHandler->issueIO(ctxt, cb, length, false);
    return Status::Ok;
  }

  /// Assume that we're aligned to 512 bytes for now.
  size_t alignment() const { return 512; }

 private:
  /// Pointer to handler on which this file enqueues all of it's IO requests.
  handler_t* ioHandler;
};

/// The following are template arguments.
///    - L: The latency of 4KB reads issued to files under this disk in
///         microseconds.
///    - T: The throughput of writes issued to files under this disk
///         in MBps.
template<uint32_t L, uint32_t T>
class SimulationDisk {
 public:
  /// Typedefs for convenience. Define the type on the IO handler, file, and
  /// hybrid log used by this device.
  typedef SimulationHandler<L, T> handler_t;
  typedef SimulationFile<L, T> file_t;
  typedef SimulationFile<L, T> log_file_t;

  /// Empty constructors. For parameter documentation, please refer to
  /// FileSystemDisk under 'src/device/file_system_disk.h'.
  SimulationDisk()
   : ioHandler()
   , hLog()
  {}

  SimulationDisk(const std::string& path, LightEpoch& epoch)
   : SimulationDisk()
  {}

  /// Returns the disk's sector size in bytes (assumed to be 512 for now).
  static uint32_t sector_size() {
    return 512;
  }

  /// Returns a reference to the hybrid log stored under this disk (const
  /// version of the method)
  const file_t& log() const {
    return hLog;
  }

  /// Returns a reference to the hybrid log stored under this disk (non-const
  /// version of the method)
  file_t& log() {
    return hLog;
  }

  /// The following are four methods to retrieve the path under which index and
  /// cpr checkpoints are stored. Since we are a simulation, we just return an
  /// empty path which should throw an error when the caller tries to open it.

  std::string relative_index_checkpoint_path(const Guid& token) const {
    return "";
  }

  std::string index_checkpoint_path(const Guid& token) const {
    return "";
  }

  std::string relative_cpr_checkpoint_path(const Guid& token) const {
    return "";
  }

  std::string cpr_checkpoint_path(const Guid& token) const {
    return "";
  }

  /// Creates a directory to store checkpoints of the hash table.
  /// Since we're just a simulation, this method currently does not do anything.
  void CreateIndexCheckpointDirectory(const Guid& token) {}

  /// Creates a directory to store cpr checkpoints.
  /// Since we're just a simulation, this method currently does not do anything.
  void CreateCprCheckpointDirectory(const Guid& token) {}

  /// Creates and returns a new file. This file will not be open, and will not
  /// be able to process reads & writes until its Open() has been called.
  file_t NewFile(const std::string& relative_path) {
    return file_t();
  }

  /// Returns a reference to the IO handler used by the device.
  handler_t& handler() {
    return ioHandler;
  }

  /// Tries to complete IO requests issued to files on the device.
  inline bool TryComplete() {
    return ioHandler.TryComplete();
  }

 private:
  /// The handler on which all IO requests will be enqueued/dequeued. This
  /// is where the actual simulation will happen.
  handler_t ioHandler;

  /// The file under which the hybrid log will be stored.
  log_file_t hLog;
};

} // end of namespace device
} // end of namespace FASTER
