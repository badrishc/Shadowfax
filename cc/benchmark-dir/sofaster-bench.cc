// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <thread>
#include <chrono>
#include <iostream>

#include "type.h"
#include "util.h"

#include "server/contexts.h"

#include "core/guid.h"
#include "core/async.h"
#include "core/auto_ptr.h"
#include "core/faster.h"
#include "core/key_hash.h"
#include "core/migration.h"

#include "environment/file.h"
#include "device/file_system_disk.h"

#include "boost/program_options.hpp"

using namespace SoFASTER;
using namespace FASTER::core;
using namespace boost::program_options;

/// The number of keys to be filled by this client into SoFASTER (initialized
/// to 250 Million), and an array to load them into.
uint64_t initKCount = 250 * 1000 * 1000;
aligned_unique_ptr_t<uint64_t> fillKeys;

/// An index into the next chunk of keys that a thread can pick up and issue
/// requests against, and the number of keys in each chunk. The index is into
/// "fillKeys" while running the target benchmark.
std::atomic<uint64_t> idx(0);
constexpr uint64_t kChunkSize = 3200;

/// Defines the type on the IO handler used by FASTER to handle asynchronous
/// reads and writes issued to records that lie below the head of the log.
typedef FASTER::environment::QueueIoHandler handler_t;

/// Defines the type on the disk used by FASTER to store the log below the
/// head address.
typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;

/// Defines the type on FASTER.
typedef FASTER::core::FasterKv<Key, Value, disk_t> faster_t;

/// Benchmarks the target's migration datapath logic on a single thread pinned
/// to a core.
void threadTarget(faster_t* faster, size_t core) {
  // Pin this thread and open a session to FASTER.
  pinThread(core);
  Guid id = faster->StartSession();

  // Stats for the benchmark.
  uint64_t serial = 0;
  auto s = std::chrono::high_resolution_clock::now();

  // As long as there are keys to insert, fetch a chunk of them from the global
  // list. For each key in the fetched chunk, issue an upsert to the server.
  for (uint64_t chunkIdx = idx.fetch_add(kChunkSize); chunkIdx < initKCount;
       chunkIdx = idx.fetch_add(kChunkSize))
  {
    for (uint64_t i = chunkIdx; i < chunkIdx + kChunkSize; i++) {
      // Allocate contexts for the Upsert.
      Key key(fillKeys.get()[i]);
      Value v(41);
      server::UpsertContext<Key, Value, Value, void> ctxt(key, v, nullptr, 0);
      auto callback = [](FASTER::core::IAsyncContext* ctxt,
                       FASTER::core::Status result)
      {
        // Need to construct this class so that the heap allocated
        // context (ctxt) is freed correctly.
        CallbackContext<server::UpsertContext<Key, Value, Value, void>> c(ctxt);
      };

      if (faster->Upsert(ctxt, callback, ++serial) != Status::Ok) {
        printf("Upsert failed on thread %lu\n!", core);
        exit(1);
      }
    }
  }

  auto f = std::chrono::high_resolution_clock::now();
  std::chrono::nanoseconds d = f - s;
  printf("Target thread %lu throughput: %.2f Mbps\n",
         core, ((double) serial * 1e3 * (sizeof(Key) + sizeof(Value))) /
               (d.count() * 1.024 * 1.024));

  // End session to FASTER.
  faster->StopSession();
}

/// Benchmarks the target's migration datapath logic on multiple threads.
void benchmarkTarget(faster_t* faster, size_t threads) {
  std::vector<std::thread> fThreads;
  for (size_t i = 0; i < threads; i++) {
    fThreads.emplace_back(threadTarget, faster, i);
  }

  for (size_t i = 0; i < threads; i++) {
    fThreads[i].join();
  }
}

/// An atomic flag to indicate to threads when they can start stressing the
/// source migration datapath.
std::atomic<bool> start(false);

/// Benchmarks the source's migration datapath logic on a single thread pinned
/// to a core.
void threadSource(faster_t* faster, size_t core) {
  // Pin this thread and open a session to FASTER.
  pinThread(core);
  Guid id = faster->StartSession();

  uint8_t buffer[4096];
  auto m = MigrationState::MigrationContext();

  while (!start);

  while (true) {
    auto size = faster->Collect(reinterpret_cast<void*>(&m), buffer, 4096);
    if (!size) break;
  }

  // End session to FASTER.
  faster->StopSession();
}

/// Benchmarks the source's migration datapath logic on multiple threads.
void benchmarkSource(faster_t* faster, size_t threads) {
  std::vector<std::thread> cThreads;
  for (size_t i = 0; i < threads; i++) {
    cThreads.emplace_back(threadSource, faster, i);
  }

  std::this_thread::sleep_for(1s);

  // Issue a migration to this instance of FASTER.
  Guid guid = faster->StartSession();
  auto cCtx = malloc(sizeof(uint64_t));
  auto cP = [](void* ctxt, KeyHash l, KeyHash r, std::string& ip,
               uint16_t p, bool prep) {};
  auto cntrl = std::make_tuple(cCtx, cP);
  auto oCtx = malloc(sizeof(uint64_t));
  auto ownershipCb = [](void* ctxt) {};
  auto ownerTfr = std::make_tuple(oCtx, ownershipCb);
  auto ctxt = malloc(sizeof(uint64_t));
  auto dataMovtStart = [](void* ctxt, KeyHash l, KeyHash r,
                          std::string& ip, uint16_t port) {};
  auto dataMovtCease = [](void* ctxt) {};
  auto dataMovt = std::make_tuple(ctxt, dataMovtStart, dataMovtCease);
  if (!faster->Migrate(KeyHash(0UL), KeyHash(~0UL), true, ownerTfr, dataMovt,
                       cntrl, "", 22790))
  {
    printf("Failed to kickoff source migration datapath!\n");
    exit(1);
  }
  faster->StopSession();

  start = true;

  for (size_t i = 0; i < threads; i++) {
    cThreads[i].join();
  }
}

int main(int argc, char* argv[]) {
  // The set of supported command line options.
  options_description desc("Allowed options");
  desc.add_options()
    ("help", "Produce a help message and exit")
    ("threads", value<size_t>()->default_value(1),
       "Number of client threads to be spawned")
    ("nKeys", value<uint64_t>()->default_value(250 * 1000 * 1000),
       "Number of keys to be loaded into SoFASTER")
    ("loadFile", value<std::string>()->default_value("/data/load_ycsb"),
       "File containing keys to be loaded into SoFASTER")
    ("htSizeM", value<uint64_t>()->default_value(128),
       "The number of entries to be created on the hash table in millions")
    ("logSizeGB", value<uint64_t>()->default_value(16),
       "The size of the in-memory log in GB")
    ("logDisk", value<std::string>()->default_value("/scratch/storage"),
       "The path under which the log should be stored on disk")
    ("onlyTarget", value<bool>()->default_value(false),
       "If true, benchmark only the target and exit")
  ;

  // Parse command line options into a variable map.
  variables_map vm;
  store(parse_command_line(argc, argv, desc), vm);
  notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  // Retrieve the parsed options from the variable map.
  size_t threads = vm["threads"].as<size_t>();
  initKCount = vm["nKeys"].as<uint64_t>();
  std::string loadFile = vm["loadFile"].as<std::string>();
  uint64_t htSize = vm["htSizeM"].as<uint64_t>() << 20;
  uint64_t logSize = vm["logSizeGB"].as<uint64_t>() << 30;
  std::string logDisk = vm["logDisk"].as<std::string>();
  bool onlyTarget = vm["onlyTarget"].as<bool>();

  printf("Loading workload data into memory......\n");
  aligned_unique_ptr_t<uint64_t> txnsKeys(nullptr);
  load(loadFile, initKCount, fillKeys, "", 0, txnsKeys);

  // Create the FASTER key-value store.
  faster_t faster(htSize, logSize, logDisk);

  printf("Benchmarking target datapath...........\n");
  benchmarkTarget(&faster, threads);
  if (onlyTarget) return 0;

  printf("Benchmarking source datapath...........\n");
  benchmarkSource(&faster, threads);

  return 0;
}
