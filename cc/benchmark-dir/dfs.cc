#include "tbb/concurrent_queue.h"

#include "core/faster.h"
#include "device/azure.h"

using namespace FASTER::device;

/// Skeleton context required to issue writes and reads.
class FContext : public IAsyncContext {
 public:
  FContext() {}

  /// Copy (and deep-copy) constructor.
  FContext(const FContext& other) {}

  /// The implicit and explicit interfaces require a key() accessor.
  inline const int& key() const {
    return _t;
  }

  inline void Get(const int& value) {}

  inline void GetAtomic(const int& value) {}

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  int _t;
};

/// Measures blobfile write latency.
void writeLatency(BlobFile& remote,
                  tbb::concurrent_queue<BlobFile::Task>& pending)
{
  #define MB 1024 * 1024
  /// Write sizes to benchmark against.
  unsigned long sizes[4] = {4 * MB, 2 * MB, 1 * MB, 512 * 1024};
  #undef MB

  /// Skeleton callback required for when async writes and reads complete.
  auto acallback = [](IAsyncContext* context, Status result, size_t n) {
    return;
  };

  /// Context required to process completed reads and writes.
  FContext ctxt;

  /// Run experiment for different write sizes.
  for (auto kWriteL : sizes) {
    /// Vector to store latency samples.
    std::vector<long long> writes;

    /// Measure write latency.
    for (uint32_t i = 0; i < 1000; i++) {
      std::vector<uint8_t> data;
      data.resize(kWriteL, 145);

      auto start = std::chrono::high_resolution_clock::now();

      auto r = remote.WriteAsync(&(data[0]), kWriteL, i * kWriteL,
                                 acallback, ctxt);
      if (r != Status::Ok) {
        printf("Write to blob store failed... Exiting\n");
        return;
      }

      /// Wait for the write to complete.
      BlobFile::Task task;
      pending.try_pop(task);
      task.wait();

      auto ended = std::chrono::high_resolution_clock::now();
      writes.push_back((ended - start).count());
    }

    std::sort(writes.begin(), writes.end());
    auto medianIndex = writes.size() / 2;
    auto tailIndex = (writes.size() * 99) / 100;
    printf("%7lu B Median, Tail write latency: %8.2f, %8.2f us\n",
      kWriteL,
      (double)(writes[medianIndex]) / 1000,
      (double)(writes[tailIndex]) / 1000);
  }
}

/// Measures blobfile read latency.
void readLatency(BlobFile& remote,
                 tbb::concurrent_queue<BlobFile::Task>& pending)
{
  /// Read sizes to benchmark
  unsigned long sizes[3] = {512, 1024, 10240};

  /// Skeleton callback required for when async writes and reads complete.
  auto acallback = [](IAsyncContext* context, Status result, size_t n) {
    return;
  };

  /// Context required to process completed reads and writes.
  FContext ctxt;

  /// Benchmark read latency.
  std::vector<uint8_t> data;
  data.resize(10240, 0);

  for (auto kReadsL : sizes) {
    /// Vector to store latency samples.
    std::vector<long long> reads;

    for (uint32_t i = 0; i < 10000; i++) {
      auto start = std::chrono::high_resolution_clock::now();

      auto r = remote.ReadAsync(i * 4 * 1024 * 1024, kReadsL, &(data[0]),
                                acallback, ctxt);
      if (r != Status::Ok) {
        printf("Read to blob store failed... Exiting\n");
        return;
      }

      /// Wait for the read to complete.
      BlobFile::Task task;
      pending.try_pop(task);
      task.wait();

      auto ended = std::chrono::high_resolution_clock::now();
      reads.push_back((ended - start).count());
    }

    std::sort(reads.begin(), reads.end());
    auto medianIndex = reads.size() / 2;
    auto tailIndex = (reads.size() * 99) / 100;
    printf("%7lu B Median, Tail read latency: %8.2f, %8.2f us\n",
           kReadsL,
          (double)(reads[medianIndex]) / 1000,
          (double)(reads[tailIndex]) / 1000);
  }
}

/// Measures write throughput of blob store.
void writeThroughput(BlobFile& remote,
                     tbb::concurrent_queue<BlobFile::Task>& pending)
{
  #define MB 1024 * 1024
  unsigned long sizes[4] = {4 * MB, 2 * MB, 1 * MB, 512 * 1024};
  #undef MB

  /// Vary the size of each write.
  for (auto size : sizes) {
    std::vector<uint8_t> data;
    data.resize(size);

    /// Vary degree of parallelism.
    const long unsigned numAsyncs[4] = { 1, 2, 4, 8};

    for (auto numAsync : numAsyncs) {
      const long unsigned numIters = (1024 * 1024 * 1024) / (size * numAsync);

      /// Skeleton callback required for when async writes and reads complete.
      auto acallback = [](IAsyncContext* context, Status result, size_t n) {
        return;
      };

      /// Context required to process completed reads and writes.
      FContext ctxt;

      auto start = std::chrono::high_resolution_clock::now();

      /// One run of the experiment.
      for (uint32_t i = 0; i < numIters; i++) {
        for (uint32_t j = 0; j < numAsync; j++) {
          auto r = remote.WriteAsync(&data[0], size, j * size,
                                     acallback, ctxt);
          if (r != Status::Ok) {
            printf("Write to blob failed... Exiting.\n");
            return;
          }
        }

        while (pending.unsafe_size() != 0) {
          BlobFile::Task task;
          pending.try_pop(task);
          task.wait();
        }
      }

      auto ended = std::chrono::high_resolution_clock::now();

      /// Calculate throughput.
      unsigned long bytes = size * numIters * numAsync;
      long long nano = (ended - start).count();
      double throupt = ((double)bytes) / nano;

      printf("%7lu B %lu asyncs %lu bytes %llu ns Write Throughput: %.2f MB/s\n",
             size,
             numAsync,
             bytes,
             nano,
             (throupt / (1024 * 1024)) * 1e9);
    }
  }
}

/// Benchmarks write and read latency of BlobFile.
void benchmarkBlob(const std::string conn)
{
  /// Create and open a blob file.
  tbb::concurrent_queue<BlobFile::Task> pending;
  BlobFile remote(conn.c_str(), pending, 1, (uint64_t)1 << 30, false);
  remote.Open();

  /// Benchmark Latency.
  writeLatency(remote, pending);
  readLatency(remote, pending);

  /// Benchmark Throughput.
  writeThroughput(remote, pending);

  /// Delete all blobs and close the remote file.
  remote.Delete();
  remote.Close();
}

int main(void) {
  /// Connection string to talk to Azure blob storage.
  std::string conn = std::string("DefaultEndpointsProtocol=https;") +
                      std::string("AccountName=chinmay;AccountKey=") +
                      std::string("sLoIUgjuIjTE9AnfQ4fkLgSUNkYi76n") +
                      std::string("6w0FlA+5esbUb8xm4xPVkXiKw52+U1v") +
                      std::string("LA4XHQQD0q8xW9isYL14xshw==;") +
                      std::string("EndpointSuffix=core.windows.net");

  benchmarkBlob(conn);
}
