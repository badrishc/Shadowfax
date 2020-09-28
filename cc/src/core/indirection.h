// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "record.h"

namespace FASTER {
namespace core {

/// An indirection record that points to records whose ownership was migrated
/// to this FASTER instance, but which were physically retained on the source's
/// log to avoid random IO. Contains all information required to retrieve the
/// record during a read or rmw.
struct alignas(Constants::kCacheLineBytes) IndirectionRecord {
  /// Generic record header. The `indirection` bit on this record identifies
  /// it as an indirection record requiring a remote request to a different
  /// machine/server's dfs log.
  RecordInfo header;

  /// Partial hash of this record. When collecting records for migration at
  /// the source, we don't have the key of any records on DFS. Just the
  /// bucket and few other bits of it's hash. We need these bits to insert
  /// the indirection record at the correct entry on the target.
  KeyHash partialHash;

  /// The size of the source's hash table when this record was migrated.
  /// Required because the target might have a larger hash table in which
  /// case we will need to insert this record into multiple entries.
  uint64_t htSize;

  /// Hash range that this indirection record belongs to. When this record was
  /// migrated over, we did not have it's complete hash. During a look up at
  /// the target we need this range to determine whether we must follow this
  /// record down to DFS or not.
  std::tuple<KeyHash, KeyHash> hashRange;

  /// The address of the actual record this indirection record points to.
  /// Contains a 16 bit identifier of the DFS log the record is on as well
  /// as it's 48 bit offset within that log.
  union {
    struct {
      uint64_t logicalAddr : 48;
      uint64_t dfsLogId : 16;
    };

    uint64_t address;
  };

  /// Constructs an indirection record appropriately setting the header bit.
  /// Refer to member variables above for documentation on the arguments.
  IndirectionRecord(KeyHash partialH, KeyHash firstH, KeyHash finalH,
                    uint16_t dfs, Address addr, uint64_t srcSize,
                    Address previous=Address(0))
   : header(0, true, false, false, previous)
   , partialHash(partialH)
   , htSize(srcSize)
   , hashRange(firstH, finalH)
   , dfsLogId(dfs)
   , logicalAddr(addr.control())
  {
    header.indirection = 1;
  }

  /// Returns the size of an indirection record.
  inline static size_t size() {
    return sizeof(IndirectionRecord);
  }
};
static_assert(sizeof(IndirectionRecord) == Constants::kCacheLineBytes,
             "sizeof(IndirectionRecord) != Constants::kCacheLineBytes");

} // namespace core
} // namespace FASTER
