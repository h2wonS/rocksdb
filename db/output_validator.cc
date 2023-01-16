//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/output_validator.h"

namespace ROCKSDB_NAMESPACE {

extern std::shared_ptr<Logger> _logger;

Status OutputValidator::Add(const Slice& key, const Slice& value) {
  if (enable_hash_) {
    // Generate a rolling 64-bit hash of the key and values
    paranoid_hash_ = NPHash64(key.data(), key.size(), paranoid_hash_);
    paranoid_hash_ = NPHash64(value.data(), value.size(), paranoid_hash_);
  }
  if (enable_order_check_) {
    TEST_SYNC_POINT_CALLBACK("OutputValidator::Add:order_check",
                             /*arg=*/nullptr);
    if (key.size() < kNumInternalBytes) {
      return Status::Corruption(
          "Compaction tries to write a key without internal bytes.");
    }

    // prev_key_ starts with empty.
    if (!prev_key_.empty() && icmp_.Compare(key, prev_key_) < 0) {

//      printf("Shit We need prev_key with empty, but this is fuck Prev(%d), Curr(%d)\n", 
//      prev_key_.size(), key.size());

//      ROCKS_LOG_INFO(_logger,"%dSST Shit We need prev_key with empty, but this is fuck Prev(%d), Curr(%d)", 
//      filenum, prev_key_.size(), key.size());


//      for(int i=0;i<=12;i++){
//        ROCKS_LOG_INFO(_logger,"%dSST PrevKey[%d]=%x", filenum, i, prev_key_.data()[i]);
//      }
//      for(int i=0;i<=12;i++){
//        ROCKS_LOG_INFO(_logger,"CurrentKey[%d]=%x", i, key.data()[i]);
//      }
//      printf(" prev_key is empty(%d), Compare w/ (key, prev_key) =%d\n",
//          prev_key_.empty(), icmp_.Compare(key, prev_key_));

      abort(); 
      return Status::Corruption("Compaction sees out-of-order keys.");
    }
    prev_key_.assign(key.data(), key.size());
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
