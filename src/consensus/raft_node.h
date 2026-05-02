#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "common/status.h"
#include "common/types.h"
#include "storage/kv_store.h"
#include "storage/log_store.h"

namespace dkv {

class RaftNode {
public:
    RaftNode(std::string nodeId, KvStore& kvStore, LogStore& logStore);

    void start();
    void becomeLeader();
    void becomeFollower(std::string leaderId, std::uint64_t term);

    std::string handleClientCommand(const ClientCommand& command);
    NodeRole role() const;
    std::uint64_t currentTerm() const;
    std::string info() const;

private:
    std::string nodeId_;
    KvStore& kvStore_;
    LogStore& logStore_;

    mutable std::mutex mutex_;
    NodeRole role_ {NodeRole::Follower};
    std::uint64_t currentTerm_ {0};
    std::string leaderId_;
};

}  // namespace dkv