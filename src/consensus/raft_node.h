#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
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
    RequestVoteResponse handleRequestVote(const RequestVoteRequest& request);
    AppendEntriesResponse handleAppendEntries(const AppendEntriesRequest& request);
    NodeRole role() const;
    std::uint64_t currentTerm() const;
    std::string info() const;

private:
    bool isCandidateLogUpToDate(std::uint64_t candidateLastLogIndex, std::uint64_t candidateLastLogTerm) const;

    std::string nodeId_;
    KvStore& kvStore_;
    LogStore& logStore_;

    mutable std::mutex mutex_;
    NodeRole role_ {NodeRole::Follower};
    std::uint64_t currentTerm_ {0};
    std::uint64_t commitIndex_ {0};
    std::string leaderId_;
    std::optional<std::string> votedFor_;
};

}  // namespace dkv