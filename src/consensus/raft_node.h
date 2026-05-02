#pragma once

#include <cstdint>
#include <chrono>
#include <random>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cluster/peer_client.h"
#include "common/status.h"
#include "common/types.h"
#include "storage/kv_store.h"
#include "storage/log_store.h"
#include "storage/persistent_state.h"

namespace dkv {

class RaftNode {
public:
    RaftNode(std::string nodeId, KvStore& kvStore, LogStore& logStore, PersistentStateStore& persistentStateStore, std::vector<Endpoint> peers = {});
    ~RaftNode();

    void start(bool bootstrapLeader = true);
    void stop();
    void becomeLeader();
    void becomeFollower(std::string leaderId, std::uint64_t term);

    std::string handleClientCommand(const ClientCommand& command);
    RequestVoteResponse handleRequestVote(const RequestVoteRequest& request);
    AppendEntriesResponse handleAppendEntries(const AppendEntriesRequest& request);
    NodeRole role() const;
    std::uint64_t currentTerm() const;
    std::string info() const;

private:
    void runCoordinationLoop(std::stop_token stopToken);
    void runElection();
    bool replicateToQuorum(const LogEntry& entry);
    void broadcastHeartbeat();
    void applyCommittedEntriesUnlocked();
    void persistStateUnlocked() const;
    void resetElectionDeadlineUnlocked();
    bool isCandidateLogUpToDate(std::uint64_t candidateLastLogIndex, std::uint64_t candidateLastLogTerm) const;

    std::string nodeId_;
    KvStore& kvStore_;
    LogStore& logStore_;
    PersistentStateStore& persistentStateStore_;
    std::vector<Endpoint> peers_;
    PeerClient peerClient_;

    mutable std::mutex mutex_;
    NodeRole role_ {NodeRole::Follower};
    std::uint64_t currentTerm_ {0};
    std::uint64_t commitIndex_ {0};
    std::uint64_t lastApplied_ {0};
    std::string leaderId_;
    std::optional<std::string> votedFor_;
    std::jthread heartbeatThread_;
    std::mt19937 randomEngine_;
    std::chrono::steady_clock::time_point electionDeadline_;
};

}  // namespace dkv