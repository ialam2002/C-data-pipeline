#include "consensus/raft_node.h"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>

namespace dkv {

RaftNode::RaftNode(std::string nodeId, KvStore& kvStore, LogStore& logStore, std::vector<Endpoint> peers)
    : nodeId_(std::move(nodeId)),
      kvStore_(kvStore),
      logStore_(logStore),
      peers_(std::move(peers)) {}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start(bool bootstrapLeader) {
    std::scoped_lock lock(mutex_);

    if (bootstrapLeader) {
        role_ = NodeRole::Leader;
        leaderId_ = nodeId_;
        currentTerm_ = std::max<std::uint64_t>(currentTerm_, 1);
        votedFor_ = nodeId_;
    } else {
        role_ = NodeRole::Follower;
        leaderId_.clear();
        votedFor_.reset();
    }

    if (!heartbeatThread_.joinable()) {
        heartbeatThread_ = std::jthread([this](std::stop_token stopToken) {
            runHeartbeatLoop(stopToken);
        });
    }
}

void RaftNode::stop() {
    if (heartbeatThread_.joinable()) {
        heartbeatThread_.request_stop();
        heartbeatThread_.join();
    }
}

void RaftNode::becomeLeader() {
    std::scoped_lock lock(mutex_);
    role_ = NodeRole::Leader;
    leaderId_ = nodeId_;
    ++currentTerm_;
    votedFor_ = nodeId_;
}

void RaftNode::becomeFollower(std::string leaderId, std::uint64_t term) {
    std::scoped_lock lock(mutex_);
    role_ = NodeRole::Follower;
    leaderId_ = std::move(leaderId);
    currentTerm_ = term;
    votedFor_.reset();
}

std::string RaftNode::handleClientCommand(const ClientCommand& command) {
    if (command.type == CommandType::Ping) {
        return "PONG";
    }

    if (command.type == CommandType::Info) {
        return info();
    }

    if (command.type == CommandType::Role) {
        return toString(role());
    }

    if (role() != NodeRole::Leader &&
        (command.type == CommandType::Put || command.type == CommandType::Delete)) {
        return leaderId_.empty() ? "ERROR not_leader" : "REDIRECT " + leaderId_;
    }

    if (command.type == CommandType::Get) {
        const auto value = kvStore_.get(command.key);
        return value.has_value() ? "VALUE " + *value : "NOT_FOUND";
    }

    if (command.type == CommandType::Put || command.type == CommandType::Delete) {
        LogEntry entry;

        {
            std::scoped_lock lock(mutex_);

            if (role_ != NodeRole::Leader) {
                return leaderId_.empty() ? "ERROR not_leader" : "REDIRECT " + leaderId_;
            }

            entry = LogEntry {
                .index = 0,
                .term = currentTerm_,
                .command = command,
            };
            entry.index = logStore_.append(entry);
        }

        return replicateToQuorum(entry) ? "OK" : "ERROR quorum_unavailable";
    }

    return "ERROR invalid_command";
}

RequestVoteResponse RaftNode::handleRequestVote(const RequestVoteRequest& request) {
    std::scoped_lock lock(mutex_);

    if (request.term < currentTerm_) {
        return RequestVoteResponse {.term = currentTerm_, .voteGranted = false};
    }

    if (request.term > currentTerm_) {
        currentTerm_ = request.term;
        role_ = NodeRole::Follower;
        leaderId_.clear();
        votedFor_.reset();
    }

    const bool hasVoteAvailable = !votedFor_.has_value() || *votedFor_ == request.candidateId;
    const bool logUpToDate = isCandidateLogUpToDate(request.lastLogIndex, request.lastLogTerm);

    if (hasVoteAvailable && logUpToDate) {
        votedFor_ = request.candidateId;
        return RequestVoteResponse {.term = currentTerm_, .voteGranted = true};
    }

    return RequestVoteResponse {.term = currentTerm_, .voteGranted = false};
}

AppendEntriesResponse RaftNode::handleAppendEntries(const AppendEntriesRequest& request) {
    std::scoped_lock lock(mutex_);

    if (request.term < currentTerm_) {
        return AppendEntriesResponse {
            .term = currentTerm_,
            .success = false,
            .matchIndex = logStore_.lastIndex(),
        };
    }

    if (request.term > currentTerm_) {
        currentTerm_ = request.term;
        votedFor_.reset();
    }

    role_ = NodeRole::Follower;
    leaderId_ = request.leaderId;

    if (request.prevLogIndex > 0) {
        const auto previousEntry = logStore_.get(request.prevLogIndex);
        if (!previousEntry.has_value() || previousEntry->term != request.prevLogTerm) {
            return AppendEntriesResponse {
                .term = currentTerm_,
                .success = false,
                .matchIndex = logStore_.lastIndex(),
            };
        }
    }

    std::uint64_t matchIndex = request.prevLogIndex;

    if (request.entry.has_value()) {
        const auto existingEntry = logStore_.get(request.entry->index);
        if (existingEntry.has_value()) {
            if (existingEntry->term != request.entry->term) {
                return AppendEntriesResponse {
                    .term = currentTerm_,
                    .success = false,
                    .matchIndex = logStore_.lastIndex(),
                };
            }

            matchIndex = existingEntry->index;
        } else {
            if (!logStore_.appendReplicated(*request.entry)) {
                return AppendEntriesResponse {
                    .term = currentTerm_,
                    .success = false,
                    .matchIndex = logStore_.lastIndex(),
                };
            }

            matchIndex = request.entry->index;
        }
    }

    commitIndex_ = std::min(request.leaderCommit, logStore_.lastIndex());
    applyCommittedEntriesUnlocked();

    return AppendEntriesResponse {
        .term = currentTerm_,
        .success = true,
        .matchIndex = matchIndex,
    };
}

NodeRole RaftNode::role() const {
    std::scoped_lock lock(mutex_);
    return role_;
}

std::uint64_t RaftNode::currentTerm() const {
    std::scoped_lock lock(mutex_);
    return currentTerm_;
}

std::string RaftNode::info() const {
    std::scoped_lock lock(mutex_);

    std::ostringstream stream;
    stream << "node_id=" << nodeId_
           << " role=" << toString(role_)
           << " term=" << currentTerm_
           << " commit_index=" << commitIndex_
           << " last_applied=" << lastApplied_
           << " leader=" << (leaderId_.empty() ? "unknown" : leaderId_)
           << " voted_for=" << (votedFor_.has_value() ? *votedFor_ : "none")
           << " last_log_index=" << logStore_.lastIndex()
           << " last_log_term=" << logStore_.lastTerm()
           << " peers=" << peers_.size()
           << " keys=" << kvStore_.size();
    return stream.str();
}

void RaftNode::runHeartbeatLoop(std::stop_token stopToken) {
    while (!stopToken.stop_requested()) {
        if (role() == NodeRole::Leader && !peers_.empty()) {
            broadcastHeartbeat();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }
}

bool RaftNode::replicateToQuorum(const LogEntry& entry) {
    std::uint64_t term = 0;
    std::uint64_t leaderCommit = 0;
    std::uint64_t prevLogIndex = 0;
    std::uint64_t prevLogTerm = 0;
    std::vector<Endpoint> peers;

    {
        std::scoped_lock lock(mutex_);
        if (role_ != NodeRole::Leader) {
            return false;
        }

        term = currentTerm_;
        leaderCommit = commitIndex_;
        peers = peers_;
    }

    if (entry.index > 1) {
        const auto previousEntry = logStore_.get(entry.index - 1);
        if (!previousEntry.has_value()) {
            return false;
        }
        prevLogIndex = previousEntry->index;
        prevLogTerm = previousEntry->term;
    }

    std::size_t successCount = 1;
    const std::size_t quorum = (peers.size() + 1) / 2 + 1;

    for (const auto& peer : peers) {
        const auto response = peerClient_.sendAppendEntries(peer, AppendEntriesRequest {
            .term = term,
            .leaderId = nodeId_,
            .prevLogIndex = prevLogIndex,
            .prevLogTerm = prevLogTerm,
            .leaderCommit = leaderCommit,
            .entry = entry,
        });

        if (!response.has_value()) {
            continue;
        }

        if (response->term > term) {
            becomeFollower(peer.host + ":" + std::to_string(peer.port), response->term);
            return false;
        }

        if (response->success) {
            ++successCount;
        }
    }

    if (successCount < quorum) {
        return false;
    }

    {
        std::scoped_lock lock(mutex_);
        commitIndex_ = std::max(commitIndex_, entry.index);
        applyCommittedEntriesUnlocked();
    }

    broadcastHeartbeat();
    return true;
}

void RaftNode::broadcastHeartbeat() {
    std::uint64_t term = 0;
    std::uint64_t leaderCommit = 0;
    std::uint64_t prevLogIndex = 0;
    std::uint64_t prevLogTerm = 0;
    std::vector<Endpoint> peers;

    {
        std::scoped_lock lock(mutex_);
        if (role_ != NodeRole::Leader) {
            return;
        }

        term = currentTerm_;
        leaderCommit = commitIndex_;
        peers = peers_;
    }

    prevLogIndex = logStore_.lastIndex();
    prevLogTerm = logStore_.lastTerm();

    for (const auto& peer : peers) {
        const auto response = peerClient_.sendAppendEntries(peer, AppendEntriesRequest {
            .term = term,
            .leaderId = nodeId_,
            .prevLogIndex = prevLogIndex,
            .prevLogTerm = prevLogTerm,
            .leaderCommit = leaderCommit,
            .entry = std::nullopt,
        });

        if (response.has_value() && response->term > term) {
            becomeFollower(peer.host + ":" + std::to_string(peer.port), response->term);
            return;
        }
    }
}

void RaftNode::applyCommittedEntriesUnlocked() {
    const auto entriesToApply = logStore_.readFrom(lastApplied_ + 1);
    for (const auto& entry : entriesToApply) {
        if (entry.index > commitIndex_) {
            break;
        }

        kvStore_.apply(entry);
        lastApplied_ = entry.index;
    }
}

bool RaftNode::isCandidateLogUpToDate(std::uint64_t candidateLastLogIndex, std::uint64_t candidateLastLogTerm) const {
    const auto localLastTerm = logStore_.lastTerm();
    if (candidateLastLogTerm != localLastTerm) {
        return candidateLastLogTerm > localLastTerm;
    }

    return candidateLastLogIndex >= logStore_.lastIndex();
}

}  // namespace dkv