#include "consensus/raft_node.h"

#include <algorithm>
#include <sstream>

namespace dkv {

RaftNode::RaftNode(std::string nodeId, KvStore& kvStore, LogStore& logStore)
    : nodeId_(std::move(nodeId)), kvStore_(kvStore), logStore_(logStore) {}

void RaftNode::start() {
    std::scoped_lock lock(mutex_);
    role_ = NodeRole::Leader;
    leaderId_ = nodeId_;
    currentTerm_ = 1;
    votedFor_ = nodeId_;
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
        std::scoped_lock lock(mutex_);

        if (role_ != NodeRole::Leader) {
            return leaderId_.empty() ? "ERROR not_leader" : "REDIRECT " + leaderId_;
        }

        LogEntry entry {
            .index = 0,
            .term = currentTerm_,
            .command = command,
        };
        entry.index = logStore_.append(entry);
        kvStore_.apply(entry);
        return "OK";
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

            kvStore_.apply(*request.entry);
            matchIndex = request.entry->index;
        }
    }

    commitIndex_ = std::min(request.leaderCommit, logStore_.lastIndex());

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
           << " leader=" << (leaderId_.empty() ? "unknown" : leaderId_)
           << " voted_for=" << (votedFor_.has_value() ? *votedFor_ : "none")
           << " last_log_index=" << logStore_.lastIndex()
           << " last_log_term=" << logStore_.lastTerm()
           << " keys=" << kvStore_.size();
    return stream.str();
}

bool RaftNode::isCandidateLogUpToDate(std::uint64_t candidateLastLogIndex, std::uint64_t candidateLastLogTerm) const {
    const auto localLastTerm = logStore_.lastTerm();
    if (candidateLastLogTerm != localLastTerm) {
        return candidateLastLogTerm > localLastTerm;
    }

    return candidateLastLogIndex >= logStore_.lastIndex();
}

}  // namespace dkv