#include "consensus/raft_node.h"

#include <sstream>

namespace dkv {

RaftNode::RaftNode(std::string nodeId, KvStore& kvStore, LogStore& logStore)
    : nodeId_(std::move(nodeId)), kvStore_(kvStore), logStore_(logStore) {}

void RaftNode::start() {
    std::scoped_lock lock(mutex_);
    role_ = NodeRole::Leader;
    leaderId_ = nodeId_;
    currentTerm_ = 1;
}

void RaftNode::becomeLeader() {
    std::scoped_lock lock(mutex_);
    role_ = NodeRole::Leader;
    leaderId_ = nodeId_;
    ++currentTerm_;
}

void RaftNode::becomeFollower(std::string leaderId, std::uint64_t term) {
    std::scoped_lock lock(mutex_);
    role_ = NodeRole::Follower;
    leaderId_ = std::move(leaderId);
    currentTerm_ = term;
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
           << " leader=" << (leaderId_.empty() ? "unknown" : leaderId_)
           << " last_log_index=" << logStore_.lastIndex()
           << " keys=" << kvStore_.size();
    return stream.str();
}

}  // namespace dkv