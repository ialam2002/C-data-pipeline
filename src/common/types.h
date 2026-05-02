#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace dkv {

enum class NodeRole {
    Follower,
    Candidate,
    Leader,
};

enum class CommandType {
    Ping,
    Get,
    Put,
    Delete,
    Info,
    Role,
    Invalid,
};

struct Endpoint {
    std::string host {"127.0.0.1"};
    std::uint16_t port {7000};
};

struct ClientCommand {
    CommandType type {CommandType::Invalid};
    std::string key;
    std::string value;
};

struct LogEntry {
    std::uint64_t index {0};
    std::uint64_t term {0};
    ClientCommand command {};
};

struct RequestVoteRequest {
    std::uint64_t term {0};
    std::string candidateId;
    std::uint64_t lastLogIndex {0};
    std::uint64_t lastLogTerm {0};
};

struct RequestVoteResponse {
    std::uint64_t term {0};
    bool voteGranted {false};
};

struct AppendEntriesRequest {
    std::uint64_t term {0};
    std::string leaderId;
    std::uint64_t prevLogIndex {0};
    std::uint64_t prevLogTerm {0};
    std::uint64_t leaderCommit {0};
    std::optional<LogEntry> entry;
};

struct AppendEntriesResponse {
    std::uint64_t term {0};
    bool success {false};
    std::uint64_t matchIndex {0};
};

using RaftRpcMessage = std::variant<RequestVoteRequest, AppendEntriesRequest>;

inline std::string toString(NodeRole role) {
    switch (role) {
    case NodeRole::Follower:
        return "follower";
    case NodeRole::Candidate:
        return "candidate";
    case NodeRole::Leader:
        return "leader";
    }

    return "unknown";
}

inline std::string toString(bool value) {
    return value ? "1" : "0";
}

}  // namespace dkv