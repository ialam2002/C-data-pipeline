#pragma once

#include <cstdint>
#include <optional>
#include <string>
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

}  // namespace dkv