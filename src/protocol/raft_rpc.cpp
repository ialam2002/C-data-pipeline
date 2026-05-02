#include "protocol/raft_rpc.h"

#include <sstream>
#include <vector>

namespace dkv {

namespace {

std::vector<std::string> split(const std::string& input) {
    std::istringstream stream(input);
    std::vector<std::string> tokens;
    std::string token;

    while (stream >> token) {
        tokens.push_back(token);
    }

    return tokens;
}

std::optional<std::uint64_t> parseUnsigned(const std::string& token) {
    try {
        return std::stoull(token);
    } catch (...) {
        return std::nullopt;
    }
}

}  // namespace

std::optional<RaftRpcMessage> RaftRpcParser::parse(const std::string& input) const {
    const auto tokens = split(input);
    if (tokens.size() < 2 || tokens[0] != "RAFT") {
        return std::nullopt;
    }

    if (tokens[1] == "REQUEST_VOTE" && tokens.size() == 6) {
        const auto term = parseUnsigned(tokens[2]);
        const auto lastLogIndex = parseUnsigned(tokens[4]);
        const auto lastLogTerm = parseUnsigned(tokens[5]);
        if (!term.has_value() || !lastLogIndex.has_value() || !lastLogTerm.has_value()) {
            return std::nullopt;
        }

        return RequestVoteRequest {
            .term = *term,
            .candidateId = tokens[3],
            .lastLogIndex = *lastLogIndex,
            .lastLogTerm = *lastLogTerm,
        };
    }

    if (tokens[1] == "APPEND_ENTRIES" && tokens.size() >= 7) {
        const auto term = parseUnsigned(tokens[2]);
        const auto prevLogIndex = parseUnsigned(tokens[4]);
        const auto prevLogTerm = parseUnsigned(tokens[5]);
        const auto leaderCommit = parseUnsigned(tokens[6]);
        if (!term.has_value() || !prevLogIndex.has_value() || !prevLogTerm.has_value() || !leaderCommit.has_value()) {
            return std::nullopt;
        }

        AppendEntriesRequest request {
            .term = *term,
            .leaderId = tokens[3],
            .prevLogIndex = *prevLogIndex,
            .prevLogTerm = *prevLogTerm,
            .leaderCommit = *leaderCommit,
        };

        if (tokens.size() == 8 && tokens[7] == "NONE") {
            return request;
        }

        if (tokens.size() >= 10 && tokens[7] == "PUT") {
            const auto entryTerm = parseUnsigned(tokens[8]);
            const auto entryIndex = parseUnsigned(tokens[9]);
            if (!entryTerm.has_value() || !entryIndex.has_value() || tokens.size() < 12) {
                return std::nullopt;
            }

            std::string value;
            for (std::size_t index = 11; index < tokens.size(); ++index) {
                if (!value.empty()) {
                    value += ' ';
                }
                value += tokens[index];
            }

            request.entry = LogEntry {
                .index = *entryIndex,
                .term = *entryTerm,
                .command = ClientCommand {
                    .type = CommandType::Put,
                    .key = tokens[10],
                    .value = value,
                },
            };
            return request;
        }

        if (tokens.size() == 11 && tokens[7] == "DELETE") {
            const auto entryTerm = parseUnsigned(tokens[8]);
            const auto entryIndex = parseUnsigned(tokens[9]);
            if (!entryTerm.has_value() || !entryIndex.has_value()) {
                return std::nullopt;
            }

            request.entry = LogEntry {
                .index = *entryIndex,
                .term = *entryTerm,
                .command = ClientCommand {
                    .type = CommandType::Delete,
                    .key = tokens[10],
                },
            };
            return request;
        }
    }

    return std::nullopt;
}

std::string RaftRpcParser::serialize(const RequestVoteResponse& response) {
    return "RAFT REQUEST_VOTE_RESPONSE " + std::to_string(response.term) + " " + toString(response.voteGranted);
}

std::string RaftRpcParser::serialize(const AppendEntriesResponse& response) {
    return "RAFT APPEND_ENTRIES_RESPONSE " + std::to_string(response.term) + " " + toString(response.success) + " " + std::to_string(response.matchIndex);
}

}  // namespace dkv