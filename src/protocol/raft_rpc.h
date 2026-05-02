#pragma once

#include <optional>
#include <string>

#include "common/types.h"

namespace dkv {

class RaftRpcParser {
public:
    std::optional<RaftRpcMessage> parse(const std::string& input) const;
    std::optional<RequestVoteResponse> parseRequestVoteResponse(const std::string& input) const;
    std::optional<AppendEntriesResponse> parseAppendEntriesResponse(const std::string& input) const;

    static std::string serialize(const RequestVoteRequest& request);
    static std::string serialize(const AppendEntriesRequest& request);
    static std::string serialize(const RequestVoteResponse& response);
    static std::string serialize(const AppendEntriesResponse& response);
};

}  // namespace dkv