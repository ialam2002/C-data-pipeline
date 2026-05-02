#pragma once

#include <optional>
#include <string>

#include "common/types.h"

namespace dkv {

class RaftRpcParser {
public:
    std::optional<RaftRpcMessage> parse(const std::string& input) const;

    static std::string serialize(const RequestVoteResponse& response);
    static std::string serialize(const AppendEntriesResponse& response);
};

}  // namespace dkv