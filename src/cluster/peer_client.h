#pragma once

#include <chrono>
#include <optional>

#include "common/types.h"

namespace dkv {

class PeerClient {
public:
    explicit PeerClient(std::chrono::milliseconds timeout = std::chrono::milliseconds(750));

    std::optional<AppendEntriesResponse> sendAppendEntries(const Endpoint& endpoint, const AppendEntriesRequest& request) const;

private:
    std::chrono::milliseconds timeout_;
};

}  // namespace dkv