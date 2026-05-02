#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

#include "common/types.h"

namespace dkv {

class LogStore {
public:
    std::uint64_t append(LogEntry entry);
    bool appendReplicated(const LogEntry& entry);
    std::optional<LogEntry> get(std::uint64_t index) const;
    std::vector<LogEntry> readFrom(std::uint64_t index) const;
    std::uint64_t lastIndex() const;
    std::uint64_t lastTerm() const;

private:
    mutable std::mutex mutex_;
    std::vector<LogEntry> entries_;
};

}  // namespace dkv