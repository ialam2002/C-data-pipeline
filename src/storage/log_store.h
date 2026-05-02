#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/types.h"

namespace dkv {

class LogStore {
public:
    explicit LogStore(std::string filePath = {});

    std::uint64_t append(LogEntry entry);
    bool appendReplicated(const LogEntry& entry);
    std::optional<LogEntry> get(std::uint64_t index) const;
    std::vector<LogEntry> readFrom(std::uint64_t index) const;
    std::uint64_t lastIndex() const;
    std::uint64_t lastTerm() const;

private:
    void loadFromDisk();
    bool persistEntryUnlocked(const LogEntry& entry) const;

    std::string filePath_;
    mutable std::mutex mutex_;
    std::vector<LogEntry> entries_;
};

}  // namespace dkv