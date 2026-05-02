#include "storage/log_store.h"

namespace dkv {

std::uint64_t LogStore::append(LogEntry entry) {
    std::scoped_lock lock(mutex_);
    entry.index = entries_.empty() ? 1 : entries_.back().index + 1;
    entries_.push_back(std::move(entry));
    return entries_.back().index;
}

bool LogStore::appendReplicated(const LogEntry& entry) {
    std::scoped_lock lock(mutex_);
    const auto expectedIndex = entries_.empty() ? 1 : entries_.back().index + 1;
    if (entry.index != expectedIndex) {
        return false;
    }

    entries_.push_back(entry);
    return true;
}

std::optional<LogEntry> LogStore::get(std::uint64_t index) const {
    std::scoped_lock lock(mutex_);

    for (const auto& entry : entries_) {
        if (entry.index == index) {
            return entry;
        }
    }

    return std::nullopt;
}

std::vector<LogEntry> LogStore::readFrom(std::uint64_t index) const {
    std::scoped_lock lock(mutex_);
    std::vector<LogEntry> result;

    for (const auto& entry : entries_) {
        if (entry.index >= index) {
            result.push_back(entry);
        }
    }

    return result;
}

std::uint64_t LogStore::lastIndex() const {
    std::scoped_lock lock(mutex_);
    return entries_.empty() ? 0 : entries_.back().index;
}

std::uint64_t LogStore::lastTerm() const {
    std::scoped_lock lock(mutex_);
    return entries_.empty() ? 0 : entries_.back().term;
}

}  // namespace dkv