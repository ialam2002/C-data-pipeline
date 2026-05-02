#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "common/types.h"

namespace dkv {

class KvStore {
public:
    std::optional<std::string> get(const std::string& key) const;
    void put(const std::string& key, const std::string& value);
    bool remove(const std::string& key);
    void apply(const LogEntry& entry);
    std::size_t size() const;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> data_;
};

}  // namespace dkv