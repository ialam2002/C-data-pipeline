#include "storage/kv_store.h"

namespace dkv {

std::optional<std::string> KvStore::get(const std::string& key) const {
    std::shared_lock lock(mutex_);
    const auto iterator = data_.find(key);
    if (iterator == data_.end()) {
        return std::nullopt;
    }

    return iterator->second;
}

void KvStore::put(const std::string& key, const std::string& value) {
    std::unique_lock lock(mutex_);
    data_[key] = value;
}

bool KvStore::remove(const std::string& key) {
    std::unique_lock lock(mutex_);
    return data_.erase(key) > 0;
}

void KvStore::apply(const LogEntry& entry) {
    switch (entry.command.type) {
    case CommandType::Put:
        put(entry.command.key, entry.command.value);
        break;
    case CommandType::Delete:
        remove(entry.command.key);
        break;
    default:
        break;
    }
}

std::size_t KvStore::size() const {
    std::shared_lock lock(mutex_);
    return data_.size();
}

}  // namespace dkv