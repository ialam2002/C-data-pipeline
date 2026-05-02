#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace dkv {

struct PersistentStateData {
    std::uint64_t currentTerm {0};
    std::uint64_t commitIndex {0};
    std::optional<std::string> votedFor;
};

class PersistentStateStore {
public:
    explicit PersistentStateStore(std::string filePath = {});

    PersistentStateData load() const;
    void save(const PersistentStateData& state) const;

private:
    std::string filePath_;
};

}  // namespace dkv