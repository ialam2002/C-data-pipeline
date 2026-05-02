#include "storage/persistent_state.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace dkv {

PersistentStateStore::PersistentStateStore(std::string filePath)
    : filePath_(std::move(filePath)) {}

PersistentStateData PersistentStateStore::load() const {
    if (filePath_.empty() || !std::filesystem::exists(filePath_)) {
        return {};
    }

    std::ifstream input(filePath_);
    if (!input.is_open()) {
        throw std::runtime_error("failed to open persistent state file: " + filePath_);
    }

    PersistentStateData state;
    std::string votedFor;
    if (!(input >> state.currentTerm >> state.commitIndex >> std::quoted(votedFor))) {
        throw std::runtime_error("failed to parse persistent state file: " + filePath_);
    }

    if (!votedFor.empty()) {
        state.votedFor = votedFor;
    }

    return state;
}

void PersistentStateStore::save(const PersistentStateData& state) const {
    if (filePath_.empty()) {
        return;
    }

    std::filesystem::create_directories(std::filesystem::path(filePath_).parent_path());
    std::ofstream output(filePath_, std::ios::trunc);
    if (!output.is_open()) {
        throw std::runtime_error("failed to write persistent state file: " + filePath_);
    }

    output << state.currentTerm << ' '
           << state.commitIndex << ' '
           << std::quoted(state.votedFor.value_or(""))
           << '\n';
}

}  // namespace dkv