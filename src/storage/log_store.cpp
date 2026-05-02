#include "storage/log_store.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace dkv {

LogStore::LogStore(std::string filePath)
    : filePath_(std::move(filePath)) {
    loadFromDisk();
}

std::uint64_t LogStore::append(LogEntry entry) {
    std::scoped_lock lock(mutex_);
    entry.index = entries_.empty() ? 1 : entries_.back().index + 1;

    if (!persistEntryUnlocked(entry)) {
        throw std::runtime_error("failed to persist log entry");
    }

    entries_.push_back(std::move(entry));
    return entries_.back().index;
}

bool LogStore::appendReplicated(const LogEntry& entry) {
    std::scoped_lock lock(mutex_);
    const auto expectedIndex = entries_.empty() ? 1 : entries_.back().index + 1;
    if (entry.index != expectedIndex) {
        return false;
    }

    if (!persistEntryUnlocked(entry)) {
        throw std::runtime_error("failed to persist replicated log entry");
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

void LogStore::loadFromDisk() {
    if (filePath_.empty() || !std::filesystem::exists(filePath_)) {
        return;
    }

    std::ifstream input(filePath_);
    if (!input.is_open()) {
        throw std::runtime_error("failed to open log file: " + filePath_);
    }

    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }

        std::istringstream stream(line);
        LogEntry entry;
        std::string operation;
        std::string key;
        std::string value;

        if (!(stream >> entry.index >> entry.term >> operation >> std::quoted(key))) {
            throw std::runtime_error("failed to parse log record: " + line);
        }

        if (operation == "PUT") {
            if (!(stream >> std::quoted(value))) {
                throw std::runtime_error("failed to parse PUT log record: " + line);
            }

            entry.command = ClientCommand {
                .type = CommandType::Put,
                .key = key,
                .value = value,
            };
        } else if (operation == "DELETE") {
            entry.command = ClientCommand {
                .type = CommandType::Delete,
                .key = key,
            };
        } else {
            throw std::runtime_error("unknown log operation in record: " + line);
        }

        entries_.push_back(std::move(entry));
    }
}

bool LogStore::persistEntryUnlocked(const LogEntry& entry) const {
    if (filePath_.empty()) {
        return true;
    }

    std::filesystem::create_directories(std::filesystem::path(filePath_).parent_path());
    std::ofstream output(filePath_, std::ios::app);
    if (!output.is_open()) {
        return false;
    }

    const auto operation = entry.command.type == CommandType::Put ? "PUT" : "DELETE";
    output << entry.index << ' '
           << entry.term << ' '
           << operation << ' '
           << std::quoted(entry.command.key);

    if (entry.command.type == CommandType::Put) {
        output << ' ' << std::quoted(entry.command.value);
    }

    output << '\n';
    return output.good();
}

}  // namespace dkv