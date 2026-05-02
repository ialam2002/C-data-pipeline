#include "protocol/command_parser.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <vector>

namespace dkv {

namespace {

std::string uppercase(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return value;
}

std::vector<std::string> split(const std::string& input) {
    std::istringstream stream(input);
    std::vector<std::string> tokens;
    std::string token;

    while (stream >> token) {
        tokens.push_back(token);
    }

    return tokens;
}

}  // namespace

std::optional<ClientCommand> CommandParser::parse(const std::string& input) const {
    const auto tokens = split(input);
    if (tokens.empty()) {
        return std::nullopt;
    }

    const auto command = uppercase(tokens.front());

    if (command == "PING") {
        return ClientCommand {.type = CommandType::Ping};
    }

    if (command == "INFO") {
        return ClientCommand {.type = CommandType::Info};
    }

    if (command == "ROLE") {
        return ClientCommand {.type = CommandType::Role};
    }

    if (command == "GET" && tokens.size() == 2) {
        return ClientCommand {.type = CommandType::Get, .key = tokens[1]};
    }

    if (command == "DELETE" && tokens.size() == 2) {
        return ClientCommand {.type = CommandType::Delete, .key = tokens[1]};
    }

    if (command == "PUT" && tokens.size() >= 3) {
        std::string value;
        for (std::size_t index = 2; index < tokens.size(); ++index) {
            if (!value.empty()) {
                value += ' ';
            }
            value += tokens[index];
        }

        return ClientCommand {.type = CommandType::Put, .key = tokens[1], .value = value};
    }

    return std::nullopt;
}

}  // namespace dkv