#pragma once

#include <optional>
#include <string>

#include "common/types.h"

namespace dkv {

class CommandParser {
public:
    std::optional<ClientCommand> parse(const std::string& input) const;
};

}  // namespace dkv