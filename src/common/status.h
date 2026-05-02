#pragma once

#include <string>

namespace dkv {

enum class StatusCode {
    Ok,
    InvalidArgument,
    NotFound,
    NotLeader,
    InternalError,
};

class Status {
public:
    Status() = default;
    Status(StatusCode code, std::string message)
        : code_(code), message_(std::move(message)) {}

    static Status ok() {
        return Status(StatusCode::Ok, "OK");
    }

    bool isOk() const {
        return code_ == StatusCode::Ok;
    }

    StatusCode code() const {
        return code_;
    }

    const std::string& message() const {
        return message_;
    }

private:
    StatusCode code_ {StatusCode::Ok};
    std::string message_ {"OK"};
};

}  // namespace dkv