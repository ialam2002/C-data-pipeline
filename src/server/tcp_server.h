#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "common/types.h"

namespace dkv {

class RaftNode;


class TcpServer {
public:
    TcpServer(Endpoint endpoint, RaftNode& raftNode);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void stop();
    bool isRunning() const;
    std::string address() const;

private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace dkv