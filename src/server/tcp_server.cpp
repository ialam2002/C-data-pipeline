#include "server/tcp_server.h"

#include <array>
#include <asio.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#include "consensus/raft_node.h"
#include "protocol/command_parser.h"

namespace dkv {

namespace {

std::string normalizeLine(std::string line) {
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    return line;
}

}  // namespace

struct TcpServer::Impl {
    struct Session : public std::enable_shared_from_this<Session> {
        Session(asio::ip::tcp::socket socket, RaftNode& raftNode)
            : socket(std::move(socket)), raftNode(raftNode) {}

        void start() {
            readLoop();
        }

        void readLoop() {
            auto self = shared_from_this();
            socket.async_read_some(asio::buffer(buffer),
                [self](const std::error_code& error, std::size_t bytesTransferred) {
                    if (error) {
                        return;
                    }

                    self->incoming.append(self->buffer.data(), bytesTransferred);
                    self->processIncoming();
                    self->readLoop();
                });
        }

        void processIncoming() {
            std::size_t newline = incoming.find('\n');
            while (newline != std::string::npos) {
                auto line = normalizeLine(incoming.substr(0, newline));
                incoming.erase(0, newline + 1);

                if (!line.empty()) {
                    handleLine(line);
                }

                newline = incoming.find('\n');
            }
        }

        void handleLine(const std::string& line) {
            const auto command = parser.parse(line);
            const auto response = command.has_value()
                ? raftNode.handleClientCommand(*command)
                : std::string("ERROR parse_error");
            writeLine(response);
        }

        void writeLine(const std::string& line) {
            outgoing.emplace_back(line + "\n");
            if (outgoing.size() == 1) {
                writeNext();
            }
        }

        void writeNext() {
            auto self = shared_from_this();
            asio::async_write(socket, asio::buffer(outgoing.front()),
                [self](const std::error_code& error, std::size_t) {
                    if (error) {
                        return;
                    }

                    self->outgoing.pop_front();
                    if (!self->outgoing.empty()) {
                        self->writeNext();
                    }
                });
        }

        asio::ip::tcp::socket socket;
        RaftNode& raftNode;
        CommandParser parser;
        std::array<char, 4096> buffer {};
        std::string incoming;
        std::deque<std::string> outgoing;
    };

    Impl(Endpoint endpoint, RaftNode& raftNode)
        : endpoint(std::move(endpoint)),
          raftNode(raftNode),
          acceptor(ioContext) {}

    void start() {
        if (running.exchange(true)) {
            return;
        }

        std::error_code error;
        const auto address = asio::ip::make_address(endpoint.host, error);
        if (error) {
            running.store(false);
            throw std::runtime_error("invalid listen address: " + endpoint.host);
        }

        asio::ip::tcp::endpoint listenEndpoint(address, endpoint.port);

        acceptor.open(listenEndpoint.protocol(), error);
        if (error) {
            running.store(false);
            throw std::runtime_error("failed to open acceptor: " + error.message());
        }

        acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true), error);
        if (error) {
            running.store(false);
            throw std::runtime_error("failed to set reuse_address: " + error.message());
        }

        acceptor.bind(listenEndpoint, error);
        if (error) {
            running.store(false);
            throw std::runtime_error("failed to bind acceptor: " + error.message());
        }

        acceptor.listen(asio::socket_base::max_listen_connections, error);
        if (error) {
            running.store(false);
            throw std::runtime_error("failed to listen: " + error.message());
        }

        acceptLoop();
        ioThread = std::jthread([this]() {
            ioContext.run();
        });
    }

    void stop() {
        if (!running.exchange(false)) {
            return;
        }

        std::error_code ignored;
        acceptor.cancel(ignored);
        acceptor.close(ignored);
        ioContext.stop();

        if (ioThread.joinable()) {
            ioThread.join();
        }
    }

    bool isRunning() const {
        return running.load();
    }

    std::string address() const {
        return endpoint.host + ":" + std::to_string(endpoint.port);
    }

    void acceptLoop() {
        acceptor.async_accept([this](const std::error_code& error, asio::ip::tcp::socket socket) {
            if (!error) {
                std::make_shared<Session>(std::move(socket), raftNode)->start();
            } else if (running.load() && error != asio::error::operation_aborted) {
                std::cerr << "accept error: " << error.message() << '\n';
            }

            if (running.load()) {
                acceptLoop();
            }
        });
    }

    Endpoint endpoint;
    RaftNode& raftNode;
    asio::io_context ioContext;
    asio::ip::tcp::acceptor acceptor;
    std::jthread ioThread;
    std::atomic_bool running {false};
};

TcpServer::TcpServer(Endpoint endpoint, RaftNode& raftNode)
    : impl_(std::make_unique<Impl>(std::move(endpoint), raftNode)) {}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::start() {
    impl_->start();
}

void TcpServer::stop() {
    impl_->stop();
}

bool TcpServer::isRunning() const {
    return impl_->isRunning();
}

std::string TcpServer::address() const {
    return impl_->address();
}

}  // namespace dkv