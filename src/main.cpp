#include <iostream>
#include <future>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#include "common/types.h"
#include "consensus/raft_node.h"
#include "protocol/command_parser.h"
#include "server/tcp_server.h"
#include "storage/kv_store.h"
#include "storage/log_store.h"

namespace {

std::optional<dkv::Endpoint> parseEndpoint(const std::string& value) {
    const auto separator = value.find(':');
    if (separator == std::string::npos || separator == 0 || separator == value.size() - 1) {
        return std::nullopt;
    }

    try {
        return dkv::Endpoint {
            .host = value.substr(0, separator),
            .port = static_cast<std::uint16_t>(std::stoul(value.substr(separator + 1))),
        };
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    dkv::Endpoint endpoint {"127.0.0.1", 7000};
    bool serverOnly = false;
    bool bootstrapLeader = true;
    bool bootstrapExplicit = false;
    std::vector<dkv::Endpoint> peers;

    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];

        if (argument == "--server-only") {
            serverOnly = true;
            continue;
        }

        if (argument == "--bootstrap-leader") {
            bootstrapLeader = true;
            bootstrapExplicit = true;
            continue;
        }

        if (argument == "--peer") {
            if (index + 1 >= argc) {
                std::cerr << "Missing value after --peer" << '\n';
                return 1;
            }

            const auto peer = parseEndpoint(argv[++index]);
            if (!peer.has_value()) {
                std::cerr << "Invalid peer endpoint: " << argv[index] << '\n';
                return 1;
            }

            peers.push_back(*peer);
            continue;
        }

        try {
            endpoint.port = static_cast<std::uint16_t>(std::stoul(argument));
        } catch (const std::exception&) {
            std::cerr << "Invalid argument: " << argument << '\n';
            std::cerr << "Usage: dkv_node [port] [--server-only] [--bootstrap-leader] [--peer host:port]" << '\n';
            return 1;
        }
    }

    if (!bootstrapExplicit && !peers.empty()) {
        bootstrapLeader = false;
    }

    dkv::KvStore kvStore;
    dkv::LogStore logStore;
    const std::string nodeId = endpoint.host + ":" + std::to_string(endpoint.port);
    dkv::RaftNode raftNode(nodeId, kvStore, logStore, peers);
    dkv::TcpServer server(endpoint, raftNode);
    dkv::CommandParser parser;

    try {
        server.start();
    } catch (const std::exception& error) {
        std::cerr << "Failed to start TCP server: " << error.what() << '\n';
        return 1;
    }

    raftNode.start(bootstrapLeader);

    std::cout << "dkv_node started at " << server.address() << '\n';
    std::cout << "TCP clients can send newline-delimited commands." << '\n';
    std::cout << "Local console commands are also enabled. Enter QUIT to exit." << '\n';

#ifdef _WIN32
    const bool interactiveInput = _isatty(_fileno(stdin)) != 0;
#else
    const bool interactiveInput = isatty(fileno(stdin)) != 0;
#endif

    if (serverOnly || !interactiveInput) {
        std::cout << "Running without interactive stdin. Stop the process with Ctrl+C." << '\n';
        std::promise<void>().get_future().wait();
        return 0;
    }

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "QUIT" || line == "EXIT") {
            break;
        }

        const auto command = parser.parse(line);
        if (!command.has_value()) {
            std::cout << "ERROR parse_error" << '\n';
            continue;
        }

        std::cout << raftNode.handleClientCommand(*command) << '\n';
    }

    raftNode.stop();
    server.stop();
    return 0;
}