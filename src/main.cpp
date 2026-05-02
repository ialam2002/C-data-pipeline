#include <iostream>
#include <future>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#include "consensus/raft_node.h"
#include "protocol/command_parser.h"
#include "server/tcp_server.h"
#include "storage/kv_store.h"
#include "storage/log_store.h"

int main(int argc, char* argv[]) {
    dkv::Endpoint endpoint {"127.0.0.1", 7000};
    if (argc >= 2) {
        try {
            endpoint.port = static_cast<std::uint16_t>(std::stoul(argv[1]));
        } catch (const std::exception&) {
            std::cerr << "Invalid port: " << argv[1] << '\n';
            return 1;
        }
    }

    dkv::KvStore kvStore;
    dkv::LogStore logStore;
    dkv::RaftNode raftNode("node-" + std::to_string(endpoint.port), kvStore, logStore);
    dkv::TcpServer server(endpoint, raftNode);
    dkv::CommandParser parser;

    try {
        server.start();
    } catch (const std::exception& error) {
        std::cerr << "Failed to start TCP server: " << error.what() << '\n';
        return 1;
    }

    raftNode.start();

    std::cout << "dkv_node started at " << server.address() << '\n';
    std::cout << "TCP clients can send newline-delimited commands." << '\n';
    std::cout << "Local console commands are also enabled. Enter QUIT to exit." << '\n';

#ifdef _WIN32
    const bool interactiveInput = _isatty(_fileno(stdin)) != 0;
#else
    const bool interactiveInput = isatty(fileno(stdin)) != 0;
#endif

    if (!interactiveInput) {
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

    server.stop();
    return 0;
}