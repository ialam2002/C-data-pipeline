#include "cluster/peer_client.h"

#include <asio.hpp>
#include <chrono>
#include <optional>
#include <sstream>
#include <string>

#include "protocol/raft_rpc.h"

namespace dkv {

PeerClient::PeerClient(std::chrono::milliseconds timeout)
    : timeout_(timeout) {}

std::optional<AppendEntriesResponse> PeerClient::sendAppendEntries(const Endpoint& endpoint, const AppendEntriesRequest& request) const {
    try {
        asio::io_context ioContext;
        asio::ip::tcp::resolver resolver(ioContext);
        asio::ip::tcp::socket socket(ioContext);

        auto endpoints = resolver.resolve(endpoint.host, std::to_string(endpoint.port));
        asio::connect(socket, endpoints);

        const auto payload = RaftRpcParser::serialize(request) + "\n";
        asio::write(socket, asio::buffer(payload));

        asio::streambuf responseBuffer;
        asio::read_until(socket, responseBuffer, '\n');

        std::istream responseStream(&responseBuffer);
        std::string line;
        std::getline(responseStream, line);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        RaftRpcParser parser;
        return parser.parseAppendEntriesResponse(line);
    } catch (...) {
        return std::nullopt;
    }
}

}  // namespace dkv