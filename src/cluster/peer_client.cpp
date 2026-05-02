#include "cluster/peer_client.h"

#include <asio.hpp>
#include <chrono>
#include <optional>
#include <sstream>
#include <string>

#include "protocol/raft_rpc.h"

namespace dkv {

namespace {

std::optional<std::string> sendLineRequest(const Endpoint& endpoint, const std::string& payload) {
    try {
        asio::io_context ioContext;
        asio::ip::tcp::resolver resolver(ioContext);
        asio::ip::tcp::socket socket(ioContext);

        auto endpoints = resolver.resolve(endpoint.host, std::to_string(endpoint.port));
        asio::connect(socket, endpoints);

        asio::write(socket, asio::buffer(payload));

        asio::streambuf responseBuffer;
        asio::read_until(socket, responseBuffer, '\n');

        std::istream responseStream(&responseBuffer);
        std::string line;
        std::getline(responseStream, line);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        return line;
    } catch (...) {
        return std::nullopt;
    }
}

}  // namespace

PeerClient::PeerClient(std::chrono::milliseconds timeout)
    : timeout_(timeout) {}

std::optional<RequestVoteResponse> PeerClient::sendRequestVote(const Endpoint& endpoint, const RequestVoteRequest& request) const {
    (void)timeout_;
    const auto response = sendLineRequest(endpoint, RaftRpcParser::serialize(request) + "\n");
    if (!response.has_value()) {
        return std::nullopt;
    }

    RaftRpcParser parser;
    return parser.parseRequestVoteResponse(*response);
}

std::optional<AppendEntriesResponse> PeerClient::sendAppendEntries(const Endpoint& endpoint, const AppendEntriesRequest& request) const {
    (void)timeout_;
    const auto response = sendLineRequest(endpoint, RaftRpcParser::serialize(request) + "\n");
    if (!response.has_value()) {
        return std::nullopt;
    }

    RaftRpcParser parser;
    return parser.parseAppendEntriesResponse(*response);
}

}  // namespace dkv