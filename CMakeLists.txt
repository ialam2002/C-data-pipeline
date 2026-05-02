cmake_minimum_required(VERSION 3.20)

if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

project(distributed_kv_store VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)

FetchContent_Declare(
    asio
    URL https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-30-2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(asio)

add_executable(dkv_node
    src/main.cpp
    src/server/tcp_server.cpp
    src/cluster/peer_client.cpp
    src/protocol/command_parser.cpp
    src/protocol/raft_rpc.cpp
    src/storage/kv_store.cpp
    src/storage/log_store.cpp
    src/storage/persistent_state.cpp
    src/consensus/raft_node.cpp
)

target_include_directories(dkv_node PRIVATE
    src
    ${asio_SOURCE_DIR}/asio/include
)

target_compile_definitions(dkv_node PRIVATE ASIO_STANDALONE)

if(WIN32)
    target_compile_definitions(dkv_node PRIVATE _WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN)
    target_link_libraries(dkv_node PRIVATE ws2_32 mswsock)
endif()

if(MSVC)
    target_compile_options(dkv_node PRIVATE /W4 /permissive-)
else()
    target_compile_options(dkv_node PRIVATE -Wall -Wextra -Wpedantic)
endif()