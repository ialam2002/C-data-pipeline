# Distributed Key-Value Store

Starter C++20 project for a portfolio-grade distributed key-value store.

## Current Scope

- CMake-based build
- newline-delimited TCP server built on standalone Asio
- command parsing for a Redis-like CLI surface
- in-memory key-value state machine
- append-only in-memory log abstraction
- first-pass Raft RPC handling for RequestVote and AppendEntries
- one-command-per-line client protocol over TCP

## Build

```powershell
cmake -S . -B build
cmake --build build
```

## Run

```powershell
.\build\Debug\dkv_node.exe
```

On non-MSVC generators, the executable path may differ.

If `cmake` is still not on PATH in the current terminal, use:

```powershell
& "C:\Program Files\CMake\bin\cmake.exe" -S . -B build
& "C:\Program Files\CMake\bin\cmake.exe" --build build
```

## Sample Commands

```text
PING
PUT user:1 alice
GET user:1
DELETE user:1
INFO
ROLE
QUIT
```

## Raft RPC Commands

RequestVote:

```text
RAFT REQUEST_VOTE <term> <candidateId> <lastLogIndex> <lastLogTerm>
```

Response:

```text
RAFT REQUEST_VOTE_RESPONSE <term> <voteGranted>
```

AppendEntries heartbeat:

```text
RAFT APPEND_ENTRIES <term> <leaderId> <prevLogIndex> <prevLogTerm> <leaderCommit> NONE
```

AppendEntries with one replicated entry:

```text
RAFT APPEND_ENTRIES <term> <leaderId> <prevLogIndex> <prevLogTerm> <leaderCommit> PUT <entryTerm> <entryIndex> <key> <value...>
RAFT APPEND_ENTRIES <term> <leaderId> <prevLogIndex> <prevLogTerm> <leaderCommit> DELETE <entryTerm> <entryIndex> <key>
```

Response:

```text
RAFT APPEND_ENTRIES_RESPONSE <term> <success> <matchIndex>
```

## Quick TCP Test

```powershell
$client = [System.Net.Sockets.TcpClient]::new("127.0.0.1", 7000)
$stream = $client.GetStream()
$writer = [System.IO.StreamWriter]::new($stream)
$reader = [System.IO.StreamReader]::new($stream)
$writer.AutoFlush = $true
$writer.WriteLine("PING")
$reader.ReadLine()
```

## Suggested Next Steps

1. Persist the Raft term, vote, and log to disk.
2. Add outbound peer communication and leader heartbeat scheduling.
3. Add multi-node integration tests.
4. Split client and cluster RPC protocols.

## Layout

```text
src/
  common/      core types and status helpers
  protocol/    client command parsing
  storage/     key-value state and log abstractions
  consensus/   Raft coordination skeleton
  server/      Asio-based TCP transport
```