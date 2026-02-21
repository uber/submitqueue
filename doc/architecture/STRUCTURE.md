# Project Structure

This document describes the structure of the submitqueue project, which follows the same Bazel and proto organization as the tango repository.

## Directory Layout

```
submitqueue/
├── .bazelversion               # Pins Bazel version to 8.4.1
├── .envrc                      # direnv configuration
├── MODULE.bazel                # Bzlmod dependency management
├── go.mod                      # Go module with YARPC dependencies
├── Makefile                    # Build automation
├── BUILD.bazel                 # Root build file
│
├── tool/                       # Bazel tooling
│   ├── bazel                   # Python-based Bazelisk wrapper
│   ├── BUILD.bazel
│   └── README.md
│
├── gateway/                    # Gateway service
│   ├── BUILD.bazel
│   ├── core/
│   │   └── controller/
│   │       ├── BUILD.bazel
│   │       └── ping.go         # Service implementation
│   ├── proto/
│   │   ├── BUILD.bazel
│   │   └── gateway.proto       # Proto definition
│   └── protopb/                # Generated proto files
│       ├── BUILD.bazel
│       ├── gateway.pb.go       # Protobuf generated code
│       ├── gateway_grpc.pb.go  # gRPC generated code
│       └── gateway.pb.yarpc.go # YARPC generated code
│
├── orchestrator/               # Orchestrator service
│   ├── BUILD.bazel
│   ├── core/
│   │   └── controller/
│   │       ├── BUILD.bazel
│   │       └── ping.go
│   ├── proto/
│   │   ├── BUILD.bazel
│   │   └── orchestrator.proto
│   └── protopb/
│       ├── BUILD.bazel
│       ├── orchestrator.pb.go
│       ├── orchestrator_grpc.pb.go
│       └── orchestrator.pb.yarpc.go
│
├── speculator/                 # Speculator service
│   ├── BUILD.bazel
│   ├── core/
│   │   └── controller/
│   │       ├── BUILD.bazel
│   │       └── ping.go
│   ├── proto/
│   │   ├── BUILD.bazel
│   │   └── speculator.proto
│   └── protopb/
│       ├── BUILD.bazel
│       ├── speculator.pb.go
│       ├── speculator_grpc.pb.go
│       └── speculator.pb.yarpc.go
│
└── example/                    # Examples (like tango/example)
    ├── README.md
    ├── server/                 # Server examples
    │   ├── gateway/
    │   ├── orchestrator/
    │   └── speculator/
    └── client/                 # Client examples
        ├── gateway/
        ├── orchestrator/
        └── speculator/
```

## Key Design Principles

This structure follows the tango repository's conventions:

### 1. **Separate `proto/` and `protopb/` Directories**

Each service has:
- `proto/` - Contains the `.proto` file(s)
- `protopb/` - Contains all generated files (`.pb.go`, `_grpc.pb.go`, `.pb.yarpc.go`)
- `core/controller/` - Contains service implementation

This separation makes it clear what is source vs. generated, and all generated files are committed to the repository.

### 2. **YARPC Support**

All proto files generate three types of files:
- `*.pb.go` - Standard protobuf code
- `*_grpc.pb.go` - gRPC service code
- `*.pb.yarpc.go` - YARPC service code for Uber's RPC framework

This allows services to support both gRPC and YARPC clients.

### 3. **Python-Based Bazel Wrapper**

The `tool/bazel` script is a Python implementation of Bazelisk that:
- Reads `.bazelversion` to determine which Bazel version to use
- Downloads and caches the appropriate Bazel binary
- Delegates to the correct version automatically

### 4. **Committed Generated Files**

All `*pb/` generated files are committed to the repository because:
- This is a library that will be consumed by other services
- Consumers can import and use the proto packages without needing protoc
- Ensures consistent generated code across builds

## Comparison with Tango

| Aspect | Tango | Submit Queue |
|--------|-------|--------------|
| Proto location | `proto/` (root) | `<service>/proto/` |
| Generated files | `tangopb/` | `<service>/protopb/` |
| Bazel tool | Python script | Python script (copied) |
| Dependency mgmt | Bzlmod | Bzlmod |
| YARPC | Yes | Yes |
| Generated committed | Yes | Yes |
| Examples dir | `example/` | `example/server/` and `example/client/` |
| Bazel config | No `.bazelrc` | No `.bazelrc` |
