# @submitqueue/api

Private Protobuf-ES bindings for the gateway protos, committed with a drift test. Two consumers, like Go's `protopb` serving both server and tests: the reference host's gateway client and the end-to-end test. The reusable packages never import it; they accept any client that matches their structural gateway contract.

```bash
make web-proto                                            # regenerate after changing .proto files
./tool/bazel test //web/api:generate_test //web/api:test
```
