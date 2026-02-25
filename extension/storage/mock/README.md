# Storage Mocks

Generated mocks for all `extension/storage` interfaces using [gomock](https://github.com/uber-go/mock).

Mocks are **not checked in** — they are generated at build time by the Bazel `gomock` rule.

## Adding a new store interface

When a new store interface file is added to `extension/storage/`:

1. Add a `//go:generate` directive to the new file:
   ```go
   //go:generate mockgen -source=new_store.go -destination=mock/new_store.go -package=mock
   ```

2. Add the file to `exports_files` in `extension/storage/BUILD.bazel`.

3. Add a new `gomock` rule in `extension/storage/mock/BUILD.bazel`:
   ```starlark
   gomock(
       name = "mock_new_store_src",
       out = "new_store_mock.go",
       mockgen_tool = _MOCKGEN,
       package = "mock",
       source = "//extension/storage:new_store.go",
       source_importpath = "github.com/uber/submitqueue/extension/storage",
   )
   ```

4. Add the new rule target to the `go_library` srcs in the same file:
   ```starlark
   go_library(
       name = "mock",
       srcs = [
           ...
           ":mock_new_store_src",
       ],
       ...
   )
   ```

> **Note:** This BUILD.bazel uses `# gazelle:ignore`, so gazelle will not update it automatically.
