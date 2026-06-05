"""Hermetic protobuf code generation helpers."""

def _strip_proto_suffix(filename):
    if not filename.endswith(".proto"):
        fail("expected a .proto source, got {}".format(filename))
    return filename[:-len(".proto")]

def _go_proto_generated_files_impl(ctx):
    src = ctx.file.src
    base = _strip_proto_suffix(src.basename)

    out_dir = ctx.attr.out_dir
    outputs = [
        ctx.actions.declare_file("{}/{}.pb.go".format(out_dir, base)),
        ctx.actions.declare_file("{}/{}.pb.yarpc.go".format(out_dir, base)),
        ctx.actions.declare_file("{}/{}_grpc.pb.go".format(out_dir, base)),
    ]

    proto_toolchain = ctx.toolchains["@rules_proto//proto:toolchain_type"].proto
    protoc = proto_toolchain.proto_compiler

    args = ctx.actions.args()
    for opt in proto_toolchain.protoc_opts:
        args.add(opt)
    args.add("--plugin=protoc-gen-go={}".format(ctx.executable._protoc_gen_go.path))
    args.add("--plugin=protoc-gen-go-grpc={}".format(ctx.executable._protoc_gen_go_grpc.path))
    args.add("--plugin=protoc-gen-yarpc-go-v2={}".format(ctx.executable._protoc_gen_yarpc_go.path))
    args.add("--go_out={}".format(outputs[0].dirname))
    args.add("--go_opt=paths=source_relative")
    args.add("--go-grpc_out={}".format(outputs[0].dirname))
    args.add("--go-grpc_opt=paths=source_relative")
    args.add("--yarpc-go-v2_out={}".format(outputs[0].dirname))
    args.add("--yarpc-go-v2_opt=paths=source_relative")
    args.add("--proto_path={}".format(src.dirname))
    args.add(src.path)

    tools = [
        protoc,
        ctx.executable._protoc_gen_go,
        ctx.executable._protoc_gen_go_grpc,
        ctx.executable._protoc_gen_yarpc_go,
    ]

    ctx.actions.run_shell(
        inputs = [src],
        outputs = outputs,
        tools = tools,
        command = "mkdir -p {out_dir} && {protoc} \"$@\"".format(
            out_dir = outputs[0].dirname,
            protoc = protoc.executable.path,
        ),
        arguments = [args],
        mnemonic = "HermeticGoProto",
        progress_message = "Generating Go protobuf files for {}".format(src.short_path),
    )

    return [DefaultInfo(files = depset(outputs))]

go_proto_generated_files = rule(
    implementation = _go_proto_generated_files_impl,
    attrs = {
        "src": attr.label(
            allow_single_file = [".proto"],
            mandatory = True,
        ),
        "out_dir": attr.string(mandatory = True),
        "_protoc_gen_go": attr.label(
            default = "@org_golang_google_protobuf//cmd/protoc-gen-go",
            executable = True,
            cfg = "exec",
        ),
        "_protoc_gen_go_grpc": attr.label(
            default = "@org_golang_google_grpc_cmd_protoc_gen_go_grpc//:protoc-gen-go-grpc",
            executable = True,
            cfg = "exec",
        ),
        "_protoc_gen_yarpc_go": attr.label(
            default = "@org_uber_go_yarpc//encoding/protobuf/protoc-gen-yarpc-go-v2",
            executable = True,
            cfg = "exec",
        ),
    },
    toolchains = ["@rules_proto//proto:toolchain_type"],
)
