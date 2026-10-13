"""Hermetic TypeScript protobuf generation for the web workspace."""

load("@aspect_rules_js//js:libs.bzl", "js_binary_lib")

def _generate_web_api_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    template = ctx.actions.declare_file(ctx.label.name + ".buf.gen.yaml")
    module = ctx.actions.declare_directory(ctx.label.name + "_module")
    stage_arguments = ctx.actions.args()
    stage_arguments.add(module.path)
    for source in ctx.files.srcs:
        stage_arguments.add(source)
        stage_arguments.add(source.short_path)
    ctx.actions.run_shell(
        arguments = [stage_arguments],
        command = """
set -euo pipefail
output="$1"
shift
while (( "$#" )); do
  source="$1"
  relative="$2"
  mkdir -p "$output/$(dirname "$relative")"
  cp "$source" "$output/$relative"
  shift 2
done
""",
        inputs = ctx.files.srcs,
        outputs = [module],
        mnemonic = "WebProtoStage",
    )

    bin_to_execroot = "../" * len(ctx.bin_dir.path.split("/"))
    protoc_gen_es = bin_to_execroot + ctx.executable.protoc_gen_es.path
    ctx.actions.write(
        output = template,
        content = "\n".join([
            "version: v2",
            "clean: true",
            "plugins:",
            "  - local: %s" % protoc_gen_es,
            "    out: %s" % output.short_path,
            "    opt:",
            "      - target=ts",
            "      - import_extension=js",
            "",
        ]),
    )

    arguments = [
        "generate",
        module.short_path,
        "--template=%s" % template.short_path,
    ]
    for source in ctx.files.srcs:
        arguments.extend([
            "--path",
            module.short_path + "/" + source.short_path,
        ])

    js_binary_lib.run_binary_action(
        ctx = ctx,
        executable = ctx.executable.buf,
        arguments = arguments,
        inputs = depset([module, template]),
        outputs = [output],
        env = ctx.configuration.default_shell_env,
        tools = [
            ctx.attr.buf[DefaultInfo].files_to_run,
            ctx.attr.protoc_gen_es[DefaultInfo].files_to_run,
        ],
        mnemonic = "WebProtoGenerate",
        progress_message = "Generating TypeScript protobuf API %{label}",
    )
    return [DefaultInfo(files = depset([output]))]

generate_web_api = rule(
    implementation = _generate_web_api_impl,
    attrs = {
        "buf": attr.label(
            executable = True,
            cfg = "exec",
            mandatory = True,
        ),
        "protoc_gen_es": attr.label(
            executable = True,
            cfg = "exec",
            mandatory = True,
        ),
        "srcs": attr.label_list(
            allow_files = [".proto"],
            mandatory = True,
        ),
    },
)
