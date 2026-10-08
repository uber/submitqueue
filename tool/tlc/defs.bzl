"""Bazel test that model-checks a TLA+ spec's design matrix with TLC."""

load("@rules_python//python:defs.bzl", "py_test")

def tlc_matrix_test(name, matrix, spec, size = "medium", **kwargs):
    """Fails when any combination in `matrix` gets a verdict other than its expectation.

    Args:
      name: test name.
      matrix: the matrix JSON file.
      spec: the .tla file (and any modules it extends) the matrix checks.
      size: Bazel test size.
      **kwargs: passed through to py_test.
    """
    py_test(
        name = name,
        srcs = ["//tool/tlc:tlc.py"],
        main = "//tool/tlc:tlc.py",
        args = ["--check", "$(rootpath %s)" % matrix],
        data = [matrix, "//tool/tlc:tlc_java"] + (spec if type(spec) == "list" else [spec]),
        legacy_create_init = 0,
        size = size,
        deps = ["@rules_python//python/runfiles"],
        **kwargs
    )
