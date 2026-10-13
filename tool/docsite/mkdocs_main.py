"""Entry point for the MkDocs CLI, pinned to the docsite's locked dependencies.

`serve` without an explicit address binds a free localhost port, like the local
service containers, so a preview never collides with something already on 8000.
"""

import os
import socket
import sys

from mkdocs.__main__ import cli


def pick_free_localhost_port():
    # MkDocs prints its URL before binding, so port 0 would be reported as ":0";
    # reserve a port up front instead. Another process could take it in between.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def with_free_dev_addr(args):
    if not args or args[0] != "serve":
        return args
    if any(a in ("-a", "--dev-addr") or a.startswith("--dev-addr=") for a in args):
        return args
    return args + ["--dev-addr", f"127.0.0.1:{pick_free_localhost_port()}"]


if __name__ == "__main__":
    # The site pins MkDocs 1.x; silence Material's banner about the incompatible MkDocs 2.0.
    os.environ.setdefault("NO_MKDOCS_2_WARNING", "1")
    sys.exit(cli(with_free_dev_addr(sys.argv[1:])))
