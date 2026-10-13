"""Builds the documentation site in strict mode, failing on broken links between pages."""

import os
import sys

from mkdocs.__main__ import cli

if __name__ == "__main__":
    # The runfiles tree mirrors the workspace layout, so docs_dir (../../doc) resolves inside it.
    config = os.path.join(os.environ["TEST_SRCDIR"], "_main", "tool", "docsite", "mkdocs.yml")
    site_dir = os.path.join(os.environ["TEST_TMPDIR"], "site")
    os.environ.setdefault("NO_MKDOCS_2_WARNING", "1")
    sys.exit(cli(["build", "--strict", "--config-file", config, "--site-dir", site_dir]))
