"""Rewrite doc links that leave docs_dir into links to the GitHub source tree.

The docs link to code and package READMEs elsewhere in the repository. Those
targets are not part of the site, so strict mode would reject them; pointing
them at GitHub keeps strict mode on for links between pages.
"""

import os
import re

REPO_BLOB_URL = "https://github.com/uber/submitqueue/blob/main/"
REPO_TREE_URL = "https://github.com/uber/submitqueue/tree/main/"

INLINE_LINK = re.compile(r"(\]\()([^)\s]+)(\))")
REFERENCE_LINK = re.compile(r"^(\s*\[[^\]]+\]:\s*)(\S+)(.*)$", re.MULTILINE)


def on_page_markdown(markdown, page, config, files):
    docs_dir = os.path.abspath(config["docs_dir"])
    repo_root = os.path.dirname(docs_dir)
    page_dir = os.path.dirname(os.path.join(docs_dir, page.file.src_path))

    def rewrite_target(target):
        if re.match(r"^[a-z][a-z0-9+.-]*:", target) or target.startswith(("#", "/")):
            return target
        path, sep, anchor = target.partition("#")
        resolved = os.path.normpath(os.path.join(page_dir, path))
        if resolved == docs_dir or resolved.startswith(docs_dir + os.sep):
            return target
        if not resolved.startswith(repo_root + os.sep):
            return target
        rel = os.path.relpath(resolved, repo_root).replace(os.sep, "/")
        base = REPO_TREE_URL if os.path.isdir(resolved) else REPO_BLOB_URL
        return base + rel + sep + anchor

    def rewrite_prose(text):
        text = INLINE_LINK.sub(lambda m: m.group(1) + rewrite_target(m.group(2)) + m.group(3), text)
        return REFERENCE_LINK.sub(lambda m: m.group(1) + rewrite_target(m.group(2)) + m.group(3), text)

    # Even-indexed chunks are prose, odd-indexed chunks are fenced code.
    chunks = re.split(r"(^\s*(?:```|~~~).*?^\s*(?:```|~~~)[^\n]*$)", markdown, flags=re.MULTILINE | re.DOTALL)
    return "".join(rewrite_prose(c) if i % 2 == 0 else c for i, c in enumerate(chunks))
