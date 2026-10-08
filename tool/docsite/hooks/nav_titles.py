"""Give navigation sections readable titles.

The nav is generated from the directory tree so new docs appear without config
changes; MkDocs titles a section by capitalizing its directory name, which
yields "Howto" and "Rfc". This maps directory names to display titles instead.
"""

from mkdocs.structure.nav import Section
from mkdocs.structure.pages import Page

SECTION_TITLES = {
    "howto": "Guides",
    "rfc": "Design (RFCs)",
    "runway": "Runway",
    "steps": "Stages",
    "stovepipe": "Stovepipe",
    "submitqueue": "SubmitQueue",
}

# Pages listed here sort first within their section, in this order.
LEADING_PAGES = ["howto/QUICKSTART.md"]


def on_nav(nav, config, files):
    _retitle_sections(nav.items)
    _relink_previous_and_next_pages(nav)
    return nav


def _retitle_sections(items):
    for item in items:
        if not isinstance(item, Section):
            continue
        item.title = SECTION_TITLES.get(item.title.lower(), item.title)
        item.children.sort(key=_leading_page_rank)
        _retitle_sections(item.children)


def _relink_previous_and_next_pages(nav):
    # MkDocs links previous/next pages before on_nav runs, so a reorder must redo them.
    nav.pages = list(_pages_in_order(nav.items))
    for i, page in enumerate(nav.pages):
        page.previous_page = nav.pages[i - 1] if i > 0 else None
        page.next_page = nav.pages[i + 1] if i + 1 < len(nav.pages) else None


def _pages_in_order(items):
    for item in items:
        if isinstance(item, Section):
            yield from _pages_in_order(item.children)
        elif isinstance(item, Page):
            yield item


def _leading_page_rank(item):
    src = getattr(getattr(item, "file", None), "src_uri", None)
    return LEADING_PAGES.index(src) if src in LEADING_PAGES else len(LEADING_PAGES)
