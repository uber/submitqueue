# Git change provider

Reads change metadata — files, line counts, author — out of a git repository, for a remote that offers no API to ask.

The GitHub and Phabricator providers query a service that already knows what a change contains. This one derives it. It keeps its own copy of the remote and computes each change from the commits themselves, which makes a plain git remote a first-class source of change metadata with nothing in front of it: an internal host, a mirror, or a bare repository on disk.

## What a change is measured against

A `git://` change URI names a commit and the ref it lives on, and nothing else. A pull request carries a base; this does not, so the baseline has to be derived — and for a stack it cannot be the target branch.

A stack's changes are cut one from the next. Measuring every change against the target would report the second as containing the first, and any consumer that sums line counts across a batch would count them twice. So the first change in a request is measured from where it diverged from the target, and each one after it from where it diverged from the change before it. The order of the URIs is the stack order, and it is load-bearing.

A change that shares no history with what it claims to land on is an error, not a change that touches nothing.

## What this package is, and isn't

This package is only the derivation: parse the URI, pick the baseline, read the diff and author, shape the result. Everything about *reaching* the repository — the local bare copy, fetching, merge bases, authentication, the git command environment — is transport plumbing that lives in [`platform/git/repo`](../../../../platform/git/repo) (built on [`platform/git/exec`](../../../../platform/git/exec)), which the merger and any other git caller share. The provider depends on a small `Repository` interface it defines, so it holds no `os/exec` and no credential handling of its own.

## Tests

Hermetic, against throwaway repositories, driving the Bazel-pinned git rather than the host's. The test that matters most is the three-step stack: reporting a stack cumulatively is wrong by default, never fails loudly, and shows up only as odd-looking scores.
