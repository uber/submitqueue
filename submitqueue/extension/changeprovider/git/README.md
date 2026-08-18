# Git change provider

Reads change metadata — files, line counts, author — out of a git repository, for a remote that offers no API to ask.

The GitHub and Phabricator providers query a service that already knows what a change contains. This one derives it. It keeps its own copy of the remote and computes each change from the commits themselves, which makes a plain git remote a first-class source of change metadata with nothing in front of it: an internal host, a mirror, or a bare repository on disk.

## What a change is measured against

A `git://` change URI names a commit and the ref it lives on, and nothing else. A pull request carries a base; this does not, so the baseline has to be derived — and for a stack it cannot be the target branch.

A stack's changes are cut one from the next. Measuring every change against the target would report the second as containing the first, and any consumer that sums line counts across a batch would count them twice. So the first change in a request is measured from where it diverged from the target, and each one after it from where it diverged from the change before it. The order of the URIs is the stack order, and it is load-bearing.

A change that shares no history with what it claims to land on is an error, not a change that touches nothing.

## Its own copy

Each service keeps its own copy of a queue's repository and configures its own remote for it, so this provider's copy is independent of anything a merger keeps. Where that copy fetches from is configuration: a bind-mounted bare repository and a remote host are the same code path, differing only in the URL.

The copy is bare. Nothing here checks anything out — the provider answers questions about commits and never produces one — so there is no working tree to leave dirty and no index to corrupt.

Git commands against one repository cannot safely interleave, so every provider sharing a copy shares its lock.

Provisioning happens once, at wiring time, rather than on first use: resolving a provider happens per message on the validate path, so a copy created there would put a clone inside a retry loop and hide an unreachable remote behind queue processing instead of failing the service that owns the configuration.

## Authentication

The provider does not decide what a credential is. It takes an `Auth` implementation and calls it before each fetch; an integrator supplies one that reads an environment variable, calls a secrets manager, or mints a short-lived token, and only that implementation changes when the answer does. `Auth` is called per fetch rather than once so an expiring credential can be refreshed.

A nil `Auth` means the remote needs none. That covers a local path, and an SSH remote served by the host's own SSH configuration and agent — the environment a fetch needs to reach a remote is passed through, while the configuration that could change what a diff says is not.

## Tests

Hermetic, against throwaway repositories, driving the Bazel-pinned git rather than the host's. The test that matters most is the three-step stack: reporting a stack cumulatively is wrong by default, never fails loudly, and shows up only as odd-looking scores.
