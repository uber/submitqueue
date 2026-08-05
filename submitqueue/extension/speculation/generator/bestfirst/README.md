# bestfirst

`bestfirst` is the default speculation candidate generator. It scores each unresolved dependency at most once per queue snapshot, treats those outcomes as independent, and ranks a path by the probability that all of its assumptions match the eventual dependency outcomes.

Each head is represented by a lazy local stream. The stream starts with the most likely assignment and enumerates successively less likely assignments by applying combinations of outcome flips in log-probability order. A global heap merges the local streams so callers receive one best-first sequence across all Speculating heads and disconnected graph components.

Resolved dependencies are fixed rather than scored. Succeeded dependencies always use the succeeds assumption; failed and cancelled dependencies always use the fails assumption. Cancelling dependencies remain probabilistic because cancellation is best-effort.

See the probability-ordered speculation generator RFC for the algorithm, numerical treatment, worked example, and complexity analysis.
