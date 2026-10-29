# Issue history and summaries

The materializer retains an issue transition before updating its summary and open-issue lookup. Issue tracking calls it inline during normal processing and recovery; there is no separate consumer. A failed projection write returns an error, so the current issue remains at the cursor for retry. Replaying an existing transition reads its original timestamp and repairs both projections.

The source issue state supplies the opening build ID because closing log entries identify the closing build, not the opening. That value comes from the same retained issue snapshot as the transition; the materializer does not look up a potentially newer issue state to reconstruct an older opening.

Summary ordering uses the source build's request ID in ingest order. Within one request, closing supersedes opening. Build IDs and timestamps do not establish order. Older transitions remain in history but cannot replace newer summaries. Conditional-create and CAS conflicts reload the winning summary and repeat the ordering check. Closed summaries remain present across reopening, with increasing projection versions.

## Open-issue lookup

After summary materialization, the lookup is repaired from the latest summary, including when an incoming transition was a duplicate or older than the summary. An open summary requires a reference to its opening; a closed summary requires no open-issue row.

Each iteration reads the open-issue row before the summary. Otherwise, a worker could read an old closed summary, then read a newer opening and use that opening's ID to delete it. Reading the open-issue row first means a concurrent reopening changes the expected ID and prevents the stale delete or replacement. Creates require absence; replacement and deletion compare the opening that was read. Both successful writes and guard mismatches repeat the reads. A delayed create can briefly leave a stale reference after a close; the next iteration removes it. Storage implementations must satisfy the causal read-after-write contract.

The two projections are not updated atomically. A future list reader must still verify that each referenced summary is open and its opening matches. No status filtering, cross-record transaction, or queue-wide scan is required to maintain one open-issue row.

This materializer does not backfill existing history. The [open-issues API](../../../doc/rfc/stovepipe/open-issues-api.md) reads the lookup and validates referenced summaries. Source builds must remain readable while their transitions can be replayed.
