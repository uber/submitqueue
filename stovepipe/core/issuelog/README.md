# Issue history and summaries

The materializer retains an issue transition before updating its summary. Issue tracking calls it inline during normal processing and recovery; there is no separate consumer. A failed summary write returns an error, so the current issue remains at the cursor for retry. Replaying an existing transition reads its original timestamp and still repairs the summary.

The source issue state supplies the opening build ID because closing log entries identify the closing build, not the opening. That value comes from the same retained issue snapshot as the transition; the materializer does not look up a potentially newer issue state to reconstruct an older opening.

Summary ordering uses the source build's request ID in ingest order. Within one request, closing supersedes opening. Build IDs and timestamps do not establish order. Older transitions remain in history but cannot replace newer summaries. Conditional-create and CAS conflicts reload the winning summary and repeat the ordering check. Closed summaries remain present across reopening, with increasing projection versions.

This materializer does not backfill existing history, maintain an open-only mapping, or expose a list API. Source builds must remain readable while their transitions can be replayed.
