# Issue tracking

## Notifications and bounded work

Notifications identify a queue, build, and whether analysis is final. They share one queue partition and consumer group. A refreshing or finalizing pass is finished before another source is accepted. After a pass completes, request ingest order rejects older builds, finalized requests reject later refreshes, and completed owning notifications are ignored on replay. A notification that helps another pass finish holds once more for its own work.

Each delivery processes at most 100 distinct task/unit keys, including a resumed change and closed records. Refresh opens newly reported issues but does not close by absence. Finalization also scans issue states: reported issues stay open or reopen, absent open issues close, and closed records remain untouched. A canceled build cannot close by absence because validation may not have run. Every scanned key advances progress, including closed records; a full read requires another batch to establish exhaustion.

Before changing an issue, planning reads its current version and saves the cursor and intended transition together using progress CAS. No issue write happens if that save loses a race. With a planned change, the cursor identifies the issue to finish; without one, scans continue after the cursor. The planned change retains its original expected version and history timestamp. Final handoff requires successful complete detection and persistence, and the request's recorded winning terminal build. Finalization uses accumulated per-build issues, not just the latest detector response.

## Applying a planned change

The controller applies one `PlannedIssueChange` already saved in the queue's progress record. The cursor identifies the issue; the change holds only its write details. Expected issue version zero means create-if-absent. A positive version means update that exact version. A retry never adopts a newly loaded version to execute old instructions.

After an issue write succeeds, the controller writes the opening or closing history event, then clears the planned change while retaining the cursor using progress CAS. If an issue write already happened, a retry verifies the exact intended issue state and version before repairing history. The saved timestamp makes duplicate history stable. Failure leaves the original planned change available for another worker to finish.

A replacement must finish that same planned change before accepting later work. Closed issue rows retain their versions. Together, these rules prevent a paused old worker from reopening or closing an issue after newer work: its create-if-absent or original version guard fails. A conflicting issue state is not treated as a successful replay.

All writes use complete primary keys. There are no cross-record transactions, leases, snapshot indexes, or additional operation counters. Records and versions must not be deleted or reset while an old delivery could resume. All issue-state writers must follow this protocol.

The reference host does not register this consumer. Repair publication and terminal recovery remain follow-ups. Permanently failing planned changes block later issue-tracking work and require recovery; skipping the planned change is not safe. A future repair handoff must persist its prerequisites and publish before clearing the change that owns that obligation.
