# Issue tracking

## Applying a planned change

The controller applies one `PlannedIssueChange` already saved in the queue's progress record. The cursor identifies the issue; the change holds only its write details. Expected issue version zero means create-if-absent. A positive version means update that exact version. A retry never adopts a newly loaded version to execute old instructions.

After an issue write succeeds, the controller writes the opening or closing history event, then clears the planned change while retaining the cursor using progress CAS. If an issue write already happened, a retry verifies the exact intended issue state and version before repairing history. The saved timestamp makes duplicate history stable. Failure leaves the original planned change available for another worker to finish.

A replacement must finish that same planned change before accepting later work. Closed issue rows retain their versions. Together, these rules prevent a paused old worker from reopening or closing an issue after newer work: its create-if-absent or original version guard fails. A conflicting issue state is not treated as a successful replay.

All writes use complete primary keys. There are no cross-record transactions, leases, snapshot indexes, or additional operation counters. Records and versions must not be deleted or reset while an old delivery could resume. All issue-state writers must follow this protocol.

This layer does not scan build issues, receive queue messages, publish repairs, or register a runtime consumer. Planning, delivery orchestration, and terminal recovery are separate additions.
