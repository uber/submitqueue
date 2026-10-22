# Build-slot ownership

`Claim` reserves capacity once per Request in the Queue snapshot provided by process. Process owns the admission policy: coalescing, capacity, and baseline selection. A successful claim returns the persisted snapshot with its new version. A version conflict returns the current Queue and `storage.ErrVersionMismatch`, so process rechecks that policy before retrying. An existing owner reuses its reservation; neither operation mutates the supplied snapshot.

`Complete` accepts a durable terminal Request. It removes only that Request’s reservation from `Queue.InFlightRequestIDs` and recomputes `InFlightCount`. For a newer successful outcome it advances `LastGreenURI` and `LastGreenRequestID` in the same Queue CAS, so the next admission sees the successful baseline when it sees available capacity.

The Request outcome must be persisted first. A crash before Queue completion temporarily holds capacity; primary retries and DLQ recovery repeat completion even for terminal Requests. Absence of the Request id makes completion a no-op, including after an ambiguous committed Queue write. CAS conflicts reload the Queue to preserve concurrent owners, enablement, and newer baselines. Inputs remain immutable snapshots.

The baseline can lead validation facts and Git promotion. Record resolves projects and persists facts and history before promotion. Buildslot neither writes facts nor calls Git.
