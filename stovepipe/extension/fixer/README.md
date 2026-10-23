# Repair execution

[`RepairExecutor`](fixer.go) submits repair work for one [`OpenIssue`](../../entity/open_issue.go). The executor can invoke an in-process implementation or wrap an external provider.

`Submit` returns promptly with a nonempty opaque fix ID on acceptance, or an error. Acceptance does not establish recovery.

An error, including a timeout or cancellation, can occur after the provider accepted the work. Return a known fix ID even alongside an error. Retry policy belongs to the caller; retrying without an ID can start duplicate work. Implementations return plain dependency errors and must support concurrent calls.

`Factory.For` resolves an executor bound to `Config.QueueName`. Generated mocks are available in [`mock`](mock/fixer_mock.go).
