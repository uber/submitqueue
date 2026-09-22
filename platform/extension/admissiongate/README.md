# Admission Gate

Vendor-neutral contract for deciding whether a domain entity may cross a logical pipeline boundary. See the [Admission Gates RFC](../../../doc/rfc/admission-gate.md) for decision semantics, policy ownership, composition, and controller integration.

The generic entity parameter preserves each domain's stage-level input contract: Stovepipe binds it to `entity.Request`, while a future SubmitQueue use may bind it to `entity.Batch` or another domain entity. Implementations evaluate policy through injected dependencies and need not own storage. A controller receives a `Gates[T]` resolver for its boundary, and that resolver selects one composite gate from a queue-scoped `Config`; concrete queue routing belongs in service wiring.
