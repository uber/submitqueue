# Admission Gate Extension

Vendor-neutral contract for deciding whether a Stovepipe request may cross a logical pipeline boundary. See the [Stovepipe Admission Gates RFC](../../../doc/rfc/stovepipe/admission-gate.md) for decision semantics, durable-state ownership, composition, and controller integration.

Implementations take request identity, resolve their own facts, and return an admitted or deferred result. A controller receives a `Gates` resolver for its boundary, and that resolver selects one composite gate by request; concrete queue routing belongs in service wiring.
