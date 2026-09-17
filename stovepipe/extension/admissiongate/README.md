# Admission Gate Extension

Vendor-neutral contract for deciding whether a Stovepipe request may cross a logical admission point. See the [Stovepipe Admission Gates RFC](../../../doc/rfc/stovepipe/admission-gate.md) for decision semantics, durable-state ownership, composition, and controller integration.

Implementations take request identity, resolve their own facts, and return an admitted or deferred result. The `Gates` resolver selects one composite gate by request and admission point; its concrete routing belongs in service wiring.
