# Queue configuration

Queue configuration is deployment policy, separate from mutable queue state in storage. The provider interface supplies immutable, fully resolved settings by queue name; it does not imply database persistence. Controllers obtain one configuration snapshot per delivery without knowing how it was loaded.

## Stage ownership

Configuration is grouped by stage: process, build signal, build analysis, and issue tracking. Resolved configuration types, validation, standard values, and source-independent optional overrides live together in `stovepipe/core/queueconfig`, organized by stage. Stage-specific application helpers copy values, truncate durations to milliseconds, and replace only explicitly supplied fields. Providers can use these helpers but are not required to expose or use overrides. Aggregates compose stages rather than inspecting individual settings.

The process stage owns concurrency and admission waits. The other current stage settings control build-status polling, build-issue analysis polling, and issue-tracking recheck and recovery waits. Stage configuration can contain both timing and non-timing settings. Immediate batch continuation and consumer error-retry policies remain separate.

## YAML provider

The YAML implementation decodes the shared optional configuration types directly and retains an immutable snapshot resolved at construction. The host supplies the registered queue names; an optional file supplies shared settings under `stages` and per-queue overrides under `queues`. Queue entries customize existing queues rather than register them. Resolution starts with the shared package's standard stage values, applies shared settings, then applies per-queue overrides. Omitted values inherit; explicit zero or negative values are retained and fail validation if still present in the resolved queue configuration. Only resolved configurations undergo semantic validation. The current concurrency limits and millisecond intervals must be positive.

The provider rejects malformed configuration, unknown fields, duplicate keys or queue overrides, overrides for unregistered queues, and multiple YAML documents. It serves the host-registered queues, returns independent values, and preserves host registration order when listing. It does not watch files or merge multiple files. Standard stage values are defined once in the shared configuration package, not repeated in each YAML file.

## Host integration

Hosts choose and construct the provider and inject the lookup interface. The example server uses its existing messaging tenant list to supply queue names and uses built-in stage settings unless a configuration file is selected. E2e selects a file containing only shorter shared stage waits through the same configuration path. A host using another configuration framework can return the same resolved values without adopting the YAML schema or its defaults mechanism. Messaging tenant registration remains a separate host concern.

Adding a setting requires updating its stage's resolved value, validation, standard value, and optional input with its application logic. A standard value lets existing configuration files keep working when a tuning setting is added. The YAML provider does not repeat stage schemas or field mappings. It does not require field-level logic in the lookup interface or the aggregate.
