# Static Dependency Limit

The static `dependencylimit.DependencyLimit` is constructed with a single integer and returns it, unchanged, from every call to `Limit`. It never errors and never consults any external signal — the simplest possible dependency limit, useful for wiring tests, local development, and any queue whose eligibility bound is a fixed operational constant rather than one derived from live capacity or other signals.
