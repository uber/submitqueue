# Static Prioritization Limit

The static `prioritizationlimit.PrioritizationLimit` is constructed with a single integer and returns it, unchanged, from every call to `Limit`. It never errors and never consults any external signal — the simplest possible prioritization limit, useful for wiring tests, local development, and any queue whose concurrent-build budget is a fixed operational constant rather than one derived from live CI capacity or cost signals.
