# No-op Consumer Gate

A consumergate.Gate whose Enter always returns an unblocked Entry — every delivery flows straight to its controller. Wire it in services and tests that do not need runtime gating.
