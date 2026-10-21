# No-op build issue detector

Returns no observations without inspecting build output. The package supplies both the detector and its factory. The reference host uses this implementation to keep the analysis stage wired; deployments supply a real detector through host wiring. An empty result does not establish build success or recovery.
