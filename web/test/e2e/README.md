# Web end-to-end test

`//web/test/e2e:web_test` verifies the browser UX against the real SubmitQueue stack. Bazel builds the Gateway, Orchestrator, Runway, web OCI image, generated API package, Playwright launcher, and Chromium. The test stages those declared runfiles into an isolated Compose context, applies the real MySQL schemas, submits requests through the gateway, and exercises the web host.

The suite covers the anonymous Basic-auth challenge, authenticated list access, newest-first ordering, slash-containing SQIDs encoded into one URL segment, current status, ordered lifecycle/build history, and axe checks on list and detail pages.

`run_e2e.py` is a Bazel `py_test` entrypoint rather than an independent build script. It stages declared runfiles, invokes Docker Compose, applies SQL through `docker exec`, locates published ports, and starts the Bazel-provided Playwright executable. Application assertions remain in `submitqueue.spec.ts`.

```bash
make web-e2e-test
```

Docker must be running. Set `SKIP_CLEANUP=true` when debugging to retain the temporary stack and staged context.
