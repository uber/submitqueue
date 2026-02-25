# Core

Shared infrastructure packages reused across services. These are internal building blocks — not domain entities, not pluggable extensions, but foundational components that services depend on directly.

## Packages

- **consumer/** — Queue message consumption framework. Manages subscription lifecycle, message routing to controllers, automatic ack/nack, error classification (retryable vs. poison pill), and graceful shutdown. Services register `Controller` implementations and the consumer handles the rest.
