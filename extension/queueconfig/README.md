# Queue Config Extension

Vendor-agnostic interface for providing queue configurations.

## Interfaces

### Store

Provides queue configurations by name.

```go
type Store interface {
    Get(ctx context.Context, name string) (queueconfig.QueueConfig, error)
    List(ctx context.Context) ([]queueconfig.QueueConfig, error)
}
```

## Entities

Queue configuration entities live in `entity/queueconfig/`:

- **QueueConfig** — configuration for a single submit queue (name, repository, destination, change provider)
- **Repository** — platform-specific repository identifier (opaque ID string)
- **Destination** — VCS-agnostic landing target (opaque ref string interpreted by the change provider)
