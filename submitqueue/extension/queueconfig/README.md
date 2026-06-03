# Queue Config Extension

Vendor-agnostic interface for providing queue configurations.

## Interfaces

### Store

Provides queue configurations by name.

```go
type Store interface {
    Get(ctx context.Context, name string) (entity.QueueConfig, error)
    List(ctx context.Context) ([]entity.QueueConfig, error)
}
```

## Entities

Queue configuration entity lives in `entity/queue_config.go`:

- **QueueConfig** — configuration for a single submit queue (name, VCS type, VCS repo, target)
