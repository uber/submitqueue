# Queue Config Extension

Vendor-agnostic interface for providing queue configurations.

## Interfaces

### Store

Provides queue configurations by name.

```go
type Store interface {
    Get(ctx context.Context, name string) (queueconfig.Config, error)
    List(ctx context.Context) ([]queueconfig.Config, error)
}
```

## Entities

Queue configuration entities live in `entity/queueconfig/`:

- **Config** — configuration for a single submit queue (name, repository, destination, change provider)
- **Repository** — platform-specific repository identifier (opaque ID string)
- **Destination** — VCS-agnostic landing target (opaque ref string interpreted by the change provider)
