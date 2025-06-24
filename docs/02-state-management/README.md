# State Management with the KVStore

One of the most powerful features of `gostage` is its built-in, type-safe key-value store. Every workflow comes with an instance of `store.KVStore`, which is shared across all of its stages and actions. This allows for seamless state management and data passing throughout the entire lifecycle of a workflow.

## Accessing the Store

You can access the store in two primary ways:

1.  **Directly from the Workflow object**: `workflow.Store`
2.  **From the ActionContext**: `ctx.Store()` inside an action's `Execute` method.

The second method is the most common, as actions are typically responsible for reading and writing data.

## Basic Operations (CRUD)

The store provides simple, type-safe methods for creating, reading, updating, and deleting data.

### Writing Data: `Put`

You can store any Go value in the store using the `Put` method. The store preserves the value's concrete type.

```go
// Inside an action's Execute method:
ctx.Store().Put("user.id", 123)
ctx.Store().Put("user.name", "Alice")
ctx.Store().Put("user.roles", []string{"admin", "editor"})

type UserConfig struct {
    Timeout int
    Retries int
}
config := UserConfig{Timeout: 30, Retries: 3}
ctx.Store().Put("user.config", config)
```

### Reading Data: `Get` and `GetOrDefault`

To retrieve a value, you use the generic `Get` function. You must provide the expected type, and `Get` will handle the type assertion for you. This prevents runtime panics from incorrect type assertions.

```go
// Inside a later action:
userID, err := store.Get[int](ctx.Store(), "user.id")
if err != nil {
    return err // Key not found or type mismatch
}

roles, err := store.Get[[]string](ctx.Store(), "user.roles")
if err != nil {
    return err
}

userConfig, err := store.Get[UserConfig](ctx.Store(), "user.config")
if err != nil {
    return err
}

fmt.Printf("Processing user %d (%s) with %d roles.\n", userID, userName, len(roles))
fmt.Printf("User config: %+v\n", userConfig)
```

If you want to provide a default value instead of handling an error when a key is not found, you can use `GetOrDefault`.

```go
// If "log.level" doesn't exist, it will default to "info".
logLevel, _ := store.GetOrDefault[string](ctx.Store(), "log.level", "info")
```

### Deleting Data: `Delete`

You can remove a key from the store using the `Delete` method.

```go
wasDeleted := ctx.Store().Delete("user.roles")
if wasDeleted {
    fmt.Println("User roles have been removed from the store.")
}
```

## Advanced Features

The `KVStore` also supports more advanced features that we will cover later:

-   **Metadata**: Attach tags and properties to store entries.
-   **TTL**: Set a time-to-live for entries to automatically expire.
-   **Cloning and Merging**: Duplicate or merge stores.

---

Next, we will look at a complete, runnable example of using the store to pass data between actions. 