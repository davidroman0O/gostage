# 01 - IPC with JSON

The default and simplest method for communicating between a parent and a spawned sub-workflow is using JSON messages over the standard I/O pipes (`stdin` and `stdout`). This method is straightforward and easy to debug since the messages are human-readable.

## The Communication Protocol

All communication from the child back to the parent happens through a standard message structure.

### The `gostage.Message` Struct

Every message is a JSON object with two fields: `type` and `payload`.

-   `type`: A string that identifies the purpose of the message (e.g., `"log"`, `"store_put"`).
-   `payload`: A JSON object containing the data for that message.

Here is an example of what a log message looks like when serialized to JSON and written to the child's `stdout`:

```json
{
  "type": "log",
  "payload": {
    "level": "INFO",
    "message": "Child action has finished."
  }
}
```

The available message types are defined as constants in the `gostage` package (e.g., `gostage.MessageTypeLog`, `gostage.MessageTypeStorePut`).

## The `RunnerBroker`: Sending and Receiving

The `RunnerBroker` is the component responsible for handling this communication.

-   **In the Child**: The `childRunner.Broker` is automatically configured to marshal `Message` structs into JSON and write them to `os.Stdout`.
-   **In the Parent**: The `parentRunner.Broker` is configured to read from the child's `stdout`, parse the JSON messages, and route them to appropriate handlers based on the message `type`.

### Child: Sending Messages

Inside an action, you use `ctx.Send(messageType, payload)` to send data back to the parent. The payload can be any `interface{}` that is serializable to JSON (like a `map[string]interface{}` or a struct).

```go
// Inside an action's Execute method in the child process:

func (a *MyAction) Execute(ctx *gostage.ActionContext) error {
    // This sends a message that the parent can use to update its store.
    payload := map[string]interface{}{
        "key":   "child.status",
        "value": "completed",
    }
    ctx.Send(gostage.MessageTypeStorePut, payload)

    // The logger also uses the broker under the hood.
    ctx.Logger.Info("Child action has finished.")

    return nil
}
```

### Parent: Handling Messages

The parent process must register handlers to process the messages it receives from the child. You use `runner.Broker.RegisterHandler()` for this. The handler is a function that receives the message type and its raw JSON payload.

```go
// In the parent process:

parentRunner := gostage.NewRunner()

// Register a handler for 'store_put' messages.
parentRunner.Broker.RegisterHandler(
    gostage.MessageTypeStorePut,
    func(msgType gostage.MessageType, payload json.RawMessage) error {
        // Define a struct to unmarshal the specific payload.
        var data struct {
            Key   string      `json:"key"`
            Value interface{} `json:"value"`
        }
        if err := json.Unmarshal(payload, &data); err != nil {
            return err
        }

        fmt.Printf("Parent: Received request to set key '%s' to value '%v'\n", data.Key, data.Value)
        // You would typically update the parent's store here.
        // parentStore.Put(data.Key, data.Value)
        return nil
    },
)

// Spawn the child process...
// The parent's broker will now correctly handle messages of this type.
```

This handler-based approach allows the parent to react to events from the child in real-time, providing a robust and flexible communication channel.

---

Next, we'll see how to achieve the same result with the more advanced [**IPC with gRPC**](./02-ipc-grpc.md).

## The Parent/Child Execution Flow

The core of the spawning pattern is a single Go binary that can be executed in one of two modes: "parent" (the main application) or "child" (the spawned worker). The mode is typically determined by a command-line flag or an environment variable that the application checks at startup.

Here is a more detailed breakdown of the execution flow:

1.  **Parent Defines Work**: The parent process constructs a `SubWorkflowDef`. This is a serializable struct that describes the workflow to be run, including the unique IDs of the actions involved (as registered in the Action Registry).
2.  **Parent Spawns Itself**: The parent executes its own binary, adding a special flag like `--gostage-child` to the command. This new process is the "child".
3.  **Parent Sends Definition**: The parent serializes the `SubWorkflowDef` into a JSON string and writes it to the child's `stdin` pipe.
4.  **Child Starts in Worker Mode**: The child process starts, sees the `--gostage-child` flag, and knows it must act as a worker. Its first task is to read the workflow definition from its `stdin`.
5.  **Child Rebuilds Workflow**: The child uses the action IDs from the definition to look up the corresponding action factories in its own Action Registry and reconstructs the entire workflow in memory.
6.  **Child Executes**: The child creates a `Runner` and executes the reconstructed workflow.
7.  **Child Communicates**: As the child's actions run, they can use `ctx.Send()` to send `Message` objects (like logs or status updates) back to the parent. The child's `RunnerBroker` automatically marshals these messages to JSON and writes them to its `stdout`.
8.  **Parent Listens for Results**: The parent process continuously reads the `stdout` of the child process, deserializing the JSON messages and routing them to registered handlers. This allows the parent to react in real-time to the child's progress.

## The Application Entry Point

Your application's `main` function needs to handle a startup argument to differentiate between parent and child modes.

```go
// In your application's main.go
func main() {
    // A simple flag check to determine the execution mode.
    if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
        // If the flag is present, run the logic for the child worker.
        childMain()
    } else {
        // Otherwise, run the main parent application logic.
        parentMain()
    }
}
```
This clear division of responsibilities makes the spawning pattern robust. The parent orchestrates, and the child executes its assigned task in isolation.