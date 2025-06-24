# Middleware

Middleware is a powerful feature in `gostage` that allows you to add cross-cutting concerns to your workflows without cluttering your core business logic. It's a way to wrap execution at different levels of the hierarchy, enabling you to implement functionality like logging, timing, error handling, and resource management in a clean and reusable way.

## The Middleware Hierarchy

`gostage` has a three-tiered middleware system. Understanding the order of execution is key. When a workflow runs, the middleware wraps the execution like layers of an onion:

1.  **Runner Middleware**: The outermost layer. It wraps the entire execution of a `Workflow`. A runner can execute multiple workflows, and this middleware will run once for each workflow.
2.  **Workflow Middleware**: The middle layer. It wraps the execution of each `Stage` within a workflow. If a workflow has three stages, this middleware will run three times.
3.  **Stage Middleware**: The innermost layer. It wraps the execution of all `Actions` within a single `Stage`.

The execution flow for a single stage looks like this:

```
Runner Middleware (starts)
  -> Workflow Middleware (starts)
    -> Stage Middleware (starts)
      -> Action 1 executes
      -> Action 2 executes
      -> ...
    -> Stage Middleware (ends)
  -> Workflow Middleware (ends)
-> Runner Middleware (ends)
```

## Why Use Middleware?

-   **Separation of Concerns**: Keep your action logic focused on its specific task, while middleware handles broader concerns like logging, metrics, or authentication.
-   **Reusability**: Write a piece of middleware once (e.g., a timer) and apply it to any runner, workflow, or stage.
-   **Consistency**: Enforce consistent behavior across all parts of your workflows, such as standardized logging formats or error handling policies.

---

### In This Section

-   [**01 - Runner Middleware**](./01-runner-middleware.md): Learn how to wrap the execution of entire workflows.
-   [**02 - Workflow Middleware**](./02-workflow-middleware.md): Learn how to wrap the execution of each stage within a workflow.
-   [**03 - Stage Middleware**](./03-stage-middleware.md): Learn how to wrap the execution of all actions within a stage.
-   [**04 - Error Handling**](./04-error-handling.md): See how to implement robust error handling and recovery strategies using middleware.

By the end of this section, you will be able to effectively use middleware to build robust, maintainable, and observable workflows. 