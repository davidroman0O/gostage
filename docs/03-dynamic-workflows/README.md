# Dynamic Workflows

One of the most advanced features of `gostage` is the ability to modify a workflow's structure *while it is running*. This allows you to build highly adaptive processes that can react to their own results or external conditions.

You can dynamically:
1.  **Add new Actions** to the current stage.
2.  **Add new Stages** to the workflow.

This is all done from within an action's `Execute` method via the `ActionContext`.

## Why Use Dynamic Workflows?

-   **Responsive Systems**: Create workflows that discover resources (like files, servers, or database records) and then generate the necessary actions or stages to process them.
-   **Conditional Logic**: Instead of complex `if/else` blocks inside a single action, you can generate different follow-up actions or stages based on a condition.
-   **Reduced Complexity**: For complex branching logic, generating the required steps dynamically can be much cleaner than defining a massive, static workflow with many conditional checks.
-   **Extensibility**: Actions can be designed as "plugins" that add their own specific processing stages to a generic workflow.

---

### In This Section

-   [**01 - Dynamic Actions**](./01-dynamic-actions.md): Learn how to add new actions to the current stage during execution.
-   [**02 - Dynamic Stages**](./02-dynamic-stages.md): Learn how to add entire new stages to the workflow on the fly.

By the end of this section, you'll be able to build workflows that are not just sequential but also intelligent and responsive. 