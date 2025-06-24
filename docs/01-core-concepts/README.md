# Core Concepts

In `gostage`, every process is built from three fundamental components: **Workflows**, **Stages**, and **Actions**. Understanding the hierarchy and interaction between these three parts is the key to using the library effectively.

-   **Workflow**: The highest-level container. It represents an entire end-to-end process and holds the shared state for all its components.
-   **Stage**: A logical phase within a `Workflow`. A workflow is composed of one or more stages that execute in sequence.
-   **Action**: The smallest unit of work. A stage is composed of one or more actions that execute sequentially.

Think of it like this: a **Workflow** is a cookbook, a **Stage** is a recipe in that book, and an **Action** is a single step in that recipe.

---

## A Practical Example

To see how these components work together, please see the complete, runnable example:

[**Core Concepts Demo**](./examples/01-core-concepts-demo/main.go)

This example demonstrates a two-stage workflow where the first stage prepares data in the shared store, and the second stage reads and processes that data. It clearly shows the hierarchy of `Action` -> `Stage` -> `Workflow` and the use of the shared store for passing state.

---

### In This Section

For more detailed information on each component, see the following pages:

-   [**01 - Workflows**](./01-workflows.md): Learn more about the top-level container.
-   [**02 - Stages**](./02-stages.md): Dive into the sequential phases that make up a workflow.
-   [**03 - Actions**](./03-actions.md): Understand the individual units of work that perform the actual tasks.

By the end of this section, you will be able to structure a complex process into a clear and manageable `gostage` workflow. 