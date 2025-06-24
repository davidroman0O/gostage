# Testing Workflows

Testing is a critical part of building reliable, production-ready workflows. `gostage` is designed to be highly testable at different levels of granularity. A good testing strategy for a `gostage` application typically involves a mix of unit, integration, and end-to-end tests.

We recommend thinking about testing in a pyramid structure:

-   **Unit Tests (Base of the Pyramid)**: These are fast, focused tests for individual `Actions`. This is where you'll test your core business logic in isolation. You'll write many of these.
-   **Integration Tests (Middle of the Pyramid)**: These tests verify that the `Actions` within a `Stage` work correctly together and that data flows between them as expected through the store.
-   **End-to-End Tests (Top of the Pyramid)**: These are the most comprehensive tests. They execute a full `Workflow`, often including spawned child processes, to ensure the entire system functions correctly. You'll have fewer of these, as they are slower to run.

This approach ensures that you have fast, targeted feedback for your core logic, while also having confidence in the integration and overall execution of your workflows.

---

### In This Section

-   [**01 - Unit Testing Actions**](./01-unit-testing-actions.md): Learn how to test a single action in isolation by mocking its context.
-   [**02 - Integration Testing Stages**](./02-integration-testing-stages.md): See how to test the interaction of multiple actions within a single stage.
-   [**03 - Testing Spawned Workflows**](./03-testing-spawned-workflows.md): Learn the pattern for writing end-to-end tests for workflows that run in separate processes. 