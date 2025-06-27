# Gostage Documentation

Welcome to the official documentation for `gostage`, a powerful and flexible workflow orchestration engine for Go.

This documentation is designed to guide you from the basic concepts to advanced features, with runnable examples for each topic.

## Table of Contents

### Getting Started

1.  [**Introduction**](./00-introduction/README.md)
    *   What is `gostage`?
    *   Core Philosophy
    *   Hello, World! Example

2.  [**Core Concepts**](./01-core-concepts/README.md)
    *   Workflows, Stages, and Actions

3.  [**State Management with the KVStore**](./02-state-management/README.md)
    *   Using the Key-Value Store to pass data.

### Guides

4.  [**Dynamic Workflows**](./03-dynamic-workflows/README.md)
    *   Adding Actions and Stages at Runtime.

5.  [**Middleware**](./04-middleware/README.md)
    *   Wrapping Execution at the Runner, Workflow, and Stage levels.

6.  [**The Action Registry**](./05-action-registry/README.md)
    *   Registering Actions for Spawned Workflows

7.  [**Spawning Sub-Workflows**](./06-spawning-subworkflows/README.md)
    *   [Pure gRPC Communication](./06-spawning-subworkflows/grpc-spawning.md)

8.  [**Testing Workflows**](./07-testing-workflows/README.md)
    *   Unit and Integration Testing Strategies.

9.  [**Advanced Topics**](./08-advanced-topics/README.md)
    *   Error Handling Strategies.
    *   [Context Messaging](./08-advanced-topics/context-messaging.md)

---

This documentation is currently under construction. We will be building it out section by section. 