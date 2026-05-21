# User guide

How to run validation, consume results, handle errors, and configure
logging for production pipelines.

## In this section

| Guide | Description |
|---|---|
| [Running validation](validation.md) | `StreamValidator`, sync/async, all formats |
| [Results, errors, and logging](results-and-errors.md) | `ValidationResult`, exceptions, stats, loggers |
| [Error strategies](error-strategies.md) | `fail_fast`, `collect`, `skip`, custom handlers |

## Workflow

```
  schema (Getting started)
       │
       ▼
  running validation  ──►  ValidationResult per row
       │
       ├── results / errors / logging
       └── error strategy (fail, collect, or skip)
```

Start with [Running validation](validation.md), then read
[Results, errors, and logging](results-and-errors.md) before choosing an
[error strategy](error-strategies.md).

## Prerequisites

- [Schemas and models](../getting-started/schemas.md)
- [Quickstart](../getting-started/quickstart.md) (optional hands-on intro)
