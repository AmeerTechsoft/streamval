# Getting started

New to streamval? Start here — install the package, define a Pydantic
schema, and run your first validation in a few minutes.

## In this section

| Guide | Description |
|---|---|
| [Quickstart](quickstart.md) | Copy-paste examples: CSV, JSONL, async, HTTP |
| [Schemas and models](schemas.md) | Pydantic models, type coercion, field design |

## Recommended path

1. **[Quickstart](quickstart.md)** — install and validate a CSV file.
2. **[Schemas and models](schemas.md)** — design models that match your data.
3. Continue to the [User guide](../user-guide/index.md) for every adapter
   and option.

## Install

```bash
pip install streamval

# Recommended for production CSV/JSONL throughput:
pip install "streamval[fast]"

# HTTP NDJSON and LLM streaming:
pip install "streamval[http]"
```

Return to the [documentation home](../index.md).
