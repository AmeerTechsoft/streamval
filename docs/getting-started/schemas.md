# Schemas and models

Every streamval run is driven by a **Pydantic v2 `BaseModel`** subclass.
Each row from the source (CSV line, JSON object, Parquet record, HTTP
chunk) is validated against that model. You get back a typed Python
object on success, or structured field-level errors on failure.

---

## Defining a schema

Use standard Pydantic v2 syntax. Field names must match the column keys
in your source data (CSV headers, JSON keys, etc.).

```python
from datetime import date, datetime
from pydantic import BaseModel, Field, EmailStr

class Customer(BaseModel):
    id: int
    name: str = Field(min_length=1, max_length=200)
    email: EmailStr
    signup_date: date
    lifetime_value: float = Field(ge=0)
    active: bool
```

### Nested models

Nested structures work the same as in Pydantic. For CSV, nested fields
are uncommon (flat columns only). JSONL, Parquet, and HTTP NDJSON support
nested objects natively.

```python
class Address(BaseModel):
    city: str
    zip: str

class Person(BaseModel):
    name: str
    address: Address
```

### Optional and nullable fields

```python
class Event(BaseModel):
    id: int
    note: str | None = None       # key may be missing or null
    tags: list[str] = []          # default empty list
```

---

## How rows become models

streamval does **not** load the whole file. The pipeline for each row is:

```
  source bytes
      │
      ▼
  adapter (CSV / JSONL / Parquet / HTTP …)
      │  yields one dict per row
      ▼
  coerce_row()  ── format-specific pre-processing
      │
      ▼
  model.model_validate(dict)  ── Pydantic v2 (Rust core)
      │
      ▼
  ValidationResult
```

You never call `coerce_row` or `model_validate` yourself — the
`StreamValidator` handles this internally.

---

## Type coercion by source format

Before Pydantic runs, streamval may adjust raw values depending on where
the row came from. This produces clearer error messages (e.g. "expected
int" instead of "expected string").

| Format | Raw value types | Coercion applied |
|---|---|---|
| **CSV** | All values arrive as `str` | Strings → `int`, `float`, `bool`, `date`, `datetime` when the model field expects those types |
| **JSONL** | Already JSON-typed | Only `date` / `datetime` strings are pre-parsed |
| **HTTP NDJSON / SSE** | Same as JSONL | Same as JSONL |
| **Parquet / Arrow** | Native pyarrow types | **No coercion** — values pass through as-is |

### CSV coercion examples

Given this schema:

```python
class Row(BaseModel):
    id: int
    active: bool
    score: float
```

And this CSV row:

```csv
42,true,9.5
```

The adapter yields `{"id": "42", "active": "true", "score": "9.5"}`.
After coercion: `{"id": 42, "active": True, "score": 9.5}`.

Boolean strings accepted: `true`/`false`, `yes`/`no`, `1`/`0` (case
insensitive).

### When coercion fails

If a string cannot be coerced (e.g. `"not-an-int"` for an `int` field),
the value is left untouched and Pydantic produces the canonical
`int_parsing` error. The row is reported as invalid — the stream
continues (unless you use `fail_fast`).

---

## Schema design tips

1. **Match column names exactly** — CSV headers become dict keys.
   `"User ID"` in the file needs `Field(alias="User ID")` or a renamed
   header.

2. **Use strict types for production** — prefer `int` over `float` for
   IDs; use `EmailStr`, `HttpUrl`, etc. where appropriate.

3. **Keep models flat for CSV** — nested CSV requires custom parsing;
   JSONL/Parquet handle nesting naturally.

4. **One model per logical record type** — if one file has multiple row
   shapes, use a discriminated union or separate validation runs.

5. **Test with bad rows early** — run with `on_error="collect"` during
   development to see all failure modes before switching to `skip` in
   production.

---

## Aliases and field renaming

Pydantic v2 field aliases work normally:

```python
from pydantic import BaseModel, Field

class Row(BaseModel):
    user_id: int = Field(alias="User ID")
    model_config = {"populate_by_name": True}
```

With `populate_by_name=True`, both `"User ID"` and `"user_id"` are
accepted.

---

## Next steps

- [Running validation](../user-guide/validation.md) — wire your schema into
  `StreamValidator` or `stream_csv`.
- [Results, errors, and logging](../user-guide/results-and-errors.md) — read `ValidationResult`
  and handle failures.
