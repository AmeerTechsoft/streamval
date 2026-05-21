# Python API (auto-generated)

This page is generated from docstrings by [mkdocstrings](https://mkdocstrings.github.io/)
during the MkDocs / Read the Docs build. For narrative guides, see
[API overview](api-reference.md).

---

## Package

::: streamval
    options:
      members:
        - StreamValidator
        - ValidationResult
        - FieldError
        - StreamStats
        - StreamValidationError
        - StreamFetchError
        - HttpNdjsonConfig
        - stream_csv
        - stream_jsonl
        - stream_parquet
        - stream_http_ndjson
        - astream_csv
        - astream_jsonl
        - astream_parquet
        - astream_http_ndjson
        - __version__
      show_submodules: true

---

## StreamValidator

::: streamval.core.validator.StreamValidator
    options:
      members: true
      show_if_no_docstring: false

---

## Result types

::: streamval.core.result.ValidationResult
    options:
      members: true

::: streamval.core.result.FieldError
    options:
      members: true

::: streamval.core.result.StreamValidationError
    options:
      members: true

::: streamval.core.result.StreamFetchError
    options:
      members: true

---

## Statistics

::: streamval.core.stats.StreamStats
    options:
      members: true

---

## HTTP configuration

::: streamval.adapters.http_ndjson_adapter.HttpNdjsonConfig
    options:
      members: true

---

## Error strategies

::: streamval.strategies.base.StrategyHandler
    options:
      members: true

::: streamval.strategies.base.ErrorStrategy

---

## LLM helpers

::: streamval.llm.LLMProvider

::: streamval.llm.validate_llm_stream

::: streamval.llm.avalidate_llm_stream

::: streamval.llm.extract_content
