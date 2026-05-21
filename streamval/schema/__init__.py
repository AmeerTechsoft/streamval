"""Pydantic-schema compilation and per-format coercion.

`plan.py` caches a CompiledValidationPlan per Pydantic model class;
`coerce.py` knows how to massage raw values from each source format into
the shape Pydantic expects.
"""
