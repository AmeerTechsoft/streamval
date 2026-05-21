"""CompiledValidationPlan — cached, per-model validation pipeline.

A plan is constructed once per Pydantic model class and reused for every
row. It holds the validator callable, the field map, and the source
format, so per-row work stays minimal. Plans are cached in a
module-level :class:`weakref.WeakKeyDictionary` keyed by model class so
short-lived models can be garbage-collected.
"""

from __future__ import annotations

import weakref
from typing import Any

from pydantic import BaseModel, ValidationError

from streamval.core.result import ValidationResult
from streamval.schema.coerce import SourceFormat, coerce_row


class CompiledValidationPlan:
    """A pre-built validation pipeline for one Pydantic model class.

    Args:
        model: The target Pydantic ``BaseModel`` subclass.
        fmt: The source format used for type coercion.
    """

    __slots__ = ("_model", "_fmt", "_field_names", "_validator")

    def __init__(self, model: type[BaseModel], fmt: SourceFormat) -> None:
        self._model = model
        self._fmt = fmt
        self._field_names: tuple[str, ...] = tuple(model.model_fields.keys())
        self._validator = model.model_validate

    @property
    def model(self) -> type[BaseModel]:
        """The Pydantic model class this plan was built for."""
        return self._model

    @property
    def source_format(self) -> SourceFormat:
        """The source-format tag that drives coercion."""
        return self._fmt

    @property
    def field_names(self) -> tuple[str, ...]:
        """All field names declared on the model."""
        return self._field_names

    def validate_row(
        self,
        row_index: int,
        raw: dict[str, Any],
    ) -> ValidationResult:
        """Coerce, validate, and wrap a single row.

        Args:
            row_index: Zero-based row position in the source stream.
            raw: The adapter's dict for this row.

        Returns:
            A :class:`ValidationResult` — successful or with field errors.
        """
        coerced = coerce_row(raw, self._model, self._fmt)
        try:
            instance = self._validator(coerced)
        except ValidationError as exc:
            return ValidationResult.from_pydantic_error(row_index, raw, exc)
        return ValidationResult.success(row_index, raw, instance)


_PLAN_CACHE: weakref.WeakKeyDictionary[
    type[BaseModel], dict[SourceFormat, CompiledValidationPlan]
] = weakref.WeakKeyDictionary()


def get_plan(
    model: type[BaseModel],
    fmt: SourceFormat,
) -> CompiledValidationPlan:
    """Return a cached :class:`CompiledValidationPlan` for ``(model, fmt)``.

    Plans are cached in a :class:`WeakKeyDictionary`; if the model class
    is garbage-collected the cache entry is dropped automatically.
    """
    by_fmt = _PLAN_CACHE.get(model)
    if by_fmt is None:
        by_fmt = {}
        _PLAN_CACHE[model] = by_fmt
    plan = by_fmt.get(fmt)
    if plan is None:
        plan = CompiledValidationPlan(model, fmt)
        by_fmt[fmt] = plan
    return plan


__all__ = ["CompiledValidationPlan", "get_plan"]
