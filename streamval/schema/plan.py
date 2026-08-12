"""CompiledValidationPlan — cached, per-model validation pipeline.

A plan is constructed once per Pydantic model class and reused for every
row. It holds the validator callable, the field map, and the source
format, so per-row work stays minimal. Plans are cached in a
module-level :class:`weakref.WeakKeyDictionary` keyed by model class so
short-lived models can be garbage-collected.
"""

from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, TypeAdapter, ValidationError

from streamval.core.result import ValidationResult
from streamval.schema.coerce import SourceFormat, cast_csv_batch, coerce_row

if TYPE_CHECKING:
    import pyarrow as pa


class CompiledValidationPlan:
    """A pre-built validation pipeline for one Pydantic model class.

    Args:
        model: The target Pydantic ``BaseModel`` subclass.
        fmt: The source format used for type coercion.

    The plan exposes both a per-row validator (:meth:`validate_row`)
    and a bulk validator (:meth:`validate_batch`). The bulk path uses
    a :class:`pydantic.TypeAdapter` compiled at construction time so
    that ``validate_python`` hands an entire list of coerced rows to
    pydantic-core's Rust layer in a single boundary crossing.
    """

    __slots__ = (
        "_model",
        "_fmt",
        "_field_names",
        "_validator",
        "_list_adapter",
    )

    def __init__(self, model: type[BaseModel], fmt: SourceFormat) -> None:
        self._model = model
        self._fmt = fmt
        self._field_names: tuple[str, ...] = tuple(model.model_fields.keys())
        self._validator = model.model_validate
        # ``list[model]`` is a legitimate runtime parametrisation but
        # mypy can't follow it because ``model`` is a runtime value.
        # Construct via Any to keep the function strict-clean.
        list_type = cast(Any, list)[model]
        self._list_adapter: TypeAdapter[list[BaseModel]] = TypeAdapter(list_type)

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

    def validate_batch(
        self,
        rows: list[dict[str, Any]],
        *,
        start_index: int = 0,
    ) -> list[ValidationResult]:
        """Validate a list of rows in one Rust-boundary crossing when possible.

        The fast path coerces every row, then calls
        :meth:`TypeAdapter.validate_python` once on the whole list. If
        that succeeds, all rows are valid and we wrap the parsed model
        instances directly.

        If it raises, the failing rows are identified from the
        ``ValidationError`` — a list adapter reports each error with the
        element index as the first entry of ``loc`` — and only those rows
        are re-validated one at a time to recover their exact error
        detail. The remaining rows go back through the bulk validator in
        a single call.

        This matters because the naive alternative (re-validating the
        whole batch per row) makes throughput depend on whether a batch
        happens to contain *any* bad row, rather than on how many. At
        ``batch_size=10000``, 20 bad rows in 200 000 is enough to put
        every batch on the slow path and roughly halve throughput.

        Args:
            rows: The batch of raw row dicts to validate.
            start_index: Row index of the first row in ``rows`` (used
                when constructing :class:`ValidationResult` objects so
                global row numbering is preserved).

        Returns:
            A list of :class:`ValidationResult` in input order, one per
            input row. Identical to validating each row individually.
        """
        if not rows:
            return []

        coerced = [coerce_row(r, self._model, self._fmt) for r in rows]

        try:
            instances = self._list_adapter.validate_python(coerced)
        except ValidationError as exc:
            return self._split_failed_batch(rows, coerced, exc, start_index)

        return [
            ValidationResult.success(start_index + i, rows[i], instances[i])
            for i in range(len(rows))
        ]

    def _validate_each(
        self,
        rows: list[dict[str, Any]],
        start_index: int,
    ) -> list[ValidationResult]:
        """Per-row validation — the reference behaviour and last resort."""
        return [
            self.validate_row(start_index + i, rows[i]) for i in range(len(rows))
        ]

    def _split_failed_batch(
        self,
        rows: list[dict[str, Any]],
        coerced: list[dict[str, Any]],
        exc: ValidationError,
        start_index: int,
    ) -> list[ValidationResult]:
        """Recover from a failed bulk validation without re-doing every row.

        Falls back to :meth:`_validate_each` whenever the failing rows
        can't be localised with confidence, so the result is always the
        same as validating row by row.
        """
        n = len(rows)
        bad: set[int] = set()
        for err in exc.errors():
            loc = err.get("loc")
            if not loc or not isinstance(loc[0], int):
                # Not an element-level error (or an unexpected shape) —
                # don't guess which rows are implicated.
                return self._validate_each(rows, start_index)
            bad.add(loc[0])

        if not bad or any(i < 0 or i >= n for i in bad):
            return self._validate_each(rows, start_index)

        good_positions = [i for i in range(n) if i not in bad]
        results: list[ValidationResult | None] = [None] * n

        if good_positions:
            try:
                good = self._list_adapter.validate_python(
                    [coerced[i] for i in good_positions]
                )
            except ValidationError:
                # The rows we believed were fine aren't; distrust the
                # whole split and do it the slow, certain way.
                return self._validate_each(rows, start_index)
            for pos, i in enumerate(good_positions):
                results[i] = ValidationResult.success(
                    start_index + i, rows[i], good[pos]
                )

        for i in bad:
            results[i] = self.validate_row(start_index + i, rows[i])

        return [r for r in results if r is not None]

    def validate_record_batch(
        self,
        batch: pa.RecordBatch,
        *,
        start_index: int = 0,
    ) -> list[ValidationResult]:
        """Validate a whole :class:`pyarrow.RecordBatch` in one Rust crossing.

        The Arrow fast path: converts the RecordBatch to Python row
        dicts once (in C, via :meth:`pyarrow.RecordBatch.to_pylist`)
        rather than constructing N dicts in a Python loop, then hands
        the resulting list to :meth:`validate_batch`.

        For CSV the columns arrive as strings, so they are first cast
        column-wise via :func:`cast_csv_batch`. That moves the bulk of
        type coercion out of the per-row Python loop and into Arrow,
        and leaves :func:`coerce_row` with nothing to do for any column
        that cast cleanly.

        Args:
            batch: A :class:`pyarrow.RecordBatch` of rows to validate.
            start_index: Row index of the first row in ``batch``.

        Returns:
            A list of :class:`ValidationResult` in batch order.
        """
        if self._fmt is SourceFormat.CSV:
            batch = cast_csv_batch(batch, self._model)
        rows: list[dict[str, Any]] = batch.to_pylist()
        return self.validate_batch(rows, start_index=start_index)


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
