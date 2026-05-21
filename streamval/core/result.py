"""Per-row result containers and the package's top-level exception.

Defines:

* :class:`FieldError` — a single field-level validation failure.
* :class:`ValidationResult` — the per-row outcome (valid or invalid).
* :class:`StreamValidationError` — raised by the ``fail_fast`` strategy
  and by ``collect`` when ``max_errors`` is exceeded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from streamval.core.stats import StreamStats


@dataclass(frozen=True)
class FieldError:
    """A single field-level validation failure.

    Attributes:
        field: Dotted path of the offending field (e.g. ``"address.zip"``).
        value: The raw value that failed validation.
        message: Human-readable description of the failure.
        error_type: Pydantic error type string (e.g. ``"int_parsing"``).
    """

    field: str
    value: Any
    message: str
    error_type: str

    def __str__(self) -> str:
        return f"{self.field}={self.value!r}: {self.message} [{self.error_type}]"


@dataclass(frozen=True)
class ValidationResult:
    """The outcome of validating a single row.

    Attributes:
        row_index: Zero-based row index in the source stream.
        raw: The original (un-coerced) row as a dict.
        valid: ``True`` if all fields passed validation.
        data: The parsed Pydantic model instance, or ``None`` if invalid.
        errors: Field-level errors; empty when ``valid`` is True.
    """

    row_index: int
    raw: dict[str, Any]
    valid: bool
    data: BaseModel | None
    errors: list[FieldError] = field(default_factory=list)

    @classmethod
    def success(
        cls,
        row_index: int,
        raw: dict[str, Any],
        data: BaseModel,
    ) -> ValidationResult:
        """Build a successful result for a row that passed validation."""
        return cls(
            row_index=row_index,
            raw=raw,
            valid=True,
            data=data,
            errors=[],
        )

    @classmethod
    def from_pydantic_error(
        cls,
        row_index: int,
        raw: dict[str, Any],
        exc: ValidationError,
    ) -> ValidationResult:
        """Build a failed result from a Pydantic ``ValidationError``.

        Each entry in ``exc.errors()`` is converted to a :class:`FieldError`.
        """
        errors: list[FieldError] = []
        for err in exc.errors():
            loc = err.get("loc", ())
            field_name = ".".join(str(p) for p in loc) if loc else "<root>"
            errors.append(
                FieldError(
                    field=field_name,
                    value=err.get("input"),
                    message=str(err.get("msg", "")),
                    error_type=str(err.get("type", "")),
                )
            )
        return cls(
            row_index=row_index,
            raw=raw,
            valid=False,
            data=None,
            errors=errors,
        )

    def __str__(self) -> str:
        if self.valid:
            return f"ValidationResult(row={self.row_index}, valid=True)"
        return (
            f"ValidationResult(row={self.row_index}, valid=False, "
            f"errors={len(self.errors)})"
        )


class StreamValidationError(Exception):
    """Raised when an error strategy decides the stream cannot continue.

    Used by:
        * ``fail_fast`` — on the first invalid row.
        * ``collect`` — when ``max_errors`` is exceeded at finalize.

    Attributes:
        message: Human-readable summary.
        results: Invalid :class:`ValidationResult` objects accumulated so far.
        stats: Best-effort :class:`StreamStats` snapshot, if available.
    """

    def __init__(
        self,
        message: str,
        results: list[ValidationResult] | None = None,
        stats: StreamStats | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.results: list[ValidationResult] = list(results) if results else []
        self.stats: StreamStats | None = stats

    def __str__(self) -> str:
        n = len(self.results)
        if n == 0:
            return self.message
        counts: dict[str, int] = {}
        for res in self.results:
            for err in res.errors:
                counts[err.field] = counts.get(err.field, 0) + 1
        top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:3]
        top_str = ", ".join(f"{name} ({n_})" for name, n_ in top) or "n/a"
        return f"{self.message} ({n} invalid rows; top fields: {top_str})"

    def __repr__(self) -> str:
        return (
            f"StreamValidationError(message={self.message!r}, "
            f"results=<{len(self.results)} items>)"
        )
