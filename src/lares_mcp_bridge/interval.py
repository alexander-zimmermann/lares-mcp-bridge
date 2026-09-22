"""The bucket width the time-series tools accept: a Postgres interval literal.

One parser for every place a tool takes a ``bucket`` or ``window``, so the
"is this at least an hour" routing question and the window/bucket ratio are
answered from the same reading of the text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Seconds per unit; a month counts as 30 days, which only matters for ratios.
_UNIT_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2_592_000,
}

_LITERAL_RE = re.compile(
    r"^\s*(\d+)\s*(second|minute|hour|day|week|month)s?\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Interval:
    """A validated ``<count> <unit>`` width; ``unit`` is singular and lower-case."""

    count: int
    unit: str

    @classmethod
    def parse(cls, text: str) -> Interval:
        """Parse ``'15 minutes'``-style text; anything else is ``invalid_bucket_interval``."""
        match = _LITERAL_RE.match(text)
        if not match:
            raise ValueError(f"invalid_bucket_interval: {text!r}")
        return cls(int(match.group(1)), match.group(2).lower())

    @property
    def seconds(self) -> int:
        """Width in seconds, a month counting as 30 days."""
        return self.count * _UNIT_SECONDS[self.unit]

    @property
    def at_least_hourly(self) -> bool:
        """True when the width covers an hour: the threshold for routing to a ``*_1h`` aggregate."""
        return self.seconds >= 3600

    def __floordiv__(self, other: Interval) -> int:
        """How many ``other`` fit into this width, floored; 0 when ``other`` is wider."""
        return self.seconds // other.seconds

    def __str__(self) -> str:
        """The normalised literal, safe to bind as a ``time_bucket`` parameter."""
        return f"{self.count} {self.unit}{'' if self.count == 1 else 's'}"
