"""Stable errors exposed by the matcher CLI and runtime."""

from dataclasses import dataclass


@dataclass
class MatcherError(Exception):
    """A machine-readable failure safe for scripts to classify."""

    code: str
    message: str
    exit_code: int

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


def fail(code: str, message: str, exit_code: int) -> MatcherError:
    """Build a stable runtime error."""
    return MatcherError(code, message, exit_code)
