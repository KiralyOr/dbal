"""Shared types for the DBAL package."""

from typing import Any

Row = dict[str, Any]
Params = tuple | list | dict
ParamsList = list[tuple] | list[list]
