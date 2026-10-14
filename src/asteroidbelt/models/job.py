"""
Job is a named request to run a function somewhere else.
"""

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class Job:
    """
    A function invocation with a unique name, a cpu request, and a
    wall time budget in seconds.
    """

    name: str
    function: Callable
    arguments: tuple = ()
    keywords: dict[str, Any] = field(default_factory=dict)
    cpus: float = 1.0
    ttl: float = 3600.0
