import abc
import io

from ..metadata import SeadSchema
from ..submission import Submission


class IDispatcher(abc.ABC):

    def __init__(self, outstream: io.TextIOBase) -> None:
        self.outstream = outstream
        
    def dispatch(
        self,
        schema: SeadSchema,
        submission: Submission,
        table_names: list[str] | None = None,
        extra_names: list[str] | None = None,
    ):
        raise NotImplementedError
