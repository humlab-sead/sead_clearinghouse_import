import abc

from ..metadata import SeadSchema
from ..submission import Submission


class IDispatcher(abc.ABC):

    def dispatch(
        self,
        schema: SeadSchema,
        submission: Submission,
        table_names: list[str] | None = None,
        extra_names: list[str] | None = None,
    ):
        raise NotImplementedError
