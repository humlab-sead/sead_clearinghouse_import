import abc

from ..metadata import Metadata
from ..submission import Submission


class IDispatcher(abc.ABC):

    def dispatch(
        self,
        metadata: Metadata,
        submission: Submission,
        table_names: list[str] = None,
        extra_names: list[str] = None,
    ):
        raise NotImplementedError
