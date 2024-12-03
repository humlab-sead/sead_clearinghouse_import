import abc

class IDispatcher(abc.ABC):

    def dispatch(self, data):
        raise NotImplementedError
    