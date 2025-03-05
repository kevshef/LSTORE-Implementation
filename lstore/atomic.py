import copy
from queue import Queue
from threading import Event, Thread
from typing import Callable, Generic, Optional, Tuple, TypeVar, Union

T = TypeVar("T")

class Atomic(Generic[T]):
    """Wrapper for variables so that modifications of the same variable
    across different threads are handled sequentially"""

    def __init__(self, var: T):
        self.var: T = var
        self._queue: Queue[
            Tuple[Callable[[T], T], Optional[Callable[[T], None]], bool]
        ] = Queue()
        self._put_event = Event()  # Set this event to check the queue
        self._del_event = Event()  # Set this event to stop handling the queue
        self._thread = Thread(target=self._handle_queue, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """
        Set del event so queue handler thread
        stops looping and finishes whatever is left of the queue
        """
        self._del_event.set()
        self._thread.join()

    def modify(
        self,
        callable: Callable[[T], T],
        after: Union[Callable[[T], None]] | None = None,
        pass_old: bool = False,
    ) -> None:
        self._queue.put((callable, after, pass_old))
        self._put_event.set()

    def get(self) -> T:
        """
        Retrieves the variable managed by the Atomic class, intended for read only access.

        Returns:
            T: Variable managed by the Atomic class
        """
        return self.var

    def copy(self) -> T:
        """
        Returns a deep copy of the variable managed by the Atomic class.

        Returns:
            T: Variable managed by the Atomic class
        """
        return copy.deepcopy(self.var)

    def _next_modification(self):
        item = self._queue.get_nowait()
        old: T = self.var
        modification: Callable[[T], T] = item[0]
        self.var = modification(self.var)
        after: Callable[[T], T] = item[1]
        pass_old: bool = item[2]
        if after:
            Thread(
                target=lambda: after(self.var if not pass_old else old), daemon=True
            ).start()

    def _handle_queue(self) -> None:
        self._put_event.wait()
        timeout: int = 1
        while not self._del_event.is_set():
            if self._queue.qsize():
                self._next_modification()
                timeout = 1
            else:
                timeout *= 2
                self._put_event.clear()
                self._put_event.wait(timeout)
        while self._queue.qsize():  # Finish remaining items in queue
            self._next_modification()

    def __del__(self) -> None:
        self.stop()
