from typing import List, Tuple, Union, Any


class XCom:
    """
    A data store used for communicating variables and state between tasks in a distributed system
    """
    def __init__(
        self,
        backend,
        workflow_id: str = None,
        task_id: str = None
    ):
        self.backend = backend
        # the workflow must have a unique id
        self.workflow_id = workflow_id
        # must be set if you want to execute the same task in parallel
        self.task_id = task_id

    def _unique_id(self):
        if self.task_id:
            return f"{self.workflow_id}:{self.task_id}"
        return self.workflow_id

    def return_data(self, **kwargs) -> None:
        unique_id = self._unique_id()
        for key, value in kwargs.items():
            self.backend.push(f"{unique_id}:{key}", value)

    def pull_arguments(self, keys: List[str]) -> Union[Tuple, Any]:
        unique_id = self._unique_id()
        returns = [
            self.backend.pull(f"{unique_id}:{key}")
            for key in keys
        ]

        if len(returns) == 1:
            return returns[0]

        return tuple(returns)

    def clean_up(self) -> None:
        self.backend.teardown()
