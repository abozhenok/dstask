class TaskEvent(Exception):
    """
    Base class for all events
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class CancelTaskEvent(TaskEvent):
    """
    Cancels current task.
    Inherit from this if you want to perform certain functions.
    """

    def __init__(self, message: str = "Task cancelled"):
        super().__init__(message)
