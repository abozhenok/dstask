import os
import inspect

from typing import Dict
from .xcom import XCom
from .events import CancelTaskEvent
from .backend import XcomBackend
from functools import wraps


def ds_task(fn=None, backend: XcomBackend = None, last_task: bool = False):
    def execute(fn):
        def wrapper(*args):
            workflow_id = os.environ.get("WORKFLOW_ID", None)

            if workflow_id is None:
                raise RuntimeError(
                    f"Failed to set WORKFLOW_ID, tasks must have a unique key"
                )

            signature = inspect.signature(fn)
            kwargs = {
                k: v.default
                for k, v in signature.parameters.items()
                if v.default is not inspect.Parameter.empty
            }
            args_spec = inspect.getfullargspec(fn).args
            args_to_pull = args_spec[len(args) :]
            # remove arg to pull if in kwargs
            args_to_pull = [arg for arg in args_to_pull if arg not in kwargs]
            # task_id must be set if you want to execute the same task in parallel
            x_com = XCom(backend, workflow_id, os.environ.get("TASK_ID", None))
            task_vars = x_com.pull_arguments(args_to_pull)

            @wraps(fn)
            def inner(*args):
                clean_up = last_task

                try:
                    returns = fn(*args)
                    # store return types
                    if returns is not None:
                        if not isinstance(returns, Dict):
                            raise RuntimeError(
                                f"Task returned {type(returns)}, expected 'dict'"
                            )
                        x_com.return_data(**returns)
                except CancelTaskEvent:
                    clean_up = True

                if clean_up:
                    x_com.clean_up()

            return inner(*args, *task_vars)

        return wrapper

    if backend is None:
        raise Exception("Backend service not initialized")

    if fn:
        return execute(fn)

    return execute
