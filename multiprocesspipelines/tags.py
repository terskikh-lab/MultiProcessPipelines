from .tag_factory import output_tag_factory, input_tag_factory


def inputs(*args, **kwargs):
    """
    Decorator to specify a processes inputs, both positional and keyword arguments.

    Parameters
    ----------
    *args
        String names of the positional input arguments to pass to the process.
    **kwargs
        Keyword mapping of names from outputs of previous processes to keyword inputs of this process.
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        if hasattr(func, "inputs"):
            raise AttributeError(f"inputs already specified for given process")
        else:
            func.inputs = {
                "args": args,
                "kwargs": kwargs,
            }
        return func

    decorator.__name__ = "inputs"
    return decorator


def outputs(*args):
    """
    Decorator to specify a processes outputs. All outputs are by definition positional

    Parameters
    ----------
    *args
        Positional arguments to pass to the function.
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        if hasattr(func, "outputs"):
            raise AttributeError(f"outputs already specified for given process")
        else:
            func.outputs = {
                "args": args,
            }
        return func

    decorator.__name__ = "outputs"
    return decorator


def iterate(num_iters: int, seed: int, max_workers: int = 10):
    """
    Decorator to tag a function as iterating over a process, executing iterations in parallel.

    Parameters
    ----------
    num_iters : int
        Number of iterations to execute.
    seed : int
        Seed to use for random number generation.
    max_workers : int, optional
        Maximum number of workers to use for parallelization, by default 10
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        if hasattr(func, "iterate"):
            raise AttributeError(f"iterate already specified for given process")
        else:
            func.iterate = {
                "kwargs": {
                    "num_iters": num_iters,
                    "seed": seed,
                    "max_workers": max_workers,
                }
            }
        return func

    decorator.__name__ = "iterate"
    return decorator
