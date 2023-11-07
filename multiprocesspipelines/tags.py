from .tag_factory import output_tag_factory, input_tag_factory


outputs_file_information_iterator = output_tag_factory(
    "outputs_file_information_iterator", "file_information_iterator"
)
outputs_file_information_iterator.__doc__ = """ Decorator to tag a function as outputting a file iterator. A file iterator is an iterator that yields file path(s) used as file_information for the first process."""

recieves_file_information = input_tag_factory(
    "recieves_file_information", "file_information"
)
outputs_file_information_iterator.__doc__ = """ Decorator to tag a function as recieving a file iterator. A file iterator is an iterator that yields file path(s)."""


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

    def iterate_decorator(func: Callable[P, T]) -> Callable[P, T]:
        if hasattr(func, "iterate"):
            raise AttributeError(f"iterate already specified for given process")
        else:
            func.iterate = True
            func.iterate_kwargs = {
                "num_iters": num_iters,
                "seed": seed,
                "max_workers": max_workers,
            }
        return func

    iterate_decorator.__name__ = "iterate"
    return iterate_decorator


@outputs("dataframe")
@inputs({"dataframe":"df", "image_fov":"img_fov"})
@iterate(num_iters=10, seed=0)
def zscore(
    df,
    img_fov,
):
    
    return 

inputs(a1, a2):
    intputs(func):
        func(a1, a2)
        
feature_extraction.dataframe = dataframe

kwargs[]