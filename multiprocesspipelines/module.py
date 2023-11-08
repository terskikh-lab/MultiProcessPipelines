from multiprocesstools import (
    MultiProcessHelper,
    RunTimeCounter,
    wait_until_file_exists,
    run_func_IO_loop,
)
from typing import Union, Callable
from functools import partial
import logging
import hashlib
import pickle
import os

logger = logging.getLogger(__name__)


def _get_representation(v):
    if isinstance(v, (int, float, str, bool, type(None))):
        return str(v)
    elif isinstance(v, (list, tuple)) and len(v) <= 5:
        return f"{type(v).__name__}({', '.join([_get_representation(i) for i in v])})"
    else:
        v_hash = hashlib.sha256(pickle.dumps(v)).hexdigest()[:10]
        return f"{type(v).__name__}({v_hash})"


def process_summary(process):
    summary = [f"Process: {process.__name__}"]
    summary.append("inputs:")
    summary.append("\targs:")
    if hasattr(process, "inputs"):
        args = process.inputs["args"]
        kwargs = process.inputs["kwargs"]
        for arg in args:
            summary.append(f"\t\t- {arg}")
        summary.append("\tkwargs:")
        for k, v in kwargs.items():
            summary.append(f"\t\t- {k}: {v}")
    if hasattr(process, "iterate"):
        summary.append("\titerate kwargs:")
        for kwarg in process.iterate["kwargs"]:
            summary.append(f"\t\t- {kwarg}")
    if hasattr(process, "outputs"):
        summary.append("outputs:")
        for output in process.outputs["args"]:
            summary.append(f"\t{output}")
    return "\n".join(summary) + "\n"


class Module(MultiProcessHelper):
    def __init__(
        self,
        name,
        output_directory,
        loggers,
    ):
        super().__init__(
            name=name,
            working_directory=os.path.join(output_directory, name),
            loggers=loggers,
        )
        self.name = name
        self._processes = {}

    @property
    def methods_list(self):
        return [
            method_name
            for method_name in dir(self)
            if not method_name.startswith("_") and callable(getattr(self, method_name))
        ]

    @property
    def processes_list(self):
        return [process for process in self._processes.keys()]

    def set_file_information_iterable(self, function):
        if isinstance(function, Callable):
            if hasattr(function, "outputs"):
                if "file_information_iterable" not in function.outputs["args"]:
                    raise ValueError(
                        "file_information_iterable must have 'file_information_iterable' in outputs"
                    )
            else:
                raise AttributeError(
                    f"outputs not specified for file_information_iterable (NOTE: file_information_iterable must have 'file_information_iterable' in outputs)"
                )
            partial_func = partial(function, *args, **kwargs)
            partial_func.__name__ = function.__name__
            partial_func.outputs = function.outputs
            self.file_information_iterable = partial_func

            prior_names = []
            for file_information_name, file_information in tqdm(
                self.file_information_iterable,
                f"Checking {function.__name__} viability",
            ):
                if not isinstance(file_information_name, str):
                    raise ValueError(
                        f"file_information_name must be a string but {type(file_information_name)} was given"
                    )
                if file_information_name in prior_names:
                    raise ValueError(
                        f"file_information_name must be unique but {file_information_name} was repeated"
                    )
                prior_names.append(file_information_name)
            logger.info(f"{function.__name__} is a viable file_information_iterable")

        else:
            raise ValueError(
                f"function must be a Callable but {type(function)} was given"
            )

    def add_process(self, function: Callable, *args, **kwargs):
        if isinstance(function, Callable):
            partial_func = partial(function, *args, **kwargs)
            partial_func.__name__ = function.__name__
            if hasattr(function, "inputs") and hasattr(function, "outputs"):
                partial_func.inputs = function.inputs
                partial_func.outputs = function.outputs
            else:
                raise AttributeError(
                    f"inputs or outputs not specified for given process"
                )
            if hasattr(function, "iterate"):
                partial_func.iterate = function.iterate
            if len(self.processes_list) == 0:
                if (
                    "file_information" not in function.inputs["args"]
                    and "file_information" not in function.inputs["kwargs"]
                ):
                    raise ValueError(
                        f"First process must have 'file_information' in inputs"
                    )
            self._processes[function.__name__] = partial_func
        else:
            raise ValueError(
                f"function must be a Callable but {type(function)} was given"
            )

    def get_process(
        self,
        process,
    ):
        if process in self._processes.keys():
            return self._processes[process]
        else:
            print(
                f"process not in processes.\n\nGiven:{process} Processes:\n\n{self.processes_list}"
            )

    def remove_process(self, process):
        if process in self._processes.keys():
            del self._processes[process]
        else:
            print(
                f"process not in processes.\n\nGiven:{process} Processes:\n\n{self.processes_list}"
            )

    def clear_all_processes(self):
        self._processes = {}

    def run(self):
        if not hasattr(self, "file_information_iterable"):
            raise AttributeError(
                f"file_information_iterable not specified for {self.name}"
            )

        logger.info(f"Running {self.name}...")
        # initialize run information
        try:
            self.create_directory("file_information_tempfiles")
            for file_information_name, file_information in tqdm(
                self.file_information_iterable, f"{self.name} progress"
            ):
                temp_file_name = f"{file_information_name}.tmp"
                final_file_name = f"{file_information_name}.fin"
                file_not_in_use = self.create_temp_file(
                    final_file_name=final_file_name,
                    temp_file_name=temp_file_name,
                    path="file_information_tempfiles",
                )
                if file_not_in_use == False:
                    logger.debug(f"File {temp_file_name} already exists, skipping...")
                    continue
                self.file_information_name = file_information_name
                self.file_information = file_information
                self._run_all_processes()
                open(
                    os.path.join(
                        self.get_directory("file_information_tempfiles"),
                        final_file_name,
                    ),
                    "x",
                ).close()
                self.delete_tempfile(
                    os.path.join(
                        self.get_directory("file_information_tempfiles"),
                        temp_file_name,
                    )
                )
            logger.info(f"Finished running {self.name}, cleaning up...")
            self.cleanup()
        except Exception as e:
            logger.exception(e)
            logger.error("An exception occurred, cleaning up...")
            self.cleanup()

    def _run_all_processes(self):
        if len(self._processes) == 0:
            raise ValueError("No processes specified")
        self.output_directory = self.get_directory("working_directory")
        for i, (name, process) in enumerate(self._processes.items()):
            logger.info(f"Running process {i}: {name}")
            if (i == 0) and ("file_information" not in process.inputs):
                raise ValueError(
                    f"First process must have 'file_information' in inputs"
                )

            args = []
            for arg in process.inputs["args"]:
                if arg is None:
                    continue
                if hasattr(self, arg):
                    args.append(self.__getattribute__(arg))
                else:
                    raise AttributeError(
                        f"Invalid input: {arg}. {arg} does not exist as an attribute. There may be an issue with the previous process output decorator"
                    )

            kwargs = {}
            for process_kwarg, attr_name in process.inputs["kwargs"]:
                # if process_kwarg is None:
                #     continue
                if hasattr(self, attr_name):
                    kwargs[process_kwarg] = self.__getattribute__(attr_name)
                else:
                    raise AttributeError(
                        f"Invalid input: {process_kwarg}={attr_name}. {attr_name} does not exist as an attribute. There may be an issue with the previous process output decorator"
                    )
            if process.hasattr("iterate"):
                if any(i in kwargs.keys() for i in process.iterate["kwargs"].keys()):
                    overlapping = [
                        i
                        for i in kwargs.keys()
                        if i in process.iterate["kwargs"].keys()
                    ]
                    raise ValueError(
                        f"iterate kwargs and process kwargs cannot overlap. Overlapping kwargs: {overlapping}"
                    )
                kwargs.update(process.iterate["kwargs"])
                self.create_directory("iteration_tempfiles")
                self._run_iterations_in_parallel(process=process, *args, **kwargs)
            else:
                output = process(*args, **kwargs)
                if (len(process.outputs["args"]) != 1) and (
                    len(output) != len(process.outputs["args"])
                ):
                    output_args = process.outputs["args"]
                    raise ValueError(
                        f"Output length does not match process output info: expected {output_args} ({len(output_args)}) but the process returned {len(output)} args"
                    )
                for i, attr in enumerate(process.outputs["args"]):
                    outi = output[i] if len(process.outputs["args"]) > 1 else output
                    if attr is None:
                        continue
                    if hasattr(self, attr):
                        logger.warning(f"Updating {attr}...")
                        self.__setattr__(attr, outi)
                    else:
                        self.__setattr__(attr, outi)

    def _run_iterations_in_parallel(
        self,
        process: Callable,
        *args,
        **kwargs,
    ):
        for i in ["num_iters", "seed", "max_workers"]:
            assert i in kwargs.keys(), f"{i} must be in kwargs"
        num_iters = kwargs["num_iters"]
        seed = kwargs["seed"]
        max_workers = kwargs.pop("max_workers")
        kwargs_str = "_".join(
            [f"{k}={get_representation(v)}" for k, v in kwargs.items()]
        )
        label = f"{process.__name__}-{kwargs_str}"
        rng = np.random.default_rng(seed=seed)
        try:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures_dict = {}
                for i in np.arange(num_iters):
                    temp_file_name = f"{label}_iter{i}.tmp"
                    final_file_name = temp_file_name.replace("tmp", "fin")
                    file_not_in_use = self.create_temp_file(
                        final_file_name=final_file_name,
                        temp_file_name=temp_file_name,
                        path="iteration_tempfiles",
                    )
                    if file_not_in_use == False:
                        logger.debug(
                            f"File {temp_file_name} already exists, skipping..."
                        )
                        continue
                    kwargs["seed"] = rng.integers(low=0, high=1000000)
                    future = executor.submit(process, **kwargs)
                    futures_dict[future] = temp_file_name
                for i, future in tqdm(
                    enumerate(as_completed(futures_dict)), "Progress on futures"
                ):
                    temp_file_name = futures_dict[future]
                    result = future.result()
                    logger.info(f"{label}: {result}")
                    open(
                        os.path.join(
                            self.get_directory("iteration_tempfiles"),
                            final_file_name,
                        ),
                        "x",
                    ).close()
                    self.delete_tempfile(
                        os.path.join(
                            self.get_directory("iteration_tempfiles"),
                            temp_file_name,
                        )
                    )
                executor.shutdown(wait=True)
        except Exception as e:
            logger.error(e)
            self.cleanup()
            if not isinstance(e, KeyError):
                raise e
        finally:
            logger.info(f"Finished running multiprocessing for {process.__name__}")
            if len(self.tempfiles) > 0:
                logger.info("Tempfiles remaining:")
                for tempfile in self.tempfiles:
                    logger.info(tempfile)

        def __repr__(self):
            repr_parts = [f"\nModule: {self.name}"]
            repr_parts.append("Process info:")
            for process_name, process in self._processes.items():
                repr_parts.append(process_summary(process))
            return "\n".join(repr_parts)


# write the dunder method to show the repr of the object as a list of processes with input / output summary information
