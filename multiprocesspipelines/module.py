import logging
import numpy as np
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocesstools import MultiProcessHelper
from typing import Union, Callable, Iterable, Dict
from functools import partial

from .tools import _get_representation

logger = logging.getLogger(__name__)


def process_summary(process):
    summary = []
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
    if hasattr(process, "other_module_inputs"):
        summary.append("\tother_module_inputs:")
        for k, v in process.other_module_inputs["kwargs"].items():
            summary.append(f"\t\t- {k}: {v}")
    if hasattr(process, "iterate"):
        summary.append("\titerate kwargs:")
        for kwarg in process.iterate["kwargs"]:
            summary.append(f"\t\t- {kwarg}")
    if hasattr(process, "outputs"):
        summary.append("outputs:")
        for output in process.outputs["args"]:
            summary.append(f"\t - {output}")
    return "\n".join(summary) + "\n"


class ProcessOutputs:
    def __init__(self):
        self._outputs = {}

    def __getitem__(self, key):
        return self._outputs[key]

    def __setitem__(self, key, value):
        if key in self._outputs:
            raise ValueError(f"Output {key} already exists in process outputs")
        self._outputs[key] = value

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        elif attr in self._outputs:
            return self._outputs[attr]
        else:
            raise AttributeError(f"No such attribute: {attr}")

    def track_process(self, process: Callable):
        for input in process.inputs["args"]:
            if input is None:
                continue
            if not hasattr(self, input):
                raise ValueError(
                    f"Output {input} not found in previous processes. Make sure to add the process dependencies to the module before tracking this process"
                )
        for input, attr_name in process.inputs["kwargs"]:
            if not hasattr(self, attr_name):
                raise ValueError(
                    f"Output {attr_name} not found in previous processes. Make sure to add the process dependencies to the module before tracking this process"
                )
        for output in process.outputs["args"]:
            self[output] = None

    def __repr__(self):
        repr_parts = ["ProcessOutputs:"]
        for output, value in self._outputs.items():
            repr_parts.append(f"\t{output}: {value}")
        return "\n".join(repr_parts)

    def merge(self, other):
        for key, value in other._outputs.items():
            if key not in self._outputs:
                self._outputs[key] = value
            elif self._outputs[key] != value:
                if isinstance(self._outputs[key], list):
                    self._outputs[key].append(value)
                else:
                    self._outputs[key] = [self._outputs[key], value]

    @classmethod
    def aggregate(cls, outputs_list):
        merged = cls()
        for outputs in outputs_list:
            merged.merge(outputs)
        return merged


class Module:

    file_information_iterable: Iterable

    def __init__(
        self,
        name: str,
        output_root_directory: Union[str, Path],
        loggers: list,
    ):
        if not isinstance(name, str):
            raise ValueError(f"name must be a string but {type(name)} was given")
        if not isinstance(output_root_directory, Path):
            try:
                output_root_directory = Path(output_root_directory)
            except Exception as e:
                logger.exception(e)
                raise ValueError(
                    f"output_root_directory must be a string or Path but {type(output_root_directory)} was given"
                )
        if not isinstance(loggers, list):
            raise ValueError(f"loggers must be a list but {type(loggers)} was given")
        loggers = [__name__, "MultiProcessTools", *loggers]

        self.module_output_directory: Path = output_root_directory / name
        self.module_output_directory.mkdir(parents=True, exist_ok=True)

        self.multiprocesshelper = MultiProcessHelper(
            name=name,
            output_root=self.module_output_directory / "multiprocesshelper",
            logger_names=loggers,
        )
        self.name = name
        self._processes: Dict = {}
        self.other_module_inputs: Dict = {}
        self.outputs: ProcessOutputs = ProcessOutputs()

    @property
    def multiprocesshelper_directory(self) -> Path:
        return self.multiprocesshelper.get_directory("working_directory")

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

    @property
    def process_outputs(self):
        all_outputs = list([*self.outputs.keys()])
        return all_outputs

    def set_file_information_iterable(self, function, *args, **kwargs):
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
            outputs = function(*args, **kwargs)
            if (len(outputs) != len(function.outputs["args"])) and (
                len(function.outputs["args"] > 1)
            ):
                raise ValueError(
                    f"Output length does not match process output info: expected {function.outputs['args']} ({len(function.outputs['args'])}) but the process returned {len(outputs)} args"
                )
            for attr, val in zip(function.outputs["args"], outputs):
                if attr is None:
                    continue
                if hasattr(self, attr):
                    raise ValueError(
                        f"file_information_iterable cannot have overlapping outputs with other processes. Overlapping output: {attr}"
                    )
                self.__setattr__(attr, val)
            assert hasattr(self, "file_information_iterable")

            prior_names = []
            for i, item in tqdm(
                enumerate(self.file_information_iterable),
                f"Checking {function.__name__} viability",
            ):
                if len(item) != 2:
                    raise ValueError(
                        f"file_information_iterable must return a tuple of length 2 but {len(item)} was returned"
                    )
                file_information_name, file_information = item
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

            self.multiprocesshelper.track_process("file_information_iterable")

        else:
            raise ValueError(
                f"function must be a Callable but {type(function)} was given"
            )

    def add_process(self, function: Callable, *args, **kwargs):
        if isinstance(function, Callable):
            partial_func = partial(function, *args, **kwargs)
            partial_func.__name__ = function.__name__
            # Rather than partial init just create a kwargs and args attr
            # that way we can make them mutable and keep original func methods (like func.__code__)
            if hasattr(function, "inputs"):
                partial_func.inputs = function.inputs
            if hasattr(function, "other_module_inputs"):
                partial_func.other_module_inputs = function.other_module_inputs
                self.other_module_inputs[function.__name__] = (
                    function.other_module_inputs
                )
            if not (
                hasattr(function, "inputs") or hasattr(function, "other_module_inputs")
            ):
                raise AttributeError(f"inputs not specified for given process")
            if hasattr(function, "outputs"):
                partial_func.outputs = function.outputs
            else:
                raise AttributeError(f"outputs not specified for given process")
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
            self.multiprocesshelper.track_process(function.__name__)
            self.outputs.track_process(partial_func)
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
            logger.warning(
                f"process not in processes.\n\nGiven:{process} Processes:\n\n{self.processes_list}"
            )

    def remove_process(self, process):
        if process in self._processes.keys():
            del self._processes[process]
        else:
            logger.warning(
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
            for file_information_name, file_information in tqdm(
                self.file_information_iterable, f"{self.name} progress"
            ):
                file_name = f"{file_information_name}_{self.name}"
                success = self.multiprocesshelper.create_process_file(
                    process_name="file_information_iterable", file_name=file_name
                )
                if success == False:
                    logger.debug(f"File {file_name} already exists, skipping...")
                    continue
                logger.info(f"Running {file_information_name}...")
                self.file_information_name = file_information_name
                self.file_information = file_information
                try:
                    self._run_all_processes()
                except Exception as e:
                    logger.exception(e)
                    self.multiprocesshelper.update_process_file(
                        process_name="file_information_iterable",
                        file_name=file_name,
                        status="failed",
                    )
                    continue
                else:
                    self.multiprocesshelper.update_process_file(
                        process_name="file_information_iterable",
                        file_name=file_name,
                        status="finished",
                    )
            logger.info(f"cleaning up...")
            self.multiprocesshelper.cleanup()
            logger.info(f"Finished running {self.name}")
        except Exception as e:
            logger.exception(e)
            logger.error("An exception occurred, cleaning up...")
            self.multiprocesshelper.cleanup()

    def _run_all_processes(self, other_module_inputs: Dict):
        if len(self._processes) == 0:
            raise ValueError("No processes specified")
        for i, (name, process) in enumerate(self._processes.items()):
            logger.info(f"Running process {i}: {name}")
            # Assemble all args and kwargs
            # do this by checking if the process has inputs and outputs
            # if it does, then we can use the inputs to get the args and kwargs
            # and the outputs to update the attributes of the module

            args = []
            kwargs = {}

            if hasattr(process, "inputs"):
                for arg in process.inputs["args"]:
                    if arg is None:
                        continue
                    assert hasattr(
                        self.outputs, arg
                    ), "ProcessOutputs not properly adding args when calling track_process"
                    assert (
                        self.outputs[arg] is not None
                    ), f"ProcessOutputs not properly updating args: Output {arg} is None"
                    args.append(self.outputs[arg])
                for process_kwarg, attr_name in process.inputs["kwargs"]:
                    assert hasattr(
                        self.outputs, attr_name
                    ), "ProcessOutputs not properly adding kwargs when calling track_process"
                    assert (
                        self.outputs[attr_name] is not None
                    ), f"ProcessOutputs not properly updating kwargs: Output {attr_name} is None"
                    kwargs[process_kwarg] = self.outputs[attr_name]

            if hasattr(process, "other_module_inputs"):
                for (
                    process_kwarg,
                    other_module_kwargname,
                ) in process.other_module_inputs["kwargs"]:
                    if other_module_kwargname in other_module_inputs.keys():
                        kwargs[process_kwarg] = other_module_inputs[
                            other_module_kwargname
                        ]
                    else:
                        raise AttributeError(
                            f"Other module input not specified for process {process.name} ({process_kwarg}={other_module_kwargname}). other_module_inputs = {other_module_inputs}"
                        )

            if hasattr(process, "iterate"):
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
                self._run_iterations_in_parallel(process=process, *args, **kwargs)
            else:
                args_str = "_".join([_get_representation(i) for i in args])
                kwargs_str = "_".join(
                    [f"{k}={_get_representation(v)}" for k, v in kwargs.items()]
                )
                file_name = f"{process.__name__}-{self.file_information_name}-{args_str}-{kwargs_str}"

                success = self.multiprocesshelper.create_process_file(
                    process_name=process.__name__,
                    file_name=file_name,
                )
                if success == False:
                    logger.debug(f"File {file_name} already exists, skipping...")
                    continue
                try:
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
                except Exception as e:
                    logger.exception(e)
                    self.multiprocesshelper.update_process_file(
                        process_name=process.__name__,
                        file_name=file_name,
                        status="failed",
                    )
                    raise e
                else:
                    self.multiprocesshelper.update_process_file(
                        process_name=process.__name__,
                        file_name=file_name,
                        status="finished",
                    )

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
        max_workers = kwargs["max_workers"]
        kwargs_str = "_".join(
            [f"{k}={_get_representation(v)}" for k, v in kwargs.items()]
        )
        label = f"{process.__name__}-{kwargs_str}"
        rng = np.random.default_rng(seed=seed)
        try:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures_dict = {}
                for i in np.arange(num_iters):
                    file_name = f"{label}_iter{i}"
                    success = self.multiprocesshelper.create_process_file(
                        process_name=process.__name__,
                        file_name=file_name,
                    )
                    if success == False:
                        logger.debug(f"File {file_name} already exists, skipping...")
                        continue
                    kwargs["seed"] = rng.integers(low=0, high=1000000)
                    future = executor.submit(process, *args, **kwargs)
                    futures_dict[future] = file_name
                for i, future in tqdm(
                    enumerate(as_completed(futures_dict)),
                    desc="Progress on futures",
                    total=num_iters,
                ):
                    try:
                        file_name = futures_dict[future]
                        result = future.result()
                        logger.info(f"{label}: {result}")
                        self.multiprocesshelper.update_process_file(
                            process_name=process.__name__,
                            file_name=file_name,
                            status="finished",
                        )
                    except Exception as e:
                        logger.exception(e)
                        self.multiprocesshelper.update_process_file(
                            process_name=process.__name__,
                            file_name=file_name,
                            status="failed",
                        )
                executor.shutdown(wait=True)
        except Exception as e:
            logger.error(e.with_traceback())
            self.multiprocesshelper.cleanup()
            if not isinstance(e, KeyError):
                raise e
        finally:
            logger.info(f"Finished running multiprocessing for {process.__name__}")

    def __repr__(self):
        repr_parts = [f"\nModule: {self.name}"]
        repr_parts.append("Process info:")
        for i, (process_name, process) in enumerate(self._processes.items()):
            repr_parts.append(f"Process {i}: {process_name}")
            repr_parts.append(process_summary(process))
        return "\n".join(repr_parts)


# write the dunder method to show the repr of the object as a list of processes with input / output summary information
