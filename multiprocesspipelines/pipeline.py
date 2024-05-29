import logging
import numpy as np
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Union, Callable, Iterable, Dict
from functools import partial

from .module import Module, ModuleOutputs
from .tools import _get_representation, format_module_parallel_execution_repr

logger = logging.getLogger(__name__)


class Pipeline:

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

        self.name = name
        self._modules: Dict[str, Module] = {}
        self._modules_io_connections: Dict[str, Dict[str, str]] = {}
        self._modules_to_run_in_parallel: Dict[str, dict] = {}

    @property
    def methods_list(self):
        return [
            method_name
            for method_name in dir(self)
            if not method_name.startswith("_") and callable(getattr(self, method_name))
        ]

    def add_module(
        self,
        module: Module,
        run_parallel: bool = False,
        executor: Union[ProcessPoolExecutor, ThreadPoolExecutor] = None,
        max_workers: int = None,
        executor_kwargs: dict = {},
    ):
        if not isinstance(module, Module):
            raise ValueError(
                f"function must be a Callable but {type(function)} was given"
            )
        if module.name in self._modules.keys():
            raise ValueError(f"module {module.name} already exists in the pipeline")
        if run_parallel == True:
            if not isinstance(executor, (ProcessPoolExecutor, ThreadPoolExecutor)):
                raise ValueError(
                    f"executor must be a ProcessPoolExecutor or ThreadPoolExecutor but {type(executor)} was given"
                )
            if max_workers is None:
                raise ValueError(
                    f"max_workers must be specified when run_parallel is True"
                )
            if not isinstance(max_workers, int):
                raise ValueError(
                    f"max_workers must be an integer but {type(max_workers)} was given"
                )
            self._modules_to_run_in_parallel[module.name] = {
                "executor": executor,
                "max_workers": max_workers,
                **executor_kwargs,
            }

        if len(module.other_module_inputs.keys()) != 0:
            for process_name, other_module_inputs in module.other_module_inputs.items():
                for (
                    this_process_kwargname,
                    other_module_str_info,
                ) in other_module_inputs["kwargs"].items():
                    other_module_name, other_module_kwargname = (
                        other_module_str_info.split(".")
                    )
                    if other_module_name not in self._modules.keys():
                        message = f"other_module_input: module {other_module_name} does not exist as a module, but process {process_name} requires it"
                        logger.error(message)
                        raise ValueError(message)
                    other_module = self._modules[other_module_name]
                    if not (other_module_kwargname in other_module.process_outputs):
                        message = f"other_module_input: module {other_module_name} does not have process output {other_module_kwargname}, but process {process_name} requires it.\n other_module.process_outputs: {other_module.process_outputs}"
                        logger.error(message)
                        raise ValueError(message)
                    if other_module_name not in self._modules_io_connections.keys():
                        self._modules_io_connections[other_module_name] = {
                            other_module_kwargname: {
                                module.name: {process_name: this_process_kwargname}
                            }
                        }
                    elif (
                        other_module_kwargname
                        not in self._modules_io_connections.get(
                            other_module_name, {}
                        ).keys()
                    ):
                        self._modules_io_connections[other_module_name][
                            other_module_kwargname
                        ] = {module.name: {process_name: this_process_kwargname}}
                    elif (
                        process_name
                        not in self._modules_io_connections.get(other_module_name, {})
                        .get(other_module_kwargname, {})
                        .get(module.name, {})
                        .keys()
                    ):
                        self._modules_io_connections[other_module_name][module.name][
                            process_name
                        ] = this_process_kwargname
                    else:
                        raise ValueError(
                            f"module {module.name} and module {other_module_name} already have an io connection for \
                                process {process_name} kwarg {this_process_kwargname} \
                                    and {other_module_name} output {other_module_kwargname}"
                        )

        self._modules[module.name] = module

    def run(self):
        logger.info(f"Running {self.name}...")
        # initialize run information
        try:
            for moduleidx, (module_name, module) in tqdm(
                enumerate(self._modules.items()),
                total=len(self._modules),
                desc=f"{self.name} progress on module execution",
            ):
                logger.info(
                    f"Running module {moduleidx} of {len(self._modules)}: {module_name}..."
                )
                other_module_inputs_kwargs = {}
                if len(module.other_module_inputs.keys()) != 0:
                    for (
                        process_name,
                        other_module_inputs,
                    ) in module.other_module_inputs.items():
                        process_other_module_input_kwargs = {}
                        for (
                            this_process_kwargname,
                            other_module_str_info,
                        ) in other_module_inputs["kwargs"].items():
                            other_module_name, other_module_kwargname = (
                                other_module_str_info.split(".")
                            )
                            other_module = self._modules[other_module_name]
                            other_module_kwargval = other_module.outputs[
                                other_module_kwargname
                            ]
                            process_other_module_input_kwargs[
                                this_process_kwargname
                            ] = other_module_kwargval
                        other_module_inputs_kwargs[process_name] = (
                            process_other_module_input_kwargs
                        )
                try:
                    if module.name in self._modules_to_run_in_parallel.keys():
                        max_workers = self._modules_to_run_in_parallel[module.name][
                            "max_workers"
                        ]
                        module = self._run_module_in_parallel(
                            module,
                            max_workers=max_workers,
                            **other_module_inputs_kwargs,
                        )
                    else:
                        module = self._run_module(module, **other_module_inputs_kwargs)
                except Exception as e:
                    logger.exception(e)
                    continue
            logger.info(f"Finished running {self.name}, cleaning up...")
        except Exception as e:
            logger.exception(e)
            logger.error("An exception occurred, cleaning up...")

    def _run_module(self, module: Module, **other_module_inputs):
        logger.info(f"Running {module.name}...")
        try:
            module.run(**other_module_inputs)
            logger.info(f"Finished running {module.name}")
        except Exception as e:
            logger.exception(e)
            raise e
        else:
            return module

    def _run_module_in_parallel(
        self,
        module: Module,
        executor: Union[ProcessPoolExecutor, ThreadPoolExecutor],
        max_workers: int,
        executor_kwargs: dict = {},
        other_module_inputs: dict = {},
    ):
        logger.info(f"Running {module.name} in parallel...")
        try:
            with executor(max_workers=max_workers, **executor_kwargs) as exe:
                futures = []
                for i in range(max_workers):
                    futures.append(exe.submit(module.run, **other_module_inputs))
                finished_modules_outputs = []
                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc=f"Progress on {module.name} parallel execution",
                ):
                    try:
                        finished_module = future.result()
                        finished_modules_outputs.append(finished_module.outputs)
                    except Exception as e:
                        logger.exception(e)
                        raise e
                exe.shutdown(wait=True)
            logger.info(f"Finished running {module.name} in parallel")
            logger.info(f"Aggregating {module.name} outputs...")
            aggregated_outputs = ModuleOutputs().aggregate(finished_modules_outputs)
            module.outputs = aggregated_outputs
            logger.info(f"Finished aggregating {module.name} outputs")
            return module
        except Exception as e:
            logger.exception(e)
            raise e

    def __repr__(self):
        repr_parts = [f"\nPipeline: {self.name}"]

        repr_parts.append("Module io connections:")
        for module_name, io_connections in self._modules_io_connections.items():
            repr_parts.append(f"Module {module_name} io connections:")
            for output_name, input_connections in io_connections.items():
                repr_parts.append(f"\tOutput {output_name}:")
                for (
                    input_module_name,
                    input_process_connections,
                ) in input_connections.items():
                    repr_parts.append(f"\t\tInput module {input_module_name}:")
                    for (
                        input_process_name,
                        input_process_kwarg,
                    ) in input_process_connections.items():
                        repr_parts.append(
                            f"\t\tProcess {input_process_name} kwarg {input_process_kwarg}"
                        )

        repr_parts.append("Module info:")
        for module_name, module in self._modules.items():
            if module_name in self._modules_to_run_in_parallel.keys():
                parallel_repr = format_module_parallel_execution_repr(
                    module_name=module_name,
                    **self._modules_to_run_in_parallel[module_name],
                )
                repr_parts.append(parallel_repr)

        return "\n".join(repr_parts)


# write the dunder method to show the repr of the object as a list of processes with input / output summary information
