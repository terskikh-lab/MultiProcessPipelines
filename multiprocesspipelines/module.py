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

    def file_information_iterator(self, function):
        if isinstance(function, Callable):
            partial_func = partial(function, *args, **kwargs)
            partial_func.__name__ = function.__name__
            if hasattr(function, "output_tag"):
                if "file_information_iterator" not in function.output_tag:
                    raise ValueError(
                        "file_information_iterator must have 'file_information_iterator' in output_tag"
                    )
                partial_func.output_tag = function.output_tag
            else:
                raise AttributeError(
                    f"output_tag not specified for file_information_iterator (NOTE: file_information_iterator must have 'file_information_iterator' in output_tag)"
                )
            self._file_information_iterator = partial_func
        else:
            raise ValueError(
                f"function must be a Callable but {type(function)} was given"
            )

    def add_process(self, function: Callable, *args, **kwargs):
        if isinstance(function, Callable):
            partial_func = partial(function, *args, **kwargs)
            partial_func.__name__ = function.__name__
            if hasattr(function, "input_tag") and hasattr(function, "output_tag"):
                partial_func.input_tag = function.input_tag
                partial_func.output_tag = function.output_tag
            else:
                raise AttributeError(
                    f"input_tag or output_tag not specified for given process"
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
        logger.info(f"Running {self.name}...")
        # initialize run information
        try:
            self.create_directory("file_information_tempfiles")
            file_information_hex_mapping = {}
            for file_information in tqdm(
                self._file_information_iterator, f"{self.name} progress"
            ):
                # Serialize the object to bytes
                input_bytes = pickle.dumps(file_information)
                # Generate a SHA256 hash of the bytes
                hash_object = hashlib.sha256(input_bytes)
                # Get hexdigest of hash for name
                fileinfo_hash = hash_object.hexdigest(16)
                temp_file_name = f"{fileinfo_hash}.tmp"
                final_file_name = f"{fileinfo_hash}.fin"
                file_not_in_use = self.create_temp_file(
                    final_file_name=final_file_name,
                    temp_file_name=temp_file_name,
                    path="file_information_tempfiles",
                )
                if file_not_in_use == False:
                    logger.debug(f"File {temp_file_name} already exists, skipping...")
                    continue
                file_information_hex_mapping[temp_file_name] = file_information
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
                        self.get_directory("file_information_tempfiles"), temp_file_name
                    )
                )
            logger.info(f"Finished running {self.name}, cleaning up...")
            self.cleanup()
        except Exception as e:
            logger.exception(e)
            logger.error("An exception occurred, cleaning up...")
            self.cleanup()

    def _run_all_processes(self):
        for i, (name, process) in enumerate(self._processes.items()):
            logger.info(f"Running process {i}: {name}")
            if (i == 0) and ("file_information" not in process.input_tag):
                raise ValueError(
                    f"First process must have 'file_information' in input_tag"
                )
            self.create_directory(name)
            self.output_directory = self.get_directory(name)
            kwargs = {}
            for attr in process.input_tag:
                if attr is None:
                    continue
                if hasattr(self, attr):
                    kwargs[attr] = self.__getattribute__(attr)
                else:
                    raise AttributeError(
                        f"Invalid input tag: {self.input_tag}. Attr {attr} does not exist"
                    )
            if process.hasattr("iterate"):
                assert (
                    process.iterate == True
                ), "iterate attribute is a dummy and must be set to True"
                if any(i in kwargs.keys() for i in process.iterate_kwargs.keys()):
                    raise ValueError(
                        f"'iterate' processes cannot have {process.iterate_kwargs.keys()} as input arguments"
                    )
                kwargs.update(process.iterate_kwargs)
                self.create_directory("iteration_tempfiles")
                self._run_iterations_in_parallel(process=process, **kwargs)
            else:
                output = process(**kwargs)
                if (len(process.output_tag) != 1) and (
                    len(output) != len(process.output_tag)
                ):
                    raise ValueError(
                        f"Output length does not match tag: output_tag: {process.output_tag} ({len(process.output_tag)}) but the process returned {len(output)} args"
                    )
                for i, attr in enumerate(process.output_tag):
                    outi = output[i] if len(process.output_tag) > 1 else output
                    if attr is None:
                        continue
                    if hasattr(self, attr):
                        logger.warning(f"Updating {attr}...")
                        self.__setattr__(attr, outi)
                    else:
                        self.__setattr__(attr, outi)
                    # else:
                    #     raise AttributeError(f"Invalid output tag: {process.output_tag}. Attr {attr} does not exist")

    def _run_iterations_in_parallel(
        self,
        process: Callable,
        num_iters: int,
        seed: int,
        max_workers: int = 10,
        **kwargs,
    ):
        kwargs_str = "_".join([f"{k}={v}" for k, v in kwargs.items()])
        rng = np.random.default_rng(seed=seed)
        try:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures_dict = {}
                for i in np.arange(num_iters):
                    temp_file_name = (
                        f"{process.__name__}_{kwargs_str}_niters{num_iters}_iter{i}.tmp"
                    )
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
                    assert "seed" in kwargs.keys()
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
