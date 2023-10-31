from multiprocesstools import (
    MultiProcessHelper,
    RunTimeCounter,
    wait_until_file_exists,
    run_func_IO_loop,
)
from typing import Union, Callable
from functools import partial
import logging

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
            working_directory=output_directory,
            loggers=loggers,
        )
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

    def run_all_processes(self):
        for name, process in self._processes.items():
            logger.info(f"Running process {name}")
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
            if process.hasattr("parallel") and process.parallel:
                self.run_multiprocessing(process=process, **kwargs)
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

    def run_multiprocessing(self, process, **kwargs):
        
    for ch in channels:
        subset = (
            tas_fstring_regex % ch if ch != "allchannels" else tas_fstring_regex % ""
        )  # Create a new name to save the segmenetation results for this set of images
        try:
            # final_file_name = f"boot_chromage_ch{ch}_h{high_threshold}_l{low_threshold}_ncells{num_cells}_niters{num_iters}_iter0.csv"
            # # final_file_name = f"auc_threshold_accuracy_ch{ch}_h{high_threshold}_l{low_threshold}_ncells{num_cells}_nboots{num_bootstraps}_niters{num_iters}.csv"
            # temp_file_name = final_file_name.replace("csv", "tmp")
            # file_not_in_use = multiprocesshelper.create_temp_file(
            #     final_file_name=final_file_name,
            #     temp_file_name=temp_file_name,
            #     path=script_name,
            # )
            # if file_not_in_use == False:
            #     logger.debug(f"File {temp_file_name} already exists, skipping...")
            #     continue
            # (
            #     boot_chromage,
            #     boot_chromage_axis,
            #     boot_train_accuracy,
            #     boot_train_confusion,
            #     boot_test_accuracy,
            #     boot_test_confusion,
            #     boot_accuracy_curve,
            #     group_sizes,
            #     # ) = s1_o1_chromage_svm_bootstrap_traintestsplit(
            # ) = script(
            #     scdata=zscore,
            #     sample_col=sample_col,
            #     group_col=group_col,
            #     group_A=group_A,
            #     group_B=group_B,
            #     num_cells=num_cells,
            #     num_bootstraps=num_bootstraps,
            #     seed=rng.integers(low=0, high=100000),
            #     subset=subset,
            # )
            # multiprocesshelper.delete_tempfile(
            #     os.path.join(
            #         multiprocesshelper.get_directory(script_name),
            #         temp_file_name,
            #     )
            # )

            with ProcessPoolExecutor(max_workers=10) as executor:
                tempfiles_created = []
                futures = []
                for i in np.arange(num_iters):
                    final_file_name = f"boot_chromage_ch{ch}_h{high_threshold}_l{low_threshold}_ncells{num_cells}_nboots{num_bootstraps}__niters{num_iters}_iter{i}.csv"
                    temp_file_name = final_file_name.replace("csv", "tmp")
                    file_not_in_use = multiprocesshelper.create_temp_file(
                        final_file_name=final_file_name,
                        temp_file_name=temp_file_name,
                        path=script_name,
                    )
                    if file_not_in_use == False:
                        logger.debug(
                            f"File {temp_file_name} already exists, skipping..."
                        )
                        continue
                    tempfiles_created.append(temp_file_name)
                    future = executor.submit(
                        script,
                        scdata=zscore,
                        sample_col=sample_col,
                        group_col=group_col,
                        group_A=group_A,
                        group_B=group_B,
                        num_cells=num_cells,
                        num_bootstraps=num_bootstraps,
                        seed=rng.integers(low=0, high=100000),
                        subset=subset,
                    )
                    futures.append(future)
                for i, future in tqdm(
                    enumerate(as_completed(futures)), "Progress on futures"
                ):
                    result = list(future.result())
                    for name, iterResult in zip(
                        [
                            "boot_chromage",
                            "boot_chromage_axis",
                            "boot_train_accuracy",
                            "boot_train_confusion",
                            "boot_test_accuracy",
                            "boot_test_confusion",
                            "group_sizes",
                        ],
                        result,
                    ):
                        if not isinstance(iterResult, (pd.DataFrame, pd.Series)):
                            iterResult = pd.Series(iterResult, name=name)
                        generic_read_write.save_dataframe_to_csv(
                            df=iterResult,
                            path=outputDir,
                            filename=tempfiles_created[i]
                            .replace("boot_chromage", name)
                            .replace("tmp", "csv"),
                        )
                        if name == "boot_chromage":
                            multiprocesshelper.delete_tempfile(
                                os.path.join(
                                    multiprocesshelper.get_directory(script_name),
                                    tempfiles_created[i],
                                )
                            )
                executor.shutdown(wait=True)

        except Exception as e:
            logger.error(e)
            multiprocesshelper.cleanup()
            if not isinstance(e, KeyError):
                raise e
    multiprocesshelper.cleanup()
