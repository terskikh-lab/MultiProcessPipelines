import hashlib
import pickle


def _get_representation(v):
    if isinstance(v, (int, float, str, bool, type(None))):
        return str(v)
    elif isinstance(v, (list, tuple)) and len(v) <= 5:
        return f"{type(v).__name__}({', '.join([_get_representation(i) for i in v])})"
    else:
        v_hash = hashlib.sha256(pickle.dumps(v)).hexdigest()[:10]
        return f"{type(v).__name__}({v_hash})"


def format_module_parallel_execution_repr(module_name, executor, max_workers, executor_kwargs):
    lines = [
        f"Running in parallel: {module_name}",
        f"Executor: {executor}",
        f"Outputs: {max_workers}",
        "Executor kwargs:",
    ]
    lines.extend([f"    - {k}: {v}" for k, v in executor_kwargs.items()])

    max_length = max(len(line) for line in lines)
    border = "*" * (max_length + 4)  # Add 4 for padding

    lines = [f"* {line.ljust(max_length)} *" for line in lines]

    return "\n".join([border] + lines + [border])


if __name__ == "__main__":
    test_kwargs = {
        "module_name": 10,
        "executor": 42,
        "max_workers": 10,
        "executor_kwargs": {
            "seed": 42,
        },
    }

    print(format_message(**test_kwargs))
