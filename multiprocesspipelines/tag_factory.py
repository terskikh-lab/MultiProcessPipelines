from typing import Callable, TypeVar, ParamSpec
from functools import partial

T = TypeVar("T")
P = ParamSpec("P")


def input_tag_factory(
    name: str,
    attribute: str,
) -> Callable:
    def input_tag(func: Callable[P, T]) -> Callable[P, T]:
        if hasattr(func, "input_tag"):
            func.input_tag.append(attribute)
        else:
            func.input_tag = [attribute]
        return func

    input_tag.__name__ = name
    return input_tag


def output_tag_factory(
    name: str,
    attribute: str,
) -> Callable:
    def output_tag(func: Callable[P, T]) -> Callable[P, T]:
        if hasattr(func, "output_tag"):
            func.output_tag.append(attribute)
        else:
            func.output_tag = [attribute]
        return func

    output_tag.__name__ = name
    return output_tag
