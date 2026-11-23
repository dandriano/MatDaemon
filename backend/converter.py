from __future__ import annotations

from typing import Dict, List, Union, cast

import matlab.engine


InputScalar = Union[str, bool, int, float]
InputValue = Union[InputScalar, List["InputValue"], Dict[str, "InputValue"]]
MatlabValue = Union[str, bool, matlab.double, Dict[str, "MatlabValue"]]
ResultScalar = Union[str, bool, int, float]
ResultValue = Union[ResultScalar, List["ResultValue"], Dict[str, "ResultValue"]]


def _is_number(value: object) -> bool:
    # bool is an int subclass in Python, but MATLAB logicals are passed through.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_input_value(value: object, path: str = "argument") -> None:
    """Validate a JSON value against the supported positional argument contract."""
    if value is None:
        raise TypeError(f"{path} cannot be null")
    if isinstance(value, (str, bool)) or _is_number(value):
        return
    if isinstance(value, dict):
        for key, field_value in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} struct field names must be strings")
            validate_input_value(field_value, f"{path}.{key}")
        return
    if isinstance(value, list):
        if not value:
            return

        if all(_is_number(item) for item in value):
            return
        if all(isinstance(item, list) for item in value):
            if not value[0]:
                raise ValueError(f"{path} matrix rows must not be empty")
            width = len(value[0])
            for row in value:
                if len(row) != width:
                    raise ValueError(f"{path} must be a rectangular numeric matrix")
                if not all(_is_number(item) for item in row):
                    raise TypeError(f"{path} must contain only numbers")
            return
        raise TypeError(
            f"{path} must be a numeric vector or matrix; mixed and nested values are unsupported"
        )
    raise TypeError(f"{path} has unsupported value type {type(value).__name__}")


def validate_input_type(value: object, matlab_type: str | None, path: str) -> InputValue:
    """Validate a typed JSON value against a MATLAB input declaration."""
    validate_input_value(value, path)
    if matlab_type is None:
        return cast(InputValue, value)

    numeric_types = {
        "double", "single", "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
    }
    if matlab_type in numeric_types:
        if not (_is_number(value) or isinstance(value, list)):
            raise TypeError(f"{path} must be numeric for MATLAB {matlab_type}")
    elif matlab_type == "logical" and not isinstance(value, bool):
        raise TypeError(f"{path} must be a JSON boolean for MATLAB logical")
    elif matlab_type == "struct" and not isinstance(value, dict):
        raise TypeError(f"{path} must be a JSON object for MATLAB struct")
    elif matlab_type in ("char", "string") and not isinstance(value, str):
        raise TypeError(f"{path} must be a JSON string for MATLAB {matlab_type}")
    return cast(InputValue, value)


def _convert_value(value: InputValue) -> MatlabValue:
    if isinstance(value, dict):
        return {key: _convert_value(field_value) for key, field_value in value.items()}
    if isinstance(value, list):
        validate_input_value(value)
        return matlab.double(value)
    if _is_number(value):
        return matlab.double([value])
    if isinstance(value, (str, bool)):
        return value
    # Keep runtime validation in place for callers that bypass the HTTP layer.
    validate_input_value(value)
    raise TypeError(f"Unsupported argument value: {type(value).__name__}")


def convert_to_matlab_types(value: InputValue) -> MatlabValue:
    """Convert one positional argument, including recursively nested struct fields."""
    validate_input_value(value)
    return _convert_value(value)


def convert_from_matlab_types(value: object) -> ResultValue:
    """Convert supported MATLAB result values into JSON-compatible Python values."""
    if isinstance(value, matlab.double):
        return value.tolist()
    if isinstance(value, dict):
        converted: dict[str, ResultValue] = {}
        for key, field_value in value.items():
            if not isinstance(key, str):
                raise TypeError("MATLAB struct field names must be strings")
            converted[key] = convert_from_matlab_types(field_value)
        return converted
    if isinstance(value, (list, tuple)):
        return [convert_from_matlab_types(item) for item in value]
    if isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"Unsupported MATLAB result type: {type(value).__name__}")
