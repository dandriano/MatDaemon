from copy import deepcopy

import matlab.engine


def convert_to_matlab_types(data: dict[str, any]) -> dict[str, any]:
    """
    Recursively convert native (numerics) types to MATLAB-compatible types.
    
    Args:
        data: Dictionary potentially containing numeric lists/values to convert
        
    Returns:
        Dictionary with MATLAB-compatible types
    """
    result = deepcopy(data)
    
    for key, value in result.items():
        if isinstance(value, dict):
            result[key] = convert_to_matlab_types(value)
        elif isinstance(value, list):
            if not value:
                continue

            if all(isinstance(item, list) and 
                    all(isinstance(sub, (int, float)) for sub in item) 
                    for item in value):
                result[key] = matlab.double(value)
            elif all(isinstance(item, (int, float)) for item in value):
                result[key] = matlab.double(value)
        elif isinstance(value, (int, float)):
            result[key] = matlab.double([value])
    
    return result


def convert_from_matlab_types(data: dict[str, any]) -> dict[str, any]:
    """
    Recursively convert MATLAB (numerics) types back to native types.
    
    Args:
        data: Dictionary potentially containing MATLAB types
        
    Returns:
        Dictionary with native types
    """
    result = deepcopy(data)
    
    for key, value in result.items():
        if isinstance(value, dict):
            result[key] = convert_from_matlab_types(value)
        elif isinstance(value, matlab.double):
            result[key] = value.tolist()
            # It's not clear, when I want a true scalar
            # if len(result[key]) == 1:
            #     result[key] = result[key][0]
    
    return result
