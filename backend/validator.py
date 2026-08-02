import re
from pathlib import Path


class MatlabFunctionValidator:
    """
    Validates uploaded MATLAB functions for some sort of safety (and correctness)
    """
    
    # Patterns for potentially dangerous operations
    PATTERNS = [
        r'\bfopen\b',      # File operations
        r'\bfwrite\b',
        r'\bfread\b',
        r'\bfclose\b',
        r'\bsystem\b',     # System calls
        r'\beval\b',       # Code execution
        r'\bexec\b',
        r'\brmdir\b',      # File system modifications
        r'\bdelete\b',
        r'\bquit\b',       # Process control
        r'\bexit\b',
    ]
    
    @staticmethod
    def validate(filepath: Path) -> tuple[bool, str]:
        """
        Validate a MATLAB function file for syntax and safety.
        Returns (is_valid, message)
        """
        try:
            if not filepath.exists():
                return False, "File does not exist"
            
            if not filepath.suffix.lower() == '.m':
                return False, "File must have .m extension"
            
            if filepath.stat().st_size > 1_000_000:  # 1 MB limit
                return False, "File size exceeds 1 MB limit"
            
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for dangerous patterns
            for pattern in MatlabFunctionValidator.PATTERNS:
                if re.search(pattern, content, re.IGNORECASE):
                    return False, f"Forbidden operation detected: {pattern}"
            
            # Check for valid function declaration
            if not re.search(r'^\s*function\s+', content, re.MULTILINE):
                return False, "File must contain a function declaration"
            
            # Extract function name and verify it matches filename
            func_match = re.search(
                r'^\s*function\s+(?:\w+\s*=\s*)?\s*(\w+)\s*\(',
                content,
                re.MULTILINE
            )
            if not func_match:
                return False, "Could not parse function signature"
            
            func_name = func_match.group(1)
            expected_name = filepath.stem
            
            if func_name != expected_name:
                return False, f"Function name '{func_name}' does not match filename '{expected_name}'"
            
            return True, "Valid MATLAB function"
            
        except UnicodeDecodeError:
            return False, "File is not valid UTF-8 text"
        except Exception as e:
            return False, f"Validation error: {str(e)}"