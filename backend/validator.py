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
    def extract_signature(filepath: Path) -> tuple[str, tuple[str, ...]]:
        """Read a MATLAB function's declared name and ordered input names."""
        fname, arguments, _ = MatlabFunctionValidator.extract_metadata(filepath)
        return fname, arguments

    @staticmethod
    def extract_metadata(filepath: Path) -> tuple[str, tuple[str, ...], dict[str, str]]:
        """Read function inputs and any types declared in its arguments block."""
        content = filepath.read_text(encoding="utf-8")
        lines = content.splitlines()
        declaration_lines: list[str] = []
        for index, line in enumerate(lines):
            if re.match(r"^\s*function\b", line):
                declaration_lines.append(line)
                while re.sub(r"%.*$", "", declaration_lines[-1]).rstrip().endswith("..."):
                    if index + 1 >= len(lines):
                        break
                    index += 1
                    declaration_lines.append(lines[index])
                break

        if not declaration_lines:
            raise ValueError("File must contain a function declaration")

        declaration = " ".join(
            re.sub(r"%.*$", "", line).replace("...", "")
            for line in declaration_lines
        ).strip()
        match = re.fullmatch(
            r"function\s+(?:(?:\[[^\]]*\]|[A-Za-z]\w*)\s*=\s*)?"
            r"(?P<name>[A-Za-z]\w*)\s*(?:\((?P<args>.*)\))?\s*;?",
            declaration,
            re.DOTALL,
        )
        if not match:
            raise ValueError("Could not parse function signature")

        raw_args = match.group("args")
        if raw_args is None or not raw_args.strip():
            args: tuple[str, ...] = ()
        else:
            names = [name.strip() for name in raw_args.split(",")]
            if any(not re.fullmatch(r"[A-Za-z]\w*", name) for name in names):
                raise ValueError("Function inputs must have simple named arguments")
            if len(set(names)) != len(names):
                raise ValueError("Function input names must be unique")
            args = tuple(names)

        argument_types: dict[str, str] = {}
        arguments_start = next(
            (
                index
                for index, line in enumerate(lines)
                if re.match(r"^\s*arguments(?:\s*\([^)]*\))?\s*$", line)
            ),
            None,
        )
        if arguments_start is not None:
            for line in lines[arguments_start + 1:]:
                declaration = re.sub(r"%.*$", "", line).strip()
                if declaration == "end":
                    break
                type_match = re.match(
                    r"^(?P<name>[A-Za-z]\w*)\s+(?:\([^)]*\)\s*)?"
                    r"(?P<type>[A-Za-z]\w*(?:\.[A-Za-z]\w*)*)\b",
                    declaration,
                )
                if type_match and type_match.group("name") in args:
                    argument_types[type_match.group("name")] = type_match.group("type").lower()

        return match.group("name"), args, argument_types
    
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
            
            # Check that the declaration exposes named inputs for API binding.
            MatlabFunctionValidator.extract_signature(filepath)
            
            return True, "Valid MATLAB function"
            
        except UnicodeDecodeError:
            return False, "File is not valid UTF-8 text"
        except Exception as e:
            return False, f"Validation error: {str(e)}"
