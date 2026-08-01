import os
from dotenv import load_dotenv

load_dotenv()

def get_env_list(key: str, default: str = "") -> list[str]:
    raw_val = os.getenv(key, default)
    return [p.strip() for p in raw_val.split(",") if p.strip()]

CONFIG = {
    "PORT": int(os.getenv("MATLAB_LISTENING_PORT", "8080")),
    "CONCURRENCY_LIMIT": int(os.getenv("MATLAB_DAEMON_CONCURRENCY_LIMIT", "10")),
    "DRAIN_TIMEOUT": float(os.getenv("MATLAB_DRAIN_TIMEOUT", "0.4")),
    "SCRIPT_PATHS": get_env_list("MATLAB_SCRIPT_PATHS", "matlab"),
    "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
    "LOG_FILE_PATH": os.getenv("LOG_FILE_PATH", "./matdaemon.log")
}
