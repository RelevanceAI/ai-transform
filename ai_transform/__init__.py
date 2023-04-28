__version__ = "0.30.10"

from ai_transform.timer import Timer
from ai_transform.logger import ic

_TIMER = Timer()
_TIMER.start()


def add_config_paths(verbose: bool = False):
    # Support all config paths
    # Add config paths
    import sys
    import os

    # Append the current files.
    sys.path.append(".")
    script_path = os.environ.get("script_path", "")
    efs_mount_path = os.environ.get("EFS_MOUNT_PATH")
    workflows_version = os.environ.get("WORKFLOWS_VERSION")
    path = script_path.replace("/main.py", "")
    main_path = f"{efs_mount_path}/scripts/{workflows_version}/{path}"
    if verbose:
        ic(main_path)
    if os.path.exists(main_path):
        sys.path.append(main_path)
    if verbose:
        ic(f"paths: {sys.path}")


add_config_paths()
