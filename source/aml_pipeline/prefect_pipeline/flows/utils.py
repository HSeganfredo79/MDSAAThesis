# flows/utils.py
from pathlib import Path
import os, sys, runpy, contextlib
from typing import Optional, List  # 3.9-compatible

def project_root() -> Path:
    """Where enrich.py, score.py, train.py, etc live."""
    home = os.environ.get("AML_PIPELINE_HOME", "/root/tese_henrique/aml_pipeline")
    return Path(home).resolve()

@contextlib.contextmanager
def pushd(path: Path):
    prev = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)

def run_script(script_relpath: str, argv: Optional[List[str]] = None) -> None:
    """
    Execute a top-level script (like score.py) in-process, as if called with `python script.py <args>`.
    - Sets CWD to project root so relative files (e.g., last_block.txt) resolve.
    - Injects sys.argv for scripts that parse CLI args.
    """
    root = project_root()
    script_path = root / script_relpath
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    args = argv or []
    old_argv = sys.argv[:]
    try:
        sys.argv = [str(script_path)] + args
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        with pushd(root):
            runpy.run_path(str(script_path), run_name="__main__")
    finally:
        sys.argv = old_argv
