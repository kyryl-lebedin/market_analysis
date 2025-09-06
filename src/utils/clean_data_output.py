import os
import sys
from pathlib import Path
import shutil


arg = sys.argv[1] if len(sys.argv) > 1 else None


if arg:
    directory = Path("data") / arg
else:
    directory = Path("data")


if directory.exists():
    shutil.rmtree(directory)
    print(f"Directory {directory} has been deleted")
else:
    print("Already deleted")
