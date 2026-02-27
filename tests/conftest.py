import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[0]
SRC = ROOT.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
