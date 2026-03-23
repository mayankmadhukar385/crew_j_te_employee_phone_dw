import pathlib
import sys
import pytest

def main() -> int:
    tests_dir = pathlib.Path(__file__).resolve().parent
    return pytest.main([
        str(tests_dir),
        "-q",
        "--no-cov",
        "-o",
        "addopts=-v",
    ])

if __name__ == "__main__":
    sys.exit(main())