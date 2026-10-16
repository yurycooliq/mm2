#!/usr/bin/env python3
"""Real offline Docker timeout/output-cap regression; no credentials or source mounts."""
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.research import patches
from tools.research.schema import ResearchError


def verify():
    for command, expected in [
        (["python", "-c", "import time; time.sleep(30)"], "time quota exceeded"),
        (["python", "-c", "import sys,time; sys.stdout.buffer.write(b'x'*600000); sys.stdout.flush(); time.sleep(30)"],
         "output quota exceeded"),
    ]:
        owner = uuid.uuid4()
        name = "mm2-patch-validation-" + owner.hex
        with tempfile.TemporaryDirectory(prefix="mm2-worker-cleanup-") as root:
            candidate, trusted = Path(root) / "candidate", Path(root) / "trusted"
            candidate.mkdir()
            (trusted / "output").mkdir(parents=True)
            try:
                with mock.patch.object(patches.uuid, "uuid4", return_value=owner):
                    try:
                        patches.run_validation_worker(patches.VALIDATION_IMAGE, candidate, trusted, command, timeout=2)
                    except ResearchError as error:
                        if expected not in str(error):
                            raise
                    else:
                        raise RuntimeError("expected bounded worker failure did not occur")
                result = subprocess.run(["docker", "container", "ls", "--all", "--filter",
                                         "label=mm2.patch-validation=" + owner.hex, "--format", "{{.ID}}"],
                                        capture_output=True, timeout=10, check=True)
                if result.stdout.strip():
                    raise RuntimeError("owned validation worker survived its failed client")
            finally:
                patches.cleanup_validation_worker(name, owner.hex)
    print("PASS: timeout and excessive-output workers removed; no surviving owned containers")


if __name__ == "__main__":
    verify()
