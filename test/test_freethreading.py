# SPDX-License-Identifier: MPL-2.0

import subprocess
import sys
import sysconfig
import textwrap

import pytest

_IS_FREETHREADING: bool = sysconfig.get_config_var("Py_GIL_DISABLED") == 1


def _run_python(code: str) -> subprocess.CompletedProcess[str]:
    """Run a Python snippet in a fresh interpreter process.

    :param code: Python source to execute via `-c`.
    :returns: The completed process.
    """

    return subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        text=True,
        capture_output=True,
    )


@pytest.mark.skipif(not _IS_FREETHREADING, reason="requires free-threading build")
def test_import_does_not_enable_gil() -> None:
    """Importing `orjson` should not enable the GIL in free-threading builds."""

    code: str = textwrap.dedent(
        """
        import sys
        import sysconfig

        assert sysconfig.get_config_var("Py_GIL_DISABLED") == 1
        before = sys._is_gil_enabled()
        import orjson  # noqa: F401
        after = sys._is_gil_enabled()
        assert before is False, before
        assert after is False, after
        """
    )
    res = _run_python(code)
    assert res.returncode == 0, res.stderr


@pytest.mark.skipif(not _IS_FREETHREADING, reason="requires free-threading build")
def test_concurrent_mutation_does_not_crash() -> None:
    """Concurrent mutation while serializing should not crash the interpreter."""

    code: str = textwrap.dedent(
        """
        import threading
        import traceback
        import time

        import orjson

        shared_list: list[int] = list(range(256))
        shared_dict: dict[str, int] = {str(i): i for i in range(256)}

        errors: list[str] = []
        stop = threading.Event()
        start = threading.Barrier(3)

        def mutate() -> None:
            \"\"\"Mutate shared objects to stress concurrent reads.\"\"\"
            try:
                start.wait(timeout=5.0)
                i: int = 0
                while stop.is_set() is False:
                    shared_list.append(i)
                    if len(shared_list) > 0 and i % 3 == 0:
                        shared_list.pop()
                    shared_dict[str(i % 512)] = i
                    if i % 5 == 0:
                        shared_dict.pop(str((i + 255) % 512), None)
                    i += 1
            except Exception:
                errors.append(traceback.format_exc())

        def dump() -> None:
            \"\"\"Continuously serialize while other thread mutates.\"\"\"
            try:
                start.wait(timeout=5.0)
                end: float = time.monotonic() + 0.75
                while time.monotonic() < end:
                    orjson.dumps(shared_list)
                    orjson.dumps(shared_dict)
                    orjson.dumps({"l": shared_list, "d": shared_dict})
                stop.set()
            except Exception:
                errors.append(traceback.format_exc())
                stop.set()

        t1 = threading.Thread(target=mutate, daemon=True)
        t2 = threading.Thread(target=dump, daemon=True)
        t1.start()
        t2.start()
        start.wait(timeout=5.0)
        t2.join(timeout=10.0)
        if t2.is_alive() is True:
            stop.set()
            raise AssertionError("dump thread did not terminate")
        stop.set()
        t1.join(timeout=0.25)

        if len(errors) > 0:
            raise AssertionError("\\n".join(errors))
        """
    )
    res = _run_python(code)
    assert res.returncode == 0, res.stderr
