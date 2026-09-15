from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import importlib.util
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "orquestador"))
import process_lifecycle as lifecycle


def test_database_jobs_cannot_be_preempted_by_quote_requests():
    for name in ["database_daily", "database_weekly_full", "sunday_db_minsa", "cotizacion_panama"]:
        assert not lifecycle.can_preempt_job(name)
    assert lifecycle.can_preempt_job("rir1")


def test_windows_termination_includes_descendants(monkeypatch):
    monkeypatch.setattr(lifecycle, "os", SimpleNamespace(name="nt"))
    command = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(lifecycle.subprocess, "run", command)
    process = Mock(pid=12345)
    process.poll.return_value = None
    lifecycle.terminate_process_tree(process)
    assert command.call_args.args[0] == ["taskkill", "/PID", "12345", "/T", "/F"]
    process.wait.assert_called_once()


def test_already_finished_process_is_not_targeted(monkeypatch):
    command = Mock()
    monkeypatch.setattr(lifecycle.subprocess, "run", command)
    process = Mock()
    process.poll.return_value = 0
    lifecycle.terminate_process_tree(process)
    command.assert_not_called()


def test_manual_pipeline_records_component_results_without_email(monkeypatch):
    import main
    job = main.JobConfig(name="database_daily", python="python", script="worker.py", days_of_week=[], times=[])
    completed = SimpleNamespace(returncode=0, stdout='ORQUESTADOR_COMPONENT_STATE={"job_name":"db_local","status":"success"}', stderr="")
    monkeypatch.setattr(main.subprocess, "run", Mock(return_value=completed))
    component = Mock()
    monkeypatch.setattr(main, "record_component_states", component)
    monkeypatch.setattr(main, "update_last_run", Mock())
    status, _ = main.run_job(job)
    assert status == "success"
    component.assert_called_once_with(completed.stdout)


def test_failed_tree_termination_is_not_silently_accepted(monkeypatch):
    import pytest
    monkeypatch.setattr(lifecycle, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(lifecycle.subprocess, "run", Mock(return_value=SimpleNamespace(returncode=1)))
    process = Mock(pid=12345)
    process.poll.return_value = None
    with pytest.raises(RuntimeError):
        lifecycle.terminate_process_tree(process)


def test_real_windows_tree_stops_grandchild(tmp_path):
    import os
    import time
    import ctypes
    import pytest
    if os.name != "nt":
        pytest.skip("Windows regression: orphaned updater after killing venv launcher")
    pidfile = tmp_path / "child.pid"
    child_code = "import time; time.sleep(90)"
    script = "import subprocess,sys,time;from pathlib import Path; p=subprocess.Popen([sys.executable,'-c'," + repr(child_code) + "]);Path(" + repr(str(pidfile)) + ").write_text(str(p.pid));time.sleep(90)"
    parent = subprocess.Popen([sys.executable, "-c", script])
    try:
        for _ in range(80):
            if pidfile.exists():
                break
            time.sleep(.1)
        assert pidfile.exists()
        child_pid = int(pidfile.read_text())
        lifecycle.terminate_process_tree(parent)
        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, child_pid)
        if handle:
            code = ctypes.c_ulong()
            ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
            ctypes.windll.kernel32.CloseHandle(handle)
            assert code.value != 259, "Grandchild was left alive"
    finally:
        if parent.poll() is None:
            lifecycle.terminate_process_tree(parent)
