import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_EXE = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
DB_SCRIPT = REPO_ROOT / "db" / "db.py"
MINSA_SCRIPT = REPO_ROOT / "minsa_scraper" / "scrape_minsa.py"


def run_step(script_path: Path, label: str) -> None:
    python_exe = PYTHON_EXE
    if not python_exe.exists():
        raise FileNotFoundError(f"No se encontró el intérprete en {python_exe}")
    if not script_path.exists():
        raise FileNotFoundError(f"No se encontró {label}: {script_path}")
    cmd = [str(python_exe), str(script_path)]
    print(f"[RUN] {label}: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)
    print(f"[OK] {label} terminó con código {result.returncode}")


def main() -> None:
    run_step(DB_SCRIPT, "DB.py")
    run_step(MINSA_SCRIPT, "scrape_minsa.py")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] Proceso falló: {exc}")
        sys.exit(exc.returncode or 1)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[ERROR] {exc}")
        sys.exit(1)
