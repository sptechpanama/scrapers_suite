import subprocess
import sys
from pathlib import Path

DB_PYTHON = r"C:/Users/rodri/cl/venv/Scripts/python.exe"
DB_SCRIPT = r"C:/Users/rodri/scrapers_repo/db/db.py"
MINSA_PYTHON = r"C:/Users/rodri/minsa_scraper/.venv/Scripts/python.exe"
MINSA_SCRIPT = r"C:/Users/rodri/scrapers_repo/minsa_scraper/scrape_minsa.py"


def run_step(python_exe: str, script_path: str, label: str) -> None:
    script = Path(script_path)
    if not script.exists():
        raise FileNotFoundError(f"No se encontró {label}: {script}")
    cmd = [python_exe, str(script)]
    print(f"[RUN] {label}: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)
    print(f"[OK] {label} terminó con código {result.returncode}")


def main() -> None:
    run_step(DB_PYTHON, DB_SCRIPT, "DB.py")
    run_step(MINSA_PYTHON, MINSA_SCRIPT, "scrape_minsa.py")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] Proceso falló: {exc}")
        sys.exit(exc.returncode or 1)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[ERROR] {exc}")
        sys.exit(1)
