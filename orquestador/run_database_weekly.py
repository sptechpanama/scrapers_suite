"""Entrada semanal de reclasificación completa del pipeline de base de datos."""

from database_pipeline import main


if __name__ == "__main__":
    raise SystemExit(main(["--mode", "full"]))
