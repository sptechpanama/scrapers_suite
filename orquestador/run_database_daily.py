"""Entrada diaria incremental con publicación de precios RIR al finalizar."""
from run_database_with_prices import main


if __name__ == "__main__":
    raise SystemExit(main(["--mode", "incremental"]))
