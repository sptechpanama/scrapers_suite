from __future__ import annotations

import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "common"))

from ficha_utils import detectar_fichas_detalladas, detectar_fichas_tokens  # noqa: E402


POSITIVE_43358_IDS = {
    290260, 291490, 294278, 301753, 306115, 309221, 324970, 327520,
    330621, 332596, 336540, 341737, 342283, 344092, 344693,
}


class FichaDetectorTests(unittest.TestCase):
    def test_codigo_contextual_con_abreviaturas(self) -> None:
        for text in (
            "CTNI: 43358",
            "C.T.N.I. 43358",
            "F.T. 43358",
            "F/TEC 43358",
            "Ficha #43358",
            "43358*",
        ):
            with self.subTest(text=text):
                matches = detectar_fichas_detalladas({"titulo": text})
                self.assertEqual(matches[0].code, "43358")
                self.assertEqual(matches[0].method, "codigo_contextual")

    def test_nombre_completo_y_compacto(self) -> None:
        complete = "KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTESIA"
        compact = "KIT CIRCUITO PACIENTE MAQUINA ANESTESIA"
        self.assertIn("* 43358", detectar_fichas_tokens(complete))
        self.assertIn("* 43358", detectar_fichas_tokens(compact))

    def test_caso_real_cl_048336_detecta_plural_pacientes(self) -> None:
        fields = {
            "titulo": (
                "KIT DE CIRCUITO DE PACIENTES PARA MAQUINA DE ANESTESIA, "
                "SE UTILIZA PARA ADMINISTRAR GASES ANESTESICOS"
            ),
            "descripcion": (
                "KIT DE CIRCUITO DE PACIENTES PARA MAQUINA DE ANESTESIA "
                "CON MANGUERAS Y ACCESORIOS"
            ),
            "item_1": (
                "KIT DE CIRCUITO DE PACIENTES PARA MAQUINA DE ANESTESIA "
                "PARA ADMINISTRAR GASES ANESTESICOS"
            ),
        }
        matches = detectar_fichas_detalladas(fields)
        matched = next(match for match in matches if match.code == "43358")
        self.assertIn(matched.method, {"nombre_exacto", "nombre_compacto"})
        self.assertIn(matched.field, fields)

    def test_plural_flexible_sigue_exigiendo_nombre_especifico(self) -> None:
        for text in (
            "Compra de kits para pacientes de anestesia",
            "Circuitos y pacientes para equipos medicos",
            "Gases anestesicos para pacientes adultos",
        ):
            with self.subTest(text=text):
                self.assertNotIn("* 43358", detectar_fichas_tokens(text))

    def test_frase_demasiado_amplia_no_clasifica(self) -> None:
        self.assertNotIn(
            "* 43358",
            detectar_fichas_tokens("CIRCUITO PARA ANESTESIA DE PACIENTE"),
        )

    def test_numero_accidental_no_se_confunde_con_ficha(self) -> None:
        for text in (
            "Registro profesional 10634, folio 115",
            "Placa vehicular 012268",
            "Serie MB-11230",
        ):
            with self.subTest(text=text):
                self.assertEqual(detectar_fichas_tokens(text), [])

    def test_detecta_desde_descripcion_de_item(self) -> None:
        matches = detectar_fichas_detalladas(
            {
                "titulo": "Compra de insumos hospitalarios",
                "item_1": "KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTESIA",
            }
        )
        self.assertEqual(matches[0].code, "43358")
        self.assertEqual(matches[0].field, "item_1")

    def test_caso_real_43358_si_hay_base_local(self) -> None:
        db_path = REPO_ROOT / "data" / "db" / "panamacompra.db"
        if not db_path.exists():
            self.skipTest("Base local no disponible")
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        placeholders = ",".join("?" for _ in POSITIVE_43358_IDS)
        rows = connection.execute(
            f"SELECT id,titulo,descripcion FROM actos_publicos WHERE id IN ({placeholders})",
            tuple(sorted(POSITIVE_43358_IDS)),
        ).fetchall()
        self.assertEqual(len(rows), len(POSITIVE_43358_IDS))
        for row in rows:
            with self.subTest(id=row["id"]):
                codes = {
                    match.code
                    for match in detectar_fichas_detalladas(
                        {"titulo": row["titulo"], "descripcion": row["descripcion"]}
                    )
                }
                self.assertIn("43358", codes)


class DatabaseReclassificationTests(unittest.TestCase):
    def test_reclassifica_y_preserva_codigo_historico(self) -> None:
        updater_path = REPO_ROOT / "db" / "db_api_updater.py"
        spec = importlib.util.spec_from_file_location("db_updater_under_test", updater_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        with tempfile.TemporaryDirectory() as temp_dir:
            module.DB_PATH = Path(temp_dir) / "test.db"
            module.init_db()
            with module.connect_db() as connection:
                connection.executemany(
                    """INSERT INTO actos_publicos
                       (enlace,titulo,descripcion,ficha_detectada)
                       VALUES (?,?,?,?)""",
                    [
                        (
                            "https://example.test/43358",
                            "KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTESIA",
                            "",
                            "No Detectada",
                        ),
                        ("https://example.test/legacy", "Texto sin ficha", "", "999999"),
                    ],
                )
            matcher = module.FichaMatcher()
            processed, _ = module.reclassify_existing(matcher, force=True, batch_size=10)
            self.assertEqual(processed, 2)
            connection = sqlite3.connect(module.DB_PATH)
            try:
                values = dict(
                    connection.execute(
                        "SELECT enlace,ficha_detectada FROM actos_publicos"
                    ).fetchall()
                )
            finally:
                connection.close()
            self.assertIn("43358", values["https://example.test/43358"])
            self.assertIn("999999", values["https://example.test/legacy"])


if __name__ == "__main__":
    unittest.main()
