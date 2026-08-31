from __future__ import annotations

import unittest
from pathlib import Path

from openpyxl import load_workbook

from common.no_requirements import (
    NO_REQUIREMENTS_MIXED,
    NO_REQUIREMENTS_ONLY,
    NO_REQUIREMENTS_SCOPE_COLUMN,
    classify_no_requirements_scope,
    ficha_codes_from_label,
    scope_column_values,
)


class NoRequirementsClassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.no_req = {"100", "200", "51119"}
        self.ct = {"300"}
        self.rs = {"400"}

    def classify(self, fichas) -> str:
        return classify_no_requirements_scope(
            fichas,
            no_requirements=self.no_req,
            requires_ct=self.ct,
            requires_rs=self.rs,
        )

    def test_only_no_requirements(self) -> None:
        self.assertEqual(self.classify(["100", "*200"]), NO_REQUIREMENTS_ONLY)

    def test_mixed_with_ct_rs_or_unknown(self) -> None:
        self.assertEqual(self.classify(["100", "300"]), NO_REQUIREMENTS_MIXED)
        self.assertEqual(self.classify(["100", "400"]), NO_REQUIREMENTS_MIXED)
        self.assertEqual(self.classify(["100", "999"]), NO_REQUIREMENTS_MIXED)

    def test_without_eligible_ficha_is_not_labeled(self) -> None:
        self.assertEqual(self.classify(["300"]), "")

    def test_contradictory_catalog_is_conservative(self) -> None:
        self.assertEqual(
            classify_no_requirements_scope(
                ["51119"],
                no_requirements=self.no_req,
                requires_ct={"51119"},
                requires_rs=set(),
            ),
            NO_REQUIREMENTS_MIXED,
        )

    def test_historical_sheet_values(self) -> None:
        rows = [
            ["ficha_detectada", NO_REQUIREMENTS_SCOPE_COLUMN],
            ["100, *200", ""],
            ["100, 300", NO_REQUIREMENTS_ONLY],
        ]
        values, changes = scope_column_values(
            rows,
            no_requirements=self.no_req,
            requires_ct=self.ct,
            requires_rs=self.rs,
        )
        self.assertEqual(values, [[NO_REQUIREMENTS_ONLY], [NO_REQUIREMENTS_MIXED]])
        self.assertEqual(changes, 2)

    def test_ficha_label_parser_preserves_order(self) -> None:
        self.assertEqual(ficha_codes_from_label("*100, 200; 100"), ["100", "200"])


class NoRequirementsCatalogTests(unittest.TestCase):
    TARGETS = {"51119", "51138", "51333", "51334", "51460"}
    ROOT = Path(__file__).resolve().parents[1]

    @staticmethod
    def _rows(path: Path) -> list[list[str]]:
        workbook = load_workbook(path, read_only=True, data_only=True)
        worksheet = workbook.active
        return [
            [str(value).strip() if value is not None else "" for value in row]
            for row in worksheet.iter_rows(values_only=True)
        ]

    def test_corrected_fichas_are_only_in_no_requirements_catalog(self) -> None:
        no_req = self._rows(self.ROOT / "data/fichas/fichas_sin_requisitos.xlsx")
        ct = self._rows(self.ROOT / "data/clrir/fichas_con_CT_sin_RS.xlsx")
        rs = self._rows(self.ROOT / "data/clrir/fichas_con_RS.xlsx")
        con_req = self._rows(self.ROOT / "data/fichas/fichas_con_requisitos.xlsx")

        no_req_codes = [row[0] for row in no_req if row and row[0] in self.TARGETS]
        self.assertEqual(set(no_req_codes), self.TARGETS)
        self.assertEqual(len(no_req_codes), len(self.TARGETS))
        self.assertFalse(self.TARGETS.intersection(row[0] for row in ct if row))
        self.assertFalse(self.TARGETS.intersection(row[0] for row in rs if row))
        self.assertFalse(self.TARGETS.intersection(row[0] for row in con_req if row))

    def test_corrected_fichas_have_one_no_no_row_in_master_catalog(self) -> None:
        master = self._rows(self.ROOT / "data/fichas/todas_las_fichas.xlsx")
        selected = [row for row in master if row and row[0] in self.TARGETS]
        self.assertEqual(len(selected), len(self.TARGETS))
        self.assertEqual({row[0] for row in selected}, self.TARGETS)
        self.assertTrue(all(row[1:3] == ["NO", "NO"] for row in selected))


if __name__ == "__main__":
    unittest.main()
