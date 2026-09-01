from __future__ import annotations

from common.purchase_unit_details import (
    CONTACT_EMAIL_COLUMN,
    CONTACT_NAME_COLUMN,
    CONTACT_PHONE_COLUMN,
    CONTACT_ROLE_COLUMN,
    DEPENDENCY_COLUMN,
    PROVINCE_COLUMN,
    PURCHASE_UNIT_COLUMN,
    extract_purchase_unit_details,
    has_purchase_unit_details,
    parse_purchase_unit_components,
    parse_purchase_unit_details,
)


def test_parses_contact_and_entity_sections_with_portal_encoding() -> None:
    details = parse_purchase_unit_details(
        [
            ["Nombre", " Hindramis   Zorrilla "],
            ["Cargo", "Analista de Compras II"],
            ["Tel�fono", "5130862"],
            ["Correo electr�nico", "hzorrilla@css.gob.pa"],
        ],
        [
            ["Entidad", "Caja de Seguro Social"],
            ["Dependencia", "CSS - Sede"],
            ["Unidad de Compra", "CSS - Direccion Nacional De Compras"],
            ["Provincia", "Panama"],
        ],
    )
    assert details == {
        CONTACT_NAME_COLUMN: "Hindramis Zorrilla",
        CONTACT_ROLE_COLUMN: "Analista de Compras II",
        CONTACT_PHONE_COLUMN: "5130862",
        CONTACT_EMAIL_COLUMN: "hzorrilla@css.gob.pa",
        DEPENDENCY_COLUMN: "CSS - Sede",
        PURCHASE_UNIT_COLUMN: "CSS - Direccion Nacional De Compras",
        PROVINCE_COLUMN: "Panama",
        "entidad": "Caja de Seguro Social",
    }
    assert has_purchase_unit_details(details)


def test_ignores_missing_values() -> None:
    details = parse_purchase_unit_details(
        [["Nombre", "No Disponible"]],
        [["Entidad", "-"], ["Provincia", ""]],
    )
    assert details[CONTACT_NAME_COLUMN] == ""
    assert details["entidad"] == ""
    assert not has_purchase_unit_details({column: "" for column in details})


def test_first_meaning_value_wins_when_portal_repeats_a_label() -> None:
    details = parse_purchase_unit_details(
        [["Nombre", "Ana Perez"], ["Nombre", "Encabezado ajeno"]],
        [["Entidad", "CSS"], ["Entidad", "Otra tabla"]],
    )
    assert details[CONTACT_NAME_COLUMN] == "Ana Perez"
    assert details["entidad"] == "CSS"


class FakeDriver:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def execute_script(self, _script: str, tokens: list[str]):
        self.calls.append(tokens)
        if tokens == ["contacto", "unidad"]:
            return [["Nombre", "Ana"], ["Correo electronico", "ana@test.local"]]
        return [["Entidad", "CSS"], ["Provincia", "Panama"]]


def test_driver_extractor_requests_both_scoped_sections() -> None:
    driver = FakeDriver()
    details = extract_purchase_unit_details(driver)
    assert driver.calls == [["contacto", "unidad"], ["inform", "entidad"]]
    assert details[CONTACT_NAME_COLUMN] == "Ana"
    assert details[CONTACT_EMAIL_COLUMN] == "ana@test.local"
    assert details["entidad"] == "CSS"
    assert details[PROVINCE_COLUMN] == "Panama"


def test_parses_public_v3_components() -> None:
    details = parse_purchase_unit_components(
        [
            {
                "titulo": "Contacto de la unidad de compra",
                "value": [
                    {"nombre": "Nombre", "value": "Hindramis Zorrilla"},
                    {"nombre": "Telefono", "value": "5130862"},
                ],
            },
            {
                "titulo": "Informacion de la entidad",
                "value": [
                    {"nombre": "Entidad", "value": "Caja de Seguro Social"},
                    {"nombre": "Unidad de Compra", "value": "CSS - Compras"},
                ],
            },
        ]
    )
    assert details[CONTACT_NAME_COLUMN] == "Hindramis Zorrilla"
    assert details[CONTACT_PHONE_COLUMN] == "5130862"
    assert details["entidad"] == "Caja de Seguro Social"
    assert details[PURCHASE_UNIT_COLUMN] == "CSS - Compras"
