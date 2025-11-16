# -*- coding: utf-8 -*-
"""Scraper de portales pÃƒÂƒÃ‚Âºblicos del MINSA usando Selenium."""







from __future__ import annotations







import argparse



import json



import mimetypes



import re



import sys



import time



import unicodedata



from dataclasses import dataclass



from io import StringIO



from pathlib import Path



from typing import Any, Callable, Iterable, Sequence, Optional







import pandas as pd



from google.oauth2.service_account import Credentials



from googleapiclient.discovery import build



from googleapiclient.errors import HttpError



from googleapiclient.http import MediaFileUpload



from selenium import webdriver



from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException,
)



from selenium.webdriver import Chrome



from selenium.webdriver.chrome.options import Options



from selenium.webdriver.chrome.service import Service



from selenium.webdriver.common.by import By



from selenium.webdriver.remote.webelement import WebElement



from selenium.webdriver.support import expected_conditions as EC



from selenium.webdriver.support.ui import WebDriverWait



from webdriver_manager.chrome import ChromeDriverManager











BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent



CTNI_URL = "https://ctni.minsa.gob.pa/Home/ConsultarFichas"



CRITERIOS_URL = "https://dndmcriterios.minsa.gob.pa/ct/Consultas/frmCRITERIOS_Criterios.aspx"



OFERENTES_URL = "https://appwebs.minsa.gob.pa/woferentes/Oferentes.aspx"



CATALOGO_PUBLICO_URL = "https://appwebs.minsa.gob.pa/woferentes/Public/Catalogos.aspx"



DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive",)



DEFAULT_DRIVE_FOLDER_ID = "0AMdsC0UugWLkUk9PVA"



DEFAULT_OUTPUT_DIR = BASE_DIR / "outputs"



_DEFAULT_CREDENTIALS_PATH = REPO_ROOT / "credentials" / "service-account.json"



COLUMN_FIXES = {
    "Cat?logo": "Cat\u00E1logo",
    "Cat\u00F3logo": "Cat\u00E1logo",
    "Cat?laogo": "Cat\u00E1logo",
    "Pa?s": "Pa\u00EDs",
    "Pa\u00EDs": "Pa\u00EDs",
    "Tel?fono": "Tel\u00E9fono",
    "Tel\u00E9fono": "Tel\u00E9fono",
    "Comit?": "Comit\u00E9",
    "Comit\u00E9": "Comit\u00E9",
    "N?": "N\u00B0",
    "N\u00B0": "N\u00B0",
    "N? Ficha": "N\u00FAmero Ficha",
    "N\u00B0 Ficha": "N\u00FAmero Ficha",
    "N\u00BA Ficha": "N\u00FAmero Ficha",
    "N\u00FAmero Ficha": "N\u00FAmero Ficha",
    "T?cnica": "T\u00E9cnica",
    "T\u00E9cnica": "T\u00E9cnica",
    "T?cnico": "T\u00E9cnico",
    "T\u00E9cnico": "T\u00E9cnico",
    "T?c.": "T\u00E9c.",
    "T\u00E9c.": "T\u00E9c.",
    "Gen?rico": "Gen\u00E9rico",
    "Gen\u00E9rico": "Gen\u00E9rico",
    "Importaci?n": "Importaci\u00F3n",
    "Importaci\u00F3n": "Importaci\u00F3n",
    "Permiso Especial Importaci?n": "Permiso Especial Importaci\u00F3n",
    "Permiso Especial Importaci\u00F3n": "Permiso Especial Importaci\u00F3n",
}

DEDUP_CONFIG: dict[str, tuple[str, ...]] = {
    "fichas_ctni": ("N\u00FAmero Ficha",),
    "criterios_tecnicos": ("Certificado",),
}

DEFAULT_DRIVE_CREDENTIALS = _DEFAULT_CREDENTIALS_PATH if _DEFAULT_CREDENTIALS_PATH.exists() else None



CATALOGO_PUBLIC_TABLE_XPATH = "//table[contains(@id,'gvCatalogo')]"



CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")



WHITESPACE_RE = re.compile(r"\s+")



@dataclass



class DuplicateLogEntry:



    dataset: str



    row_number: int



    original_row_number: int



    row_data: dict[str, Any]



    original_row_data: dict[str, Any]







@dataclass



class CatalogRunCounters:



    pages: int = 0



    unique_rows: int = 0



    duplicate_rows: int = 0







DUPLICATE_LOGS: list[DuplicateLogEntry] = []



CATALOG_COUNTERS = CatalogRunCounters()



SKIPPED_OFERENTES: list[dict[str, str]] = []



SKIP_OFERENTE_NAMES: set[str] = {"MEDIKCORP"}



CATALOG_SCRAPE_MODE = "legacy"



SKIP_OFERENTE_NAMES = {
    'MEDIKCORP',
}




@dataclass



class ScrapeResult:



    name: str



    dataframe: pd.DataFrame







def normalize_column_label(label: str) -> str:
    text = str(label).strip()
    if not text:
        return text
    try:
        text = text.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    for wrong, right in COLUMN_FIXES.items():
        text = text.replace(wrong, right)
    if "::" in text:
        text = text.split("::", 1)[1].strip()
    return text


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:

    if df is None or df.empty:

        return df

    df = df.copy()

    columns = df.columns.astype(str)

    valid_mask = ~columns.str.startswith("Unnamed", na=False)

    df = df.loc[:, valid_mask]

    df.columns = [normalize_column_label(col) for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    return df


def _sanitize_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    text = str(value)
    text = CONTROL_CHAR_PATTERN.sub(" ", text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def sanitize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].map(_sanitize_text)
    return df


def build_full_row_signature(row: pd.Series, columns: Sequence[str]) -> tuple[str, ...]:
    return tuple(_sanitize_text(row.get(column, "")) for column in columns)


def log_duplicate(
    dataset: str,
    row_number: int,
    original_row_number: int,
    *,
    row_data: dict[str, Any],
    original_row_data: dict[str, Any],
) -> None:
    entry = DuplicateLogEntry(
        dataset=dataset,
        row_number=row_number,
        original_row_number=original_row_number,
        row_data=row_data,
        original_row_data=original_row_data,
    )
    DUPLICATE_LOGS.append(entry)
    print(f"[CAT][DUP] fila #{row_number} repetida (original #{original_row_number})")
    print(f"    DUP: {row_data}")
    print(f"    ORG: {original_row_data}")


def _column_series(df: pd.DataFrame, column: str) -> pd.Series:

    if column in df.columns:

        return df[column].astype(str).fillna("").str.strip()

    return pd.Series([""] * len(df), index=df.index)


def build_key_series(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:

    if not columns:

        raise ValueError("Se requieren columnas para construir la clave de deduplicaciÃ³n.")

    if df is None or df.empty:

        return pd.Series(dtype="object")

    key = _column_series(df, columns[0])

    for column in columns[1:]:

        key = key + "||" + _column_series(df, column)

    key = key.str.strip("|").str.strip()

    key = key.replace("", pd.NA)

    return key











@dataclass



class DriveUploader:



    """PequeÃƒÂƒÃ‚Â±o helper para subir archivos a Google Drive."""







    service: Any



    folder_id: str



    drive_id: str | None







    @classmethod



    def from_service_account(cls, credentials_path: Path, folder_id: str) -> "DriveUploader":



        credentials = Credentials.from_service_account_file(



            str(credentials_path),



            scopes=DRIVE_SCOPES,



        )



        service = build("drive", "v3", credentials=credentials, cache_discovery=False)



        drive_id = None



        try:



            folder_metadata = (



                service.files()



                .get(fileId=folder_id, fields="id,name,driveId", supportsAllDrives=True)



                .execute()



            )



            drive_id = folder_metadata.get("driveId")



        except HttpError as exc:



            status = getattr(exc, "resp", None)



            status_code = getattr(status, "status", None)



            if status_code == 404:



                try:



                    drive_metadata = (



                        service.drives()



                        .get(driveId=folder_id, fields="id,name")



                        .execute()



                    )



                    drive_id = drive_metadata["id"]



                except HttpError as drive_exc:



                    raise RuntimeError(



                        f"No se pudo acceder a la carpeta o drive ({folder_id}): {drive_exc}"



                    ) from drive_exc



            else:



                raise RuntimeError(f"No se pudo acceder a la carpeta de Drive ({folder_id}): {exc}") from exc



        return cls(service=service, folder_id=folder_id, drive_id=drive_id)







    def _list_kwargs(self) -> dict[str, Any]:



        kwargs = {



            "supportsAllDrives": True,



            "includeItemsFromAllDrives": True,



            "spaces": "drive",



        }



        if self.drive_id:



            kwargs["driveId"] = self.drive_id



            kwargs["corpora"] = "drive"



        else:



            kwargs["corpora"] = "user"



        return kwargs







    def _write_kwargs(self) -> dict[str, Any]:



        return {"supportsAllDrives": True} if self.drive_id else {}







    def upload_many(self, files: Sequence[Path]) -> None:



        for file_path in files:



            self.upload(file_path)







    def upload(self, file_path: Path) -> None:



        if not file_path.exists():



            raise FileNotFoundError(f"No se encontrÃƒÂƒÃ‚Â³ el archivo a subir: {file_path}")







        file_name = file_path.name



        query_name = file_name.replace("'", "\\'")



        query = f"name = '{query_name}' and '{self.folder_id}' in parents and trashed = false"



        media = MediaFileUpload(



            str(file_path),



            mimetype=mimetypes.guess_type(file_name)[0] or "application/octet-stream",



            resumable=False,



        )







        list_kwargs = self._list_kwargs()



        write_kwargs = self._write_kwargs()







        try:



            response = (



                self.service.files()



                .list(q=query, fields="files(id,name)", pageSize=1, **list_kwargs)



                .execute()



            )



            files = response.get("files", [])



            if files:



                file_id = files[0]["id"]



                self.service.files().update(



                    fileId=file_id,



                    media_body=media,



                    **write_kwargs,



                ).execute()



                print(f"[Drive] Actualizado {file_name} ({file_id})")



                return







            metadata = {"name": file_name, "parents": [self.folder_id]}



            created = (



                self.service.files()



                .create(



                    body=metadata,



                    media_body=media,



                    fields="id",



                    **write_kwargs,



                )



                .execute()



            )



            print(f"[Drive] Creado {file_name} ({created['id']})")



        except HttpError as exc:



            raise RuntimeError(f"FallÃƒÂƒÃ‚Â³ la subida a Drive para {file_name}: {exc}") from exc











def create_driver(headless: bool = False) -> Chrome:



    """Crea una instancia de Chrome lista para automatizaciÃƒÂƒÃ‚Â³n."""



    chrome_options = Options()



    chrome_options.add_argument("--start-maximized")



    if headless:



        chrome_options.add_argument("--headless=new")







    service = Service(ChromeDriverManager().install())



    return webdriver.Chrome(service=service, options=chrome_options)











def table_to_dataframe(table_element) -> pd.DataFrame:



    """Convierte un elemento <table> en DataFrame."""



    html = table_element.get_attribute("outerHTML")



    dfs = pd.read_html(StringIO(html))



    if not dfs:



        return pd.DataFrame()



    df = dfs[0]



    df.columns = [str(col).strip() for col in df.columns]



    return df











def wait_for_nonempty_table(



    driver: Chrome,



    table_xpath: str,



    *,



    timeout: int = 30,



    min_rows: int = 1,



) -> WebElement:



    """Espera a que la tabla tenga registros reales y la devuelve."""







    def _table_if_ready(_driver: Chrome) -> WebElement | bool:



        try:



            table = _driver.find_element(By.XPATH, table_xpath)



            rows = table.find_elements(By.XPATH, "./tbody/tr")



        except (NoSuchElementException, StaleElementReferenceException):



            return False



        valid = 0



        try:



            for row in rows:



                cells = row.find_elements(By.TAG_NAME, "td")



                if not cells:



                    continue



                text = row.text.strip().lower()



                if not text:



                    continue



                if "no se encontraron" in text or "no matching records" in text:



                    continue



                valid += 1



        except StaleElementReferenceException:



            return False



        return table if valid >= min_rows else False







    return WebDriverWait(driver, timeout).until(_table_if_ready)



def wait_prm_idle(driver: Chrome, timeout: int = 15) -> bool:
    """Espera a que no haya postbacks asíncronos activos (UpdatePanel/jQuery)."""
    end_time = time.time() + timeout
    last_ok = False
    while time.time() < end_time:
        ok_ready = True
        ok_jq = True
        ok_prm = True

        try:
            ok_ready = driver.execute_script("return document.readyState") == "complete"
        except Exception:
            ok_ready = True

        try:
            jq_active = driver.execute_script(
                "return (window.jQuery && jQuery.active) ? jQuery.active : 0"
            )
            ok_jq = jq_active == 0
        except Exception:
            ok_jq = True

        try:
            prm_busy = driver.execute_script(
                "return (window.Sys && Sys.WebForms && Sys.WebForms.PageRequestManager) ? "
                "Sys.WebForms.PageRequestManager.getInstance().get_isInAsyncPostBack() : false;"
            )
            ok_prm = prm_busy is False
        except Exception:
            ok_prm = True

        last_ok = ok_ready and ok_jq and ok_prm
        if last_ok:
            return True
        time.sleep(0.2)
    if not last_ok:
        print("[WAIT][WARN] wait_prm_idle agotó timeout; continuando.")
    return False


def _table_signature(table: WebElement, sample_rows: int = 5) -> str:
    """Genera una huella ligera de la tabla para detectar si cambió."""
    signature_parts: list[str] = []
    try:
        rows = table.find_elements(By.XPATH, "./tbody/tr")
        for idx, row in enumerate(rows):
            if idx >= sample_rows:
                break
            cells = row.find_elements(By.TAG_NAME, "td")
            signature_parts.append("|".join(cell.text.strip() for cell in cells))
    except Exception:
        return ""
    return "||".join(signature_parts)


def _ensure_oferentes_table(driver: Chrome, wait: WebDriverWait, target_page: int) -> WebElement:
    """Garantiza que estemos en la tabla principal de oferentes (página deseada)."""
    for attempt in range(3):
        try:
            if "Oferentes.aspx" not in driver.current_url:
                driver.get(OFERENTES_URL)
                wait_prm_idle(driver, timeout=20)
            table = wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))
            if target_page > 1:
                driver.execute_script(
                    "__doPostBack(arguments[0], arguments[1]);",
                    "ctl00$MainContent$gvOferentes",
                    f"Page${target_page}",
                )
                wait_prm_idle(driver, timeout=20)
                table = wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))
            return table
        except TimeoutException:
            continue
    raise TimeoutException("No se pudo recuperar la tabla principal de oferentes.")


def _scrape_catalog_via_print_button(

    driver: Chrome,

    wait: WebDriverWait,

    base_entry: dict[str, Any],

    oferente_label: str,

) -> tuple[list[dict[str, Any]], str | None]:

    main_handle = driver.current_window_handle

    try:

        print_button = wait.until(

            EC.element_to_be_clickable(

                (

                    By.XPATH,

                    "//input[contains(@value,'Catálogo Completo') or "

                    "contains(@value,'Catalogo Completo') or contains(@id,'btnImprimir')]",

                )

            )

        )

    except TimeoutException:

        return [], "botón 'Imprimir Catálogo Completo' no disponible"



    handles_before = set(driver.window_handles)

    try:

        print_button.click()

    except WebDriverException as exc:

        return [], f"no se pudo hacer clic en Imprimir: {exc}"



    try:

        new_handle = WebDriverWait(driver, 10).until(

            lambda d: next((h for h in d.window_handles if h not in handles_before), None)

        )

    except TimeoutException:

        return [], "no se abrió la ventana de impresión"



    if not new_handle:

        return [], "no se identificó la ventana de impresión"



    driver.switch_to.window(new_handle)

    try:

        wait_print = WebDriverWait(driver, 30)

        table = wait_print.until(EC.presence_of_element_located((By.XPATH, "//table[1]")))

        catalog_df = clean_catalog_dataframe(table_to_dataframe(table))

    except TimeoutException:

        driver.close()

        driver.switch_to.window(main_handle)

        wait_prm_idle(driver, timeout=20)

        return [], "ventana de impresión sin tabla"



    driver.close()

    driver.switch_to.window(main_handle)

    wait_prm_idle(driver, timeout=20)



    rows: list[dict[str, Any]] = []

    for _, catalog_row in catalog_df.iterrows():

        combined = base_entry.copy()

        for col_name, value in catalog_row.items():

            combined[f"Catálogo:{col_name}"] = value

        rows.append(combined)



    return rows, None


def clean_catalog_dataframe(df: pd.DataFrame) -> pd.DataFrame:



    """Elimina filas del catÃƒÂƒÃ‚Â¡logo que corresponden a paginadores o filas vacÃƒÂƒÃ‚Â­as."""



    if df is None or df.empty:



        return pd.DataFrame()



    df = df.loc[:, ~df.columns.str.contains("^Unnamed", case=False)]



    if df.empty:



        return df







    pagination_keywords = {"...", "?", "anterior", "siguiente", "ultimo", "primero", "pagina"}



    pagination_pattern = re.compile(r"^[\d\s.,;:<>?????-]+$")







    def _strip_accents(text: str) -> str:



        return "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))







    def _is_pagination_row(row: pd.Series) -> bool:



        tokens = []



        for value in row.tolist():



            if pd.isna(value):



                continue



            text = str(value).strip()



            if not text:



                continue



            tokens.append(text)



        if not tokens:



            return True



        normalized = [_strip_accents(token).lower() for token in tokens]



        return all(



            token in pagination_keywords or pagination_pattern.fullmatch(token)



            for token in normalized



        )







    mask = ~df.apply(_is_pagination_row, axis=1)



    return df[mask].reset_index(drop=True)













def drop_numeric_pagination_rows(df: pd.DataFrame) -> pd.DataFrame:

    """Quita filas que solo contienen numeraciones o puntos usados como paginadores."""

    if df is None or df.empty:

        return pd.DataFrame()



    def _is_pagination_row(row: pd.Series) -> bool:

        has_value = False

        for value in row:

            if pd.isna(value):

                continue

            text = str(value).strip()

            if not text:

                continue

            has_value = True

            normalized = text.replace('.', '').replace('Ã¢Â€Â¦', '').replace(' ', '')

            if not normalized:

                continue

            if not normalized.isdigit():

                return False

        return has_value



    mask = df.apply(_is_pagination_row, axis=1)

    return df[~mask].reset_index(drop=True)





def _find_next_postback(



    driver: Chrome,



    current_page: int,



    *,



    table: WebElement | None = None,



):



    search_context = table if table is not None else driver



    anchors = search_context.find_elements(



        By.XPATH, ".//a[contains(@href,'Page$')]"



    )



    expected = f"Page${current_page + 1}"



    for anchor in anchors:



        args = parse_postback_arguments(anchor)



        if not args:



            continue



        text = anchor.text.strip()



        href = anchor.get_attribute("href") or ""



        if text.isdigit() and int(text) == current_page + 1:



            return args



        if expected in href:



            return args



    return None











def click_if_present(driver: Chrome, xpath: str, timeout: int = 5) -> bool:



    """Intenta hacer clic en un elemento si existe."""



    try:



        btn = WebDriverWait(driver, timeout).until(



            EC.element_to_be_clickable((By.XPATH, xpath))



        )



        btn.click()



        return True



    except TimeoutException:



        return False











def scrape_ctni_fichas(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:



    """Extrae las fichas tÃƒÂƒÃ‚Â©cnicas publicadas."""



    driver.get(CTNI_URL)
    print("[LOG] CTNI: portal cargado, esperando tablas...")



    wait = WebDriverWait(driver, 30)



    dataframes: list[pd.DataFrame] = []



    page = 1



    while True:



        table_xpath = "//table[@id='FichasTable']"



        table = wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))



        wait_for_nonempty_table(driver, table_xpath)



        df = table_to_dataframe(table)



        df["PÃƒÂƒÃ‚Â¡gina"] = page



        dataframes.append(df)
        print(f"[LOG] CTNI: pagina {page} procesada ({len(df)} filas)")



        if max_pages > 0 and page >= max_pages:



            break



        next_clicked = click_if_present(



            driver,



            "//li[@id='FichasTable_next' and not(contains(@class,'disabled'))]/a",



        )



        if not next_clicked:



            break



        time.sleep(1.5)



        page += 1



    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()











def scrape_criterios(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:



    """Extrae criterios t?cnicos desde el sitio ASP.NET."""



    driver.get(CRITERIOS_URL)
    print("[LOG] Criterios: portal cargado, preparando grid...")



    wait = WebDriverWait(driver, 30)







    # Algunas veces el grid se llena solo tras pulsar "Mostrar Todos".



    try:



        mostrar_todos = wait.until(



            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_cmdMostrarTodos"))



        )



        mostrar_todos.click()



        time.sleep(2)



    except TimeoutException:



        pass







    dataframes: list[pd.DataFrame] = []



    page = 1



    while True:



        table = wait.until(



            EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_Grddata"))



        )



        df = drop_numeric_pagination_rows(table_to_dataframe(table))

        df["PÃƒÂƒÃ‚Â¡gina"] = page



        dataframes.append(df)
        print(f"[LOG] Criterios: pagina {page} procesada ({len(df)} filas utiles)")



        if max_pages > 0 and page >= max_pages:



            break







        next_postback = _find_next_postback(driver, page, table=table)



        if not next_postback:



            break







        try:



            driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)



        except WebDriverException:



            break



        time.sleep(2)



        page += 1



    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()











def parse_postback_arguments(link) -> tuple[str, str] | None:



    """Extrae target y argumento de un enlace javascript:__doPostBack."""



    href = link.get_attribute("href")



    if not href:



        return None



    match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)



    if not match:



        return None



    return match.group(1), match.group(2)











def scrape_catalogo_public(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:
    global CATALOG_COUNTERS
    driver.get(CATALOGO_PUBLICO_URL)
    print("[LOG] Catálogo público: portal cargado, esperando tabla...")
    records: list[dict[str, Any]] = []
    signature_map: dict[tuple[str, ...], tuple[int, dict[str, Any]]] = {}
    page = 1
    row_sequence = 0

    stagnant_retries = 0
    signature_retries = 0
    last_signature: str | None = None
    while True:
        try:
            table = wait_for_nonempty_table(
                driver,
                CATALOGO_PUBLIC_TABLE_XPATH,
                timeout=40,
                min_rows=2,
            )
        except TimeoutException:
            print(f"[CAT][WARN] No se encontró tabla útil en la página {page}.")
            break

        df_page = table_to_dataframe(table)
        df_page = drop_numeric_pagination_rows(df_page)
        base_columns = list(df_page.columns)
        rows_total = len(df_page)
        added = 0
        current_signature = _table_signature(table)
        if last_signature is not None and current_signature == last_signature:
            signature_retries += 1
            if signature_retries < 3:
                print(
                    f"[CAT][WARN] Página {page} no cambió (huella repetida). "
                    f"Reintentando ({signature_retries}/3)..."
                )
                wait_prm_idle(driver, timeout=15)
                time.sleep(1.5)
                continue
            print(
                "[CAT][ERR] La tabla no cambió tras 3 intentos; se saltará a la página siguiente."
            )
            next_postback = _find_next_postback(driver, page, table=table)
            if not next_postback:
                print("[CAT][ERR] No se pudo localizar enlace a la siguiente página; se detiene.")
                break
            try:
                driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)
            except WebDriverException as exc:
                print(f"[NAV][ERR] No se pudo saltar a la página siguiente ({exc}); se detiene.")
                break
            try:
                WebDriverWait(driver, 25).until(EC.staleness_of(table))
            except TimeoutException:
                print("[CAT][ERR] La tabla no se actualizó tras intentar saltar; se detiene.")
                break
            wait_prm_idle(driver, timeout=20)
            time.sleep(1.0)
            page += 1
            last_signature = None
            signature_retries = 0
            stagnant_retries = 0
            continue
        else:
            signature_retries = 0
        last_signature = current_signature

        for _, row in df_page.iterrows():
            row_sequence += 1
            row_dict = {str(col): _sanitize_text(row.get(col, "")) for col in df_page.columns}
            has_payload = any(row_dict.get(col, "") for col in base_columns)
            if not has_payload:
                continue
            signature = tuple(row_dict.get(col, "") for col in base_columns)
            if signature in signature_map:
                original_row_number, original_row = signature_map[signature]
                CATALOG_COUNTERS.duplicate_rows += 1
                log_duplicate(
                    "oferentes_catalogos",
                    row_sequence,
                    original_row_number,
                    row_data=row_dict,
                    original_row_data=original_row,
                )
                continue
            signature_map[signature] = (row_sequence, row_dict.copy())
            records.append(row_dict)
            CATALOG_COUNTERS.unique_rows += 1
            added += 1

        CATALOG_COUNTERS.pages += 1
        print(
            f"[CAT] Página {page}: {added} filas nuevas (total página={rows_total}, acumulado={len(records)})"
        )

        if max_pages > 0 and page >= max_pages:
            break

        next_postback = _find_next_postback(driver, page, table=table)
        if not next_postback:
            break
        try:
            driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)
        except WebDriverException as exc:
            print(f"[NAV][WARN] No se pudo avanzar a la página {page + 1}: {exc}")
            break
        try:
            WebDriverWait(driver, 25).until(EC.staleness_of(table))
        except TimeoutException:
            stagnant_retries += 1
            print(
                f"[CAT][WARN] La tabla no cambió tras solicitar la página {page + 1} "
                f"(reintento {stagnant_retries}/3)."
            )
            if stagnant_retries >= 3:
                print("[CAT][ERR] Se alcanzó el máximo de reintentos; se detiene el scraping de catálogos.")
                break
        else:
            stagnant_retries = 0
            signature_retries = 0
        wait_prm_idle(driver, timeout=20)
        time.sleep(1.0)
        page += 1

    return pd.DataFrame(records)


def scrape_oferentes(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:



    """Descarga los oferentes y sus catÃƒÂƒÃ‚Â¡logos completos."""



    driver.get(OFERENTES_URL)
    print("[LOG] Oferentes: portal cargado, esperando listado...")



    wait = WebDriverWait(driver, 30)



    time.sleep(3)



    records: list[dict] = []



    page = 1



    table = _ensure_oferentes_table(driver, wait, page)



    while True:



        header_cells = table.find_elements(By.XPATH, ".//thead//th")



        headers = [cell.text.strip() or f"Columna {idx+1}" for idx, cell in enumerate(header_cells)]



        row_selector = ".//tbody/tr[not(contains(@style,'display:none'))]"



        row_idx = 0
        page_oferentes = 0

        while True:



            rows = table.find_elements(By.XPATH, row_selector)



            if row_idx >= len(rows):



                break



            row = rows[row_idx]



            cells = row.find_elements(By.XPATH, "./td")



            text_vals = [cell.text.strip() for cell in cells]



            base_entry = {



                "Oferente:No. de Oferente": text_vals[0] if len(text_vals) > 0 else "",



                "Oferente:Oferente": text_vals[1] if len(text_vals) > 1 else "",



                "Oferente:Vencimiento Cert.": text_vals[2] if len(text_vals) > 2 else "",



                "Oferente:Representante Legal": text_vals[3] if len(text_vals) > 3 else "",



                "Oferente:Contacto": text_vals[4] if len(text_vals) > 4 else "",



                "Oferente:TelóƒÂƒÃ‚Â©fono": text_vals[5] if len(text_vals) > 5 else "",



                "Oferente:Correo": text_vals[6] if len(text_vals) > 6 else "",



            }



            if len(cells) > 9:



                base_entry["Oferente:Certificado"] = cells[9].text.strip()



            catalog_link = None



            try:



                anchor = row.find_element(By.XPATH, ".//a[contains(@href,'Catalogo$')]")



                catalog_link = parse_postback_arguments(anchor)



            except NoSuchElementException:



                catalog_link = None







            oferente_label = (

                base_entry.get("Oferente:Oferente")

                or base_entry.get("Oferente:No. de Oferente")

                or f"Fila {page_oferentes + 1}"

            )

            page_oferentes += 1

            print(

                f"[LOG] Oferentes: procesando '{oferente_label}' (página {page}, fila {page_oferentes})"

            )



            catalog_rows_added = False
            catalog_rows_total = 0



            normalized_label = (oferente_label or "").upper()
            if any(skip_name.upper() in normalized_label for skip_name in SKIP_OFERENTE_NAMES):
                reason = "omitido por configuración (lista de exclusión)"
                print(f"[WARN] Oferentes: se omite '{oferente_label}' -> {reason}")
                SKIPPED_OFERENTES.append(
                    {
                        "oferente": oferente_label,
                        "pagina_listado": str(page),
                        "razon": reason,
                    }
                )
                row_idx += 1
                continue




            if catalog_link:

                target, argument = catalog_link

                driver.execute_script("__doPostBack(arguments[0], arguments[1]);", target, argument)

                catalog_page = 1
                catalog_stalled_reason: str | None = None


try:

    if CATALOG_SCRAPE_MODE == "download":

        rows, reason = _scrape_catalog_via_print_button(
            driver,
            wait,
            base_entry,
            oferente_label,
        )

        if rows:
            for combined in rows:
                records.append(combined)
                catalog_rows_total += 1
            catalog_rows_added = True
        else:
            catalog_stalled_reason = reason or "falló la descarga del catálogo completo"

    else:

        catalog_table = wait.until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(@id,'gvCatalogo')]"))
        )

        last_catalog_signature: str | None = None
        signature_retries = 0
        empty_retries = 0

        while True:

            catalog_df = clean_catalog_dataframe(table_to_dataframe(catalog_table))

            print(
                f"[LOG] Catálogo página {catalog_page} para '{oferente_label}': {len(catalog_df)} filas"
            )

            signature = _table_signature(catalog_table)
            if last_catalog_signature is not None and signature == last_catalog_signature:
                signature_retries += 1
            else:
                signature_retries = 0
                last_catalog_signature = signature

            if signature_retries >= 3:
                catalog_stalled_reason = f"tabla sin cambios (página {catalog_page})"
                break

            if catalog_df.empty:
                empty_retries += 1
            else:
                empty_retries = 0

            if empty_retries >= 3:
                catalog_stalled_reason = f"catálogo vacío repetido (página {catalog_page})"
                break

            if not catalog_df.empty:
                catalog_rows_added = True

            for _, catalog_row in catalog_df.iterrows():

                combined = base_entry.copy()

                for col_name, value in catalog_row.items():

                    combined[f"Catálogo:{col_name}"] = value

                records.append(combined)
                catalog_rows_total += 1

            next_postback = _find_next_postback(
                driver,
                catalog_page,
                table=catalog_table,
            )

            if not next_postback:
                break

            previous_table = catalog_table

            driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)

            try:
                WebDriverWait(driver, 15).until(EC.staleness_of(previous_table))
                catalog_table = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//table[contains(@id,'gvCatalogo')]"))
                )
            except TimeoutException:
                catalog_stalled_reason = f"timeout cambiando a la página {catalog_page + 1}"
                break

            catalog_page += 1

                except TimeoutException:

                    catalog_stalled_reason = "timeout esperando tabla del catálogo"
                finally:



                    if catalog_rows_total:



                        print(
                            f"[LOG] Oferentes: '{oferente_label}' acumuló {catalog_rows_total} filas de catálogos"
                        )



                    else:



                        print(
                            f"[LOG] Oferentes: '{oferente_label}' no mostró catálogos"
                        )



                    if catalog_stalled_reason:



                        print(
                            f"[WARN] Oferentes: '{oferente_label}' se saltó el catálogo -> {catalog_stalled_reason}"
                        )
                        SKIPPED_OFERENTES.append(
                            {
                                "oferente": oferente_label,
                                "pagina_listado": str(page),
                                "razon": catalog_stalled_reason,
                            }
                        )



                    # Cerrar catÃƒÂƒÃ‚Â¡logo y regresar al listado principal



                    clicked_back = click_if_present(



                        driver,



                        "//input[@id='MainContent_btnVolver']",



                        timeout=5,



                    )



                    if not clicked_back:



                        click_if_present(



                            driver,



                            "//input[contains(@id,'btnCerrar') or contains(@value,'Cerrar') or contains(@value,'Regresar')]",



                            timeout=5,



                        )



                    try:



                        wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))



                        table = driver.find_element(By.ID, "MainContent_gvOferentes")



                    except TimeoutException:



                        print("[Oferentes][WARN] No se pudo volver al listado; se recarga la página.")



                        table = _ensure_oferentes_table(driver, wait, page)







            if not catalog_rows_added:



                row_idx += 1



                continue







            row_idx += 1



        if max_pages > 0 and page >= max_pages:



            break







        next_page_postback = _find_next_postback(driver, page, table=table)



        if not next_page_postback:



            break



        try:



            driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_page_postback)



        except WebDriverException:



            break



        try:



            wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))



            table = driver.find_element(By.ID, "MainContent_gvOferentes")



        except TimeoutException:



            print("[Oferentes][WARN] No se pudo cargar la siguiente página; se recarga el listado.")



            target_page = page + 1



            table = _ensure_oferentes_table(driver, wait, target_page)



            page = target_page



            continue



        time.sleep(1)



        print(
            f"[LOG] Oferentes: página {page} completada ({page_oferentes} oferentes revisados)"
        )



        page += 1







    return pd.DataFrame(records)











def _deduplicate_dataframe(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])
    keys = build_key_series(df, columns)
    keep_mask = ~(keys.duplicated(keep="first") & keys.notna())
    return df[keep_mask].reset_index(drop=True)


def export_results(
    results: Iterable[ScrapeResult],
    output_dir: Path,
    *,
    drive_uploader: DriveUploader | None = None,
    ignore_existing: bool = True,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_paths: list[Path] = []
    for result in results:
        output_path = output_dir / f"{result.name}.xlsx"
        df = normalize_dataframe_columns(result.dataframe)
        dedup_columns = DEDUP_CONFIG.get(result.name)
        new_rows_count = len(df)
        if dedup_columns:
            existing_len = 0
            if not ignore_existing and output_path.exists():
                existing_df = normalize_dataframe_columns(pd.read_excel(output_path))
                existing_len = len(existing_df)
                df = pd.concat([existing_df, df], ignore_index=True)
            df = _deduplicate_dataframe(df, dedup_columns)
            if not ignore_existing:
                new_rows_count = max(0, len(df) - existing_len)
            else:
                new_rows_count = len(df)
        df = sanitize_dataframe_for_excel(df)
        df.to_excel(output_path, index=False)
        if dedup_columns:
            print(f"[LOG] {result.name}: añadidos {new_rows_count} nuevos (total {len(df)})")
        else:
            print(f"[LOG] {result.name}: exportadas {len(df)} filas (sin deduplicación)")
        print(f"[OK] {result.name} -> {output_path}")
        saved_paths.append(output_path)
        if drive_uploader is not None:
            drive_uploader.upload(output_path)
    return saved_paths



def export_duplicate_report(
    output_dir: Path,
    *,
    drive_uploader: DriveUploader | None = None,
) -> Path | None:
    if not DUPLICATE_LOGS:
        print("[DUP] No se detectaron filas duplicadas durante la corrida.")
        return None
    rows: list[dict[str, Any]] = []
    for entry in DUPLICATE_LOGS:
        rows.append(
            {
                "dataset": entry.dataset,
                "fila_duplicada": entry.row_number,
                "fila_original": entry.original_row_number,
                "datos_duplicado": json.dumps(entry.row_data, ensure_ascii=False),
                "datos_original": json.dumps(entry.original_row_data, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    df = sanitize_dataframe_for_excel(df)
    path = output_dir / "duplicates_oferentes_catalogos.xlsx"
    df.to_excel(path, index=False)
    print(f"[DUP] Reporte de duplicados -> {path}")
    if drive_uploader is not None:
        drive_uploader.upload(path)
    return path



def print_catalog_summary() -> None:
    if (
        CATALOG_COUNTERS.pages == 0
        and CATALOG_COUNTERS.unique_rows == 0
        and CATALOG_COUNTERS.duplicate_rows == 0
    ) and not SKIPPED_OFERENTES:
        return
    print(
        f"[REPORT] Catálogos únicos(run)={CATALOG_COUNTERS.unique_rows} | duplicados(run)={CATALOG_COUNTERS.duplicate_rows}"
    )
    if SKIPPED_OFERENTES:
        print("[REPORT] Oferentes saltados por catálogo:")
        for entry in SKIPPED_OFERENTES:
            print(
                "  - {oferente} (página listado {page}): {reason}".format(
                    oferente=entry.get("oferente", "?"),
                    page=entry.get("pagina_listado", "?"),
                    reason=entry.get("razon", "motivo desconocido"),
                )
            )
    else:
        print("[REPORT] Oferentes saltados: ninguno.")



def parse_args(argv: list[str]) -> argparse.Namespace:



    parser = argparse.ArgumentParser(



        description="Scraper de fichas, criterios y oferentes del MINSA."



    )



    parser.add_argument(



        "--headless",



        action="store_true",



        help="Ejecuta Chrome en modo headless.",



    )



    parser.add_argument(



        "--max-pages",



        type=int,



        default=0,



        help="NÃºmero mÃ¡ximo de pÃ¡ginas por portal (0 recorre todas).",



    )



    parser.add_argument(



        "--skip",



        choices=["fichas", "criterios", "oferentes"],



        nargs="*",



        default=[],



        help="Portales que quieres omitir en esta ejecuciÃƒÂƒÃ‚Â³n.",



    )
    parser.add_argument(



        "--mode",



        choices=["all", "fichas", "criterios", "catalogo"],



        default="all",



        help="Define qué portal ejecutar (all=fichas+criterios+catálogo).",



    )



    parser.add_argument(



        "--oferentes-source",



        choices=["catalog", "legacy"],



        default="legacy",



        help=(



            "Origen del dataset de oferentes: 'catalog' usa el catálogo público, 'legacy' el flujo antiguo."



        ),



    )



    parser.add_argument(



        "--catalog-mode",



        choices=["legacy", "download"],



        default="legacy",



        help=(



            "Modo de extracción de catálogos por oferente: 'legacy' pagina la tabla, "



            "'download' usa el botón 'Imprimir Catálogo Completo'."



        ),



    )







    parser.add_argument(



        "--respect-existing-output",



        action="store_true",



        help="Mantiene los datos previos de los Excel y suma nuevos registros en vez de recalcular desde cero.",



    )




    parser.add_argument(



        "--upload-to-drive",



        dest="upload_to_drive",



        action="store_true",



        default=True,



        help="Sube los archivos generados a Google Drive (por defecto activado).",



    )



    parser.add_argument(



        "--no-upload",



        dest="upload_to_drive",



        action="store_false",



        help="Desactiva la subida automática a Drive para esta ejecución.",



    )



    parser.add_argument(



        "--drive-folder-id",



        default=DEFAULT_DRIVE_FOLDER_ID,



        help="ID de la carpeta de Drive destino.",



    )



    parser.add_argument(



        "--drive-credentials",



        type=Path,



        default=DEFAULT_DRIVE_CREDENTIALS,



        help="Ruta del JSON del service account con permisos de Drive.",



    )



    return parser.parse_args(argv)











def resolve_drive_uploader(args: argparse.Namespace) -> DriveUploader | None:



    if not args.upload_to_drive:



        return None



    if not args.drive_folder_id:



        raise SystemExit("Debes indicar --drive-folder-id para poder subir a Drive.")



    credentials_path = args.drive_credentials



    if credentials_path is None:



        raise SystemExit("Debes indicar --drive-credentials con el JSON del service account.")



    credentials_path = Path(credentials_path)



    if not credentials_path.exists():



        raise SystemExit(f"No se encontrÃƒÂƒÃ‚Â³ el archivo de credenciales: {credentials_path}")



    print(f"[Drive] Carpeta destino: {args.drive_folder_id}")



    return DriveUploader.from_service_account(credentials_path, args.drive_folder_id)











def run_with_driver(



    scraper: Callable[[Chrome, int], pd.DataFrame],



    *,



    headless: bool,



    max_pages: int,



) -> pd.DataFrame:



    driver = create_driver(headless=headless)



    try:



        return scraper(driver, max_pages)



    finally:



        driver.quit()











def main(argv: list[str] | None = None) -> int:

    global SKIPPED_OFERENTES, CATALOG_SCRAPE_MODE

    SKIPPED_OFERENTES = []

    args = parse_args(argv or sys.argv[1:])
    drive_uploader = resolve_drive_uploader(args)
    CATALOG_SCRAPE_MODE = args.catalog_mode

    def _mode_allows(target: str) -> bool:
        if args.mode == "all":
            return True
        if target == "oferentes":
            return args.mode == "catalogo"
        return args.mode == target

    exported_any = False
    ran_catalog = False

    if _mode_allows("fichas") and "fichas" not in args.skip:
        print("[LOG] Iniciando scraping de fichas CTNI...")
        df_fichas = run_with_driver(
            scrape_ctni_fichas,
            headless=args.headless,
            max_pages=args.max_pages,
        )
        export_results(
            [ScrapeResult("fichas_ctni", df_fichas)],
            DEFAULT_OUTPUT_DIR,
            drive_uploader=drive_uploader,
            ignore_existing=not args.respect_existing_output,
        )
        print(f"[LOG] Fichas CTNI: recuperadas {len(df_fichas)} filas.")
        exported_any = True

    if _mode_allows("criterios") and "criterios" not in args.skip:
        print("[LOG] Iniciando scraping de criterios técnicos...")
        df_criterios = run_with_driver(
            scrape_criterios,
            headless=args.headless,
            max_pages=args.max_pages,
        )
        export_results(
            [ScrapeResult("criterios_tecnicos", df_criterios)],
            DEFAULT_OUTPUT_DIR,
            drive_uploader=drive_uploader,
            ignore_existing=not args.respect_existing_output,
        )
        print(f"[LOG] Criterios técnicos: recuperadas {len(df_criterios)} filas.")
        exported_any = True

    if _mode_allows("oferentes") and "oferentes" not in args.skip:
        print(f"[LOG] Iniciando scraping de oferentes ({args.oferentes_source})...")
        scraper_fn = (
            scrape_catalogo_public if args.oferentes_source == "catalog" else scrape_oferentes
        )
        df_oferentes = run_with_driver(
            scraper_fn,
            headless=args.headless,
            max_pages=args.max_pages,
        )
        export_results(
            [ScrapeResult("oferentes_catalogos", df_oferentes)],
            DEFAULT_OUTPUT_DIR,
            drive_uploader=drive_uploader,
            ignore_existing=not args.respect_existing_output,
        )
        print(f"[LOG] Oferentes: recuperadas {len(df_oferentes)} filas.")
        ran_catalog = True
        exported_any = True

    if ran_catalog:
        export_duplicate_report(DEFAULT_OUTPUT_DIR, drive_uploader=drive_uploader)
        print_catalog_summary()

    if not exported_any:
        print("[WARN] No se ejecutó ningún portal (revisa --mode y --skip).")

    return 0


if __name__ == "__main__":



    raise SystemExit(main())









