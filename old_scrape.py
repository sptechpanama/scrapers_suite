# -*- coding: utf-8 -*-
"""Scraper de portales pÃÂºblicos del MINSA usando Selenium."""







from __future__ import annotations







import argparse



import mimetypes



import re



import sys



import time



import unicodedata



from dataclasses import dataclass



from io import StringIO



from pathlib import Path



from typing import Any, Callable, Iterable, Sequence







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
    "oferentes_catalogos": ("No. de Oferente", "Nombre del Producto"),
}

DEFAULT_DRIVE_CREDENTIALS = _DEFAULT_CREDENTIALS_PATH if _DEFAULT_CREDENTIALS_PATH.exists() else None











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


def _column_series(df: pd.DataFrame, column: str) -> pd.Series:

    if column in df.columns:

        return df[column].astype(str).fillna("").str.strip()

    return pd.Series([""] * len(df), index=df.index)


def build_key_series(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:

    if not columns:

        raise ValueError("Se requieren columnas para construir la clave de deduplicación.")

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



    """PequeÃÂ±o helper para subir archivos a Google Drive."""







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



            raise FileNotFoundError(f"No se encontrÃÂ³ el archivo a subir: {file_path}")







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



            raise RuntimeError(f"FallÃÂ³ la subida a Drive para {file_name}: {exc}") from exc











def create_driver(headless: bool = False) -> Chrome:



    """Crea una instancia de Chrome lista para automatizaciÃÂ³n."""



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



) -> None:



    """Espera a que la tabla tenga registros reales (no mensajes vacÃÂ­os)."""







    def _has_data(_driver: Chrome) -> bool:



        rows = _driver.find_elements(By.XPATH, f"{table_xpath}/tbody/tr")



        valid = 0



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



        return valid >= min_rows







    WebDriverWait(driver, timeout).until(_has_data)











def clean_catalog_dataframe(df: pd.DataFrame) -> pd.DataFrame:



    """Elimina filas del catÃÂ¡logo que corresponden a paginadores o filas vacÃÂ­as."""



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

            normalized = text.replace('.', '').replace('â¦', '').replace(' ', '')

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



    """Extrae las fichas tÃÂ©cnicas publicadas."""



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



        df["PÃÂ¡gina"] = page



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

        df["PÃÂ¡gina"] = page



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











def scrape_oferentes(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:



    """Descarga los oferentes y sus catÃÂ¡logos completos."""



    driver.get(OFERENTES_URL)
    print("[LOG] Oferentes: portal cargado, esperando listado...")



    wait = WebDriverWait(driver, 30)



    time.sleep(3)



    records: list[dict] = []



    page = 1



    while True:



        try:



            table = driver.find_element(By.ID, "MainContent_gvOferentes")



        except NoSuchElementException:



            Path("debug_oferentes.html").write_text(driver.page_source, encoding="utf-8")



            raise TimeoutException("No se pudo ubicar la tabla de oferentes.")



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



                "Oferente::No. de Oferente": text_vals[0] if len(text_vals) > 0 else "",



                "Oferente::Oferente": text_vals[1] if len(text_vals) > 1 else "",



                "Oferente::Vencimiento Cert.": text_vals[2] if len(text_vals) > 2 else "",



                "Oferente::Representante Legal": text_vals[3] if len(text_vals) > 3 else "",



                "Oferente::Contacto": text_vals[4] if len(text_vals) > 4 else "",



                "Oferente::TelÃÂ©fono": text_vals[5] if len(text_vals) > 5 else "",



                "Oferente::Correo": text_vals[6] if len(text_vals) > 6 else "",



            }



            if len(cells) > 9:



                base_entry["Oferente::Certificado"] = cells[9].text.strip()



            catalog_link = None



            try:



                anchor = row.find_element(By.XPATH, ".//a[contains(@href,'Catalogo$')]")



                catalog_link = parse_postback_arguments(anchor)



            except NoSuchElementException:



                catalog_link = None







            oferente_label = (

                base_entry.get("Oferente::Oferente")

                or base_entry.get("Oferente::No. de Oferente")

                or f"Fila {page_oferentes + 1}"

            )

            page_oferentes += 1

            print(

                f"[LOG] Oferentes: procesando '{oferente_label}' (p�gina {page}, fila {page_oferentes})"

            )



            catalog_rows_added = False
            catalog_rows_total = 0



            if catalog_link:



                target, argument = catalog_link



                driver.execute_script("__doPostBack(arguments[0], arguments[1]);", target, argument)



                catalog_page = 1



                try:



                    catalog_table = wait.until(



                        EC.presence_of_element_located((By.XPATH, "//table[contains(@id,'gvCatalogo')]"))



                    )



                    while True:



                        catalog_df = clean_catalog_dataframe(table_to_dataframe(catalog_table))

                        print(f"[LOG] Cat�logo p�gina {catalog_page} para '{oferente_label}': {len(catalog_df)} filas")



                        for _, catalog_row in catalog_df.iterrows():



                            combined = base_entry.copy()



                            for col_name, value in catalog_row.items():



                                combined[f"CatÃ¡logo::{col_name}"] = value



                            records.append(combined)



                            catalog_rows_added = True



                        next_postback = _find_next_postback(



                            driver,



                            catalog_page,



                            table=catalog_table,



                        )



                        if not next_postback:



                            break



                        previous_table = catalog_table



                        driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)



                        WebDriverWait(driver, 15).until(EC.staleness_of(previous_table))



                        catalog_table = WebDriverWait(driver, 15).until(



                            EC.presence_of_element_located((By.XPATH, "//table[contains(@id,'gvCatalogo')]"))



                        )



                        catalog_page += 1



                except TimeoutException:



                    pass



                finally:



                    if catalog_rows_total:



                        print(
                            f"[LOG] Oferentes: '{oferente_label}' acumul� {catalog_rows_total} filas de cat�logos"
                        )



                    else:



                        print(
                            f"[LOG] Oferentes: '{oferente_label}' no mostr� cat�logos"
                        )



                    # Cerrar catÃÂ¡logo y regresar al listado principal



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



                    wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))



                    table = driver.find_element(By.ID, "MainContent_gvOferentes")







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



        wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))



        time.sleep(1)



        print(
            f"[LOG] Oferentes: p�gina {page} completada ({page_oferentes} oferentes revisados)"
        )



        page += 1







    return pd.DataFrame(records)











def export_results(results: Iterable[ScrapeResult], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_paths: list[Path] = []
    for result in results:
        output_path = output_dir / f"{result.name}.xlsx"
        df = normalize_dataframe_columns(result.dataframe)
        dedup_columns = DEDUP_CONFIG.get(result.name)
        existing_df = None
        new_rows_count = len(df)
        if dedup_columns:
            existing_keys: set[str] = set()
            if output_path.exists():
                existing_df = normalize_dataframe_columns(pd.read_excel(output_path))
                existing_keys = set(
                    build_key_series(existing_df, dedup_columns).dropna().tolist()
                )
            key_series = build_key_series(df, dedup_columns)
            duplicate_mask = key_series.duplicated(keep="first") & key_series.notna()
            mask = ~duplicate_mask
            if existing_keys:
                mask &= key_series.isna() | ~key_series.isin(existing_keys)
            df_new = df[mask].copy()
            new_rows_count = len(df_new)
            if existing_df is not None and not existing_df.empty:
                df = pd.concat([existing_df, df_new], ignore_index=True)
            else:
                df = df_new
            if df is not None and not df.empty:
                all_keys = build_key_series(df, dedup_columns)
                keep_mask = ~(all_keys.duplicated(keep="first") & all_keys.notna())
                df = df[keep_mask].reset_index(drop=True)
        df.to_excel(output_path, index=False)
        if dedup_columns:
            print(
                f"[LOG] {result.name}: añadidos {new_rows_count} nuevos (total {len(df)})"
            )
        else:
            print(f"[LOG] {result.name}: exportadas {len(df)} filas (sin deduplicación)")
        print(f"[OK] {result.name} -> {output_path}")
        saved_paths.append(output_path)
    return saved_paths


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



        help="Número máximo de páginas por portal (0 recorre todas).",



    )



    parser.add_argument(



        "--skip",



        choices=["fichas", "criterios", "oferentes"],



        nargs="*",



        default=[],



        help="Portales que quieres omitir en esta ejecuciÃÂ³n.",



    )



    parser.add_argument(



        "--upload-to-drive",



        action="store_true",



        help="Sube los archivos generados a Google Drive al finalizar.",



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



        raise SystemExit(f"No se encontrÃÂ³ el archivo de credenciales: {credentials_path}")



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



    args = parse_args(argv or sys.argv[1:])



    drive_uploader = resolve_drive_uploader(args)







    results: list[ScrapeResult] = []



    if "fichas" not in args.skip:

        print("[LOG] Iniciando scraping de fichas CTNI...")



        df_fichas = run_with_driver(



            scrape_ctni_fichas,



            headless=args.headless,



            max_pages=args.max_pages,



        )



        results.append(ScrapeResult("fichas_ctni", df_fichas))
        print(f"[LOG] Fichas CTNI: recuperadas {len(df_fichas)} filas.")







    if "criterios" not in args.skip:

        print("[LOG] Iniciando scraping de criterios técnicos...")



        df_criterios = run_with_driver(



            scrape_criterios,



            headless=args.headless,



            max_pages=args.max_pages,



        )



        results.append(ScrapeResult("criterios_tecnicos", df_criterios))
        print(f"[LOG] Criterios técnicos: recuperadas {len(df_criterios)} filas.")







    if "oferentes" not in args.skip:

        print("[LOG] Iniciando scraping de oferentes y catálogos...")



        df_oferentes = run_with_driver(



            scrape_oferentes,



            headless=args.headless,



            max_pages=args.max_pages,



        )



        results.append(ScrapeResult("oferentes_catalogos", df_oferentes))
        print(f"[LOG] Oferentes: recuperadas {len(df_oferentes)} filas.")







    saved_paths = export_results(results, DEFAULT_OUTPUT_DIR)



    if drive_uploader and saved_paths:



        drive_uploader.upload_many(saved_paths)



    return 0











if __name__ == "__main__":



    raise SystemExit(main())











