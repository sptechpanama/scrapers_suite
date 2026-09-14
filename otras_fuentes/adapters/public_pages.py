"""Small, shared parsers for public purchasing pages; no browser or credentials."""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlsplit

from ..models import SourceDocument, clean_text
from ..qualification import deadline_info
from .base import soup_from_html


def official_date(value: object, *, local_time: bool = False) -> str:
    raw = clean_text(value)
    short = re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", raw)
    if short:
        raw = raw[:short.start()] + f"{short[1]}/{short[2]}/20{short[3]}" + raw[short.end():]
    day, _ = deadline_info(raw)
    if not day:
        return ""
    hour = re.search(r"(?:T|\s)(\d{1,2}):(\d{2})\s*(am|pm)?", raw, re.I)
    if local_time and hour:
        h = int(hour[1])
        if hour[3]:
            h = h % 12 + (12 if hour[3].lower() == "pm" else 0)
        try:
            return datetime.fromisoformat(day).replace(hour=h, minute=int(hour[2]), tzinfo=timezone(timedelta(hours=-5))).isoformat()
        except ValueError:
            return ""
    return day


def document_links(node, base: str) -> list[SourceDocument]:
    found = {}
    for a in node.select("a[href]"):
        url = urljoin(base, a["href"])
        path = urlsplit(url).path.lower()
        if urlsplit(url).scheme not in {"https", "http"}:
            continue
        if path.endswith((".pdf", ".doc", ".docx", ".xlsx", ".xls", ".zip")) or any(t in path for t in ("/download/", "descargar", "downloadfile")):
            title = clean_text(a.get_text(" ", strip=True)) or "Documento oficial"
            found[url] = SourceDocument(title=title, url=url)
    return list(found.values())


def html_page(client, url, **kwargs):
    response = client.get(url, **kwargs).response
    soup = soup_from_html(response.text)
    title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
    if any(t in title for t in ("just a moment", "access denied", "403 forbidden", "iniciar sesión")):
        raise RuntimeError("El portal no entregó el listado público; acceso temporalmente no disponible")
    return soup


def next_pages(soup, base: str) -> list[str]:
    host = urlsplit(base).netloc
    return list(dict.fromkeys(
        urljoin(base, a["href"]) for a in soup.select('a[rel="next"][href], .pagination a[href], .page-numbers[href], a[aria-label*="Next"][href]')
        if urlsplit(urljoin(base, a["href"])).netloc == host
        and not a.find_parent(attrs={"aria-disabled": "true"})
    ))
