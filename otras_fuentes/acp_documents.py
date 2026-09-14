"""Read public SLI links exposed by its own document controls."""
from urllib.parse import urlsplit, parse_qs, urlencode


def acp_detail(soup, original_url):
    # The search sidebar contains every buying category. It is not tender evidence.
    for node in soup.select('form, select, script, style, nav, footer, header'):
        node.decompose()
    text = soup.get_text(' ', strip=True)
    start = text.find('Licitación No.')
    if start < 0:
        raise ValueError('SLI no entregó el detalle público de la licitación')
    text = text[start:]
    for end in ('Manual de usuario', 'Información Adicional ×'):
        text = text.split(end)[0]
    query = parse_qs(urlsplit(original_url).query)
    code = (query.get('NumeroLicitacion') or [''])[0]
    header = (query.get('HeaderId') or [''])[0]
    base = 'https://apps.pancanal.com/sli/'
    links = []
    def add(title, path, params):
        links.append({'title': title, 'url': base + path + '?' + urlencode(params)})
    if soup.select_one('.Modal_PC_1_2') and header and code:
        add('Pliego de cargos · Partes 1 y 2', 'Comunes/ImpresionPliegoCargo_1_2', {'po_header': header, 'p_rfq': code})
    if soup.select_one('.Modal_PC_3_4') and code:
        add('Pliego de cargos · Partes 3 y 4', 'Comunes/ImpresionPliegoCargo_3_4', {'p_rfq': code})
    for a in soup.select('.selectNumberAttachment[data-anexos]'):
        add('Anexo ' + a['data-anexos'], 'Licitaciones/GetPliegoAttachment', {'rfqNumberAttachment': a['data-anexos']})
    for a in soup.select('.selectAI[data-poline]'):
        add('Renglón · ' + a.get('data-linedesc', a['data-poline']), 'Comunes/ImpresionInformacionAdicional', {'po_lineid': a['data-poline']})
    return text, links
