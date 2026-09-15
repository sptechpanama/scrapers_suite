"""Product-word screening for external portals; never asserts a MINSA ficha match."""
from __future__ import annotations

import re
from functools import lru_cache

from .models import clean_text, normalized_text

STOPWORDS = frozenset('a al con de del el en la las los o para por un una y kit kits equipo equipos paquete paquetes sistema universal adulto adultos pediatrico pediatrica neonatal ninos'.split())

# Enabled by the watched PRODUCT NAME, not by any hard-coded ficha number.
# Groups require all their words close together. Aliases indicate a product
# family for commercial review, not equivalence in size/specification to MINSA.
FAMILIES = (
    (('circuit*', 'anest*'), (('circuit*', 'anest*'), ('circuit*', 'respiratori*'), ('circuit*', 'paciente*'), ('anesthe*', 'circuit*'), ('anaesthe*', 'circuit*'), ('breathing', 'circuit*'))),
    (('circuit*', 'ventilador*'), (('circuit*', 'ventilador*'), ('ventilator*', 'circuit*'), ('breathing', 'circuit*'))),
    (('mascarill*', 'traqueostom*'), (('mascarill*', 'traqueostom*'), ('tracheostom*', 'mask*'))),
    (('tubo*', 'endotraque*'), (('tubo*', 'endotraque*'), ('endotracheal', 'tube*'))),
    (('micronebuliz*',), (('micronebulizador*',), ('nebulizador*',), ('nebulizer*',), ('nebuliser*',), ('mascarill*', 'nebuliz*'), ('mascara*', 'nebuliz*'), ('equipo*', 'nebuliz*'), ('kit*', 'nebuliz*'))),
    (('nebuliz*',), (('nebulizador*',), ('micronebulizador*',), ('nebulizer*',), ('nebuliser*',), ('mascarill*', 'nebuliz*'), ('mascara*', 'nebuliz*'), ('equipo*', 'nebuliz*'), ('kit*', 'nebuliz*'))),
    (('humidificador*', 'alto', 'flujo'), (('humidificador*', 'alto', 'flujo'), ('humificador*', 'alto', 'flujo'), ('high', 'flow', 'humidifi*'))),
    (('prueba*', 'penetracion', 'extraccion'), (('prueba*', 'penetracion', 'extraccion'), ('bowie', 'dick'), ('steam', 'penetration', 'test*'))),
    (('envoltura*', 'esteriliz*'), (('envoltura*', 'esteriliz*'), ('envoltura*', 'sms'), ('steriliz*', 'wrap*'), ('sterilis*', 'wrap*'))),
    (('respirador*', 'facial', 'particula*'), (('respirador*', 'facial', 'particula*'), ('respirador*', 'n95'), ('respirator*', 'n95'), ('mascarill*', 'n95'), ('mask*', 'n95'))),
)


def usable_name(value):
    name = clean_text(value)
    norm = normalized_text(name)
    if not norm or norm in {'si', 'no', 'nan', 'none'}:
        return ''
    if re.fullmatch(r'(?:ficha(?: tecnica)?\s*)?\d+', norm):
        return ''
    return name


@lru_cache(maxsize=1024)
def _pattern(term):
    return re.compile(r'\b' + re.escape(term.rstrip('*')) + (r'[a-z]*\b' if term.endswith('*') else r'\b'))


def _group_spans(norm, group, window=95):
    """All anchors in a small span, never unrelated words across a long PDF."""
    spans = [[m.span() for m in _pattern(term).finditer(norm)] for term in group]
    if not spans or any(not hits for hits in spans):
        return
    # Search only forward from each candidate minimum position; no cartesian product.
    import bisect
    for start in sorted({p[0] for hits in spans for p in hits}):
        chosen = []
        for hits in spans:
            index = bisect.bisect_left(hits, (start, -1))
            if index == len(hits) or hits[index][1] - start > window:
                break
            chosen.append(hits[index])
        if len(chosen) == len(spans):
            end = max(p[1] for p in chosen)
            yield start, end


def _match_group(norm, group, window=95):
    return next(_group_spans(norm, group, window), None)


def _plural_root(word):
    if len(word) >= 6 and word.endswith('es'):
        return word[:-2] + '*'
    if len(word) >= 5 and word.endswith('s'):
        return word[:-1] + '*'
    if len(word) >= 5 and word.endswith(('o', 'a')):
        return word[:-1] + '*'
    if len(word) >= 5 and word.endswith('e'):
        return word + '*'
    return word


@lru_cache(maxsize=1024)
def product_groups(name):
    norm = normalized_text(name)
    groups = []
    for trigger, aliases in FAMILIES:
        if _match_group(norm, trigger, window=200):
            groups.extend(aliases)
    # Names not in the curated families still work conservatively: retain all
    # meaningful tokens (up to six), including product qualifiers. Drop the
    # last clipped token rather than requiring "ANESTE..." literally.
    complete = re.sub(r'\w*(?:\.{3}|…)\s*$', '', name)
    tokens = list(dict.fromkeys(w for w in normalized_text(complete).split()
                               if len(w) > 2 and not w.isdigit() and w not in STOPWORDS))
    if len(tokens) >= 2:
        groups.append(tuple(_plural_root(w) for w in tokens[:6]))
    return tuple(dict.fromkeys(groups))


def match_products(value, products, field):
    norm = normalized_text(value)
    found = []
    for product in products:
        name = usable_name(product.get('name'))
        if not name:
            continue
        hit = None
        for group in product_groups(name):
            for start, end in _group_spans(norm, group):
                context = norm[max(0, start - 100):end + 100]
                if field == 'documento' and re.search(r'\b(?:personal del contratista|sus trabajadores|sus empleados|sus operarios)\b', context):
                    continue
                if any('nebul' in t for t in group) and re.search(r'\b(?:fumig\w*|insecticid\w*|plaguicid\w*|agricol\w*|mosquito\w*|termonebul\w*)\b', context):
                    continue
                if any(t.startswith('circuit') for t in group) and any(t.startswith('ventila') for t in group):
                    if end - start > 55 or re.search(r'\b(?:control|electric\w*|motor\w*|techo)\b', norm[start:end]):
                        continue
                hit = {'ficha': product['ficha'], 'name': name, 'field': field,
                       'terms': list(group), 'evidence': norm[max(0,start-40):end+60],
                       'basis': 'Palabras del producto; familia candidata, no equivalencia de ficha'}
                break
            if hit:
                found.append(hit)
                break
    return found
