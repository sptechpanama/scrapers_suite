"""Narrow context exclusions for catalog names shared with industrial materials.

Inputs are already accent-free, lowercase text from the ficha detector. This
does not filter the opportunity, change its text or invalidate explicit codes.
"""
import re

_SILVER_SOLDER = re.compile(r'\bsoldaduras?\s+(?:de\s+)?plata\b')
_SILVER = re.compile(r'\bplata\b')


def catalog_name_context_allowed(name: str, text: str) -> bool:
    if name != 'plata':
        return True
    # Ignore only the metal named in solder. A separate medical mention in the
    # same act must remain detectable (e.g. nitrato de plata / plata coloidal).
    return bool(_SILVER.search(_SILVER_SOLDER.sub(' ', text)))
