"""Public access instructions, never synthetic tenders or authenticated scraping."""
from ..models import normalized_text
from .base import SourceAdapter
from .public_pages import html_page


class SupplierPortalAdapter(SourceAdapter):
    required_terms = ()
    pending = ''

    def fetch_opportunities(self):
        soup = html_page(self.client, self.url)
        text = normalized_text((soup.select_one('main') or soup).get_text(' ', strip=True))
        if not all(term in text for term in self.required_terms):
            raise RuntimeError('La página de proveedores cambió; revisar las instrucciones oficiales de acceso')
        self.pages_fetched = 1
        return []

    def fetch(self):
        result = super().fetch()
        if result.status == 'success':
            result.status = 'access_required'
            result.coverage = self.pending + '. Página de acceso verificada; licitaciones privadas no consultadas.'
        return result


class NaturgyAdapter(SupplierPortalAdapter):
    source = 'naturgy'
    source_name = 'Naturgy Panamá · Acceso a proveedores'
    url = 'https://www.naturgy.com.pa/proveedores/'
    required_terms = ('evaluacion inicial', 'compraseinstalaciones')
    pending = 'Pendiente: solicitar invitación a Compras y completar evaluación de proveedor para RS/SP'


class AesAdapter(SupplierPortalAdapter):
    source = 'aes'
    source_name = 'AES Panamá · Acceso a proveedores'
    url = 'https://www.aespanama.com/es/proveedores'
    required_terms = ('ariba', 'invitacion', 'precalificacion')
    pending = 'Pendiente: registro en Ariba, precalificación e invitación de AES para RS/SP'
