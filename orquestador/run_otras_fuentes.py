from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from otras_fuentes import run_monitor


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor diario de fuentes externas")
    publication = parser.add_mutually_exclusive_group()
    publication.add_argument("--require-postgres", action="store_true", help='Supabase obligatorio (comportamiento predeterminado)')
    publication.add_argument('--local-only', action='store_true', help='Permitir una captura local sin publicación en Supabase')
    parser.add_argument('--silent', action='store_true', help='Actualizar sin generar eventos de correo')
    parser.add_argument('--sources', nargs='+', help='Identificadores de fuentes; por defecto todas')
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    from otras_fuentes.monitor import DEFAULT_ADAPTERS
    selected = DEFAULT_ADAPTERS
    if args.sources:
        unknown = set(args.sources) - {cls.source for cls in DEFAULT_ADAPTERS}
        if unknown:
            parser.error('Fuentes desconocidas: ' + ', '.join(sorted(unknown)))
        selected = tuple(cls for cls in DEFAULT_ADAPTERS if cls.source in args.sources)
    if args.silent:
        os.environ['OTRAS_FUENTES_SILENT_RUN'] = '1'
    silent = os.getenv('OTRAS_FUENTES_SILENT_RUN', '').lower() in {'1', 'true', 'yes', 'si', 'sí'}
    if not silent:
        # Retry durable pending mail even when the next capture subsequently
        # fails. The child process loads current code without restarting jobs.
        _deliver_notifications()
    result = run_monitor(selected, require_postgres=not args.local_only)
    summary = result.summary()
    if not silent:
        summary['notifications'] = _deliver_notifications()
    # The scheduler's older in-memory callback must not send these again.
    # Events remain in both databases; delivery is owned by the durable queue.
    summary['events_queued_count'] = len(summary.get('events', []))
    summary['events'] = []
    summary['notifications_handled'] = True
    if result.status == 'partial':
        logging.warning('Captura parcial: %s fuentes correctas, %s parciales, %s con error y %s pendientes de acceso. Consultar Fuentes y cobertura.',
                        result.counts.get('success',0), result.counts.get('partial',0),
                        result.counts.get('error',0), result.counts.get('access_required',0))
    print("OTRAS_FUENTES_SUMMARY_JSON=" + json.dumps(summary, ensure_ascii=False, default=str))
    return 1 if result.status == "error" or (not args.local_only and not result.postgres_synced) else 0


def _deliver_notifications():
    from otras_fuentes.notifications import deliver_for_orchestrator
    try:
        result = deliver_for_orchestrator()
        logging.info('Correo externo (SMTP, no confirmación de lectura): %s', result)
        return result
    except Exception as exc:
        # Capture may continue; events/receipts remain on disk for retry.
        logging.error('Correo externo pendiente; %s', type(exc).__name__)
        return {'status': 'error', 'error': type(exc).__name__}


if __name__ == "__main__":
    raise SystemExit(main())
