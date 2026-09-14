"""Bounded Google retries. Call factories must obtain the current thread's client."""
import http.client
import logging
import random
import socket
import ssl
import time

import httplib2
from google.auth.exceptions import TransportError
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

NETWORK_ERRORS = (
    TimeoutError, socket.timeout, socket.gaierror, socket.herror,
    ConnectionError, ssl.SSLError, TransportError, httplib2.HttpLib2Error,
    http.client.RemoteDisconnected, http.client.IncompleteRead,
    http.client.BadStatusLine, http.client.CannotSendRequest,
    http.client.ResponseNotReady,
)
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}
HTTP_TIMEOUT_SECONDS = 15


def is_transient_google_error(error):
    if isinstance(error, HttpError):
        return getattr(error.resp, 'status', None) in RETRYABLE_STATUS
    return isinstance(error, NETWORK_ERRORS)


def build_sheets_service(credentials):
    # The service and transport must remain local to the calling thread.
    transport = AuthorizedHttp(credentials, http=httplib2.Http(timeout=HTTP_TIMEOUT_SECONDS))
    return build('sheets', 'v4', http=transport, cache_discovery=False)


def close_service(service):
    close = getattr(service, 'close', None)
    if callable(close):
        try:
            close()
        except Exception:
            logging.debug('No se pudo cerrar una conexion Google anterior', exc_info=True)


def retry_google_call(action, *, reset, label, max_attempts=3, base_delay=1.5):
    if max_attempts < 1:
        raise ValueError('Se requiere al menos un intento')
    for attempt in range(max_attempts):
        try:
            return action()
        except Exception as error:
            if not is_transient_google_error(error):
                raise
            reset()
            if attempt + 1 == max_attempts:
                raise
            delay = base_delay * 2**attempt + random.uniform(0, .5)
            logging.warning('Google Sheets %s: %s; reconexion %s/%s en %.1fs',
                            label, type(error).__name__, attempt + 1, max_attempts, delay)
            time.sleep(delay)


def google_failure_detail(error):
    if is_transient_google_error(error):
        return f'Google no disponible por red/DNS o servicio temporal ({type(error).__name__}); se reintentara'
    return f'{type(error).__name__}: {error}'


def log_google_failure(label, error):
    if is_transient_google_error(error):
        logging.warning('%s: %s', label, google_failure_detail(error))
    else:
        logging.exception('%s', label)
