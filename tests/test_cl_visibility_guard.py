from datetime import datetime

from common.cl_lifecycle import (
    PANAMA_TZ, ProposalObservation, apply_observation, can_retire_active_cl,
    record_from_mapping, should_inspect_cl,
)

URL = 'https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-03-CL-050508/token'


def test_missing_cuadro_on_last_day_does_not_remove_050508():
    record = {'fecha_presentacion_texto': '11-09-2026 a 16-09-2026'}
    for status in ('cerrada_pendiente_publicacion', 'error_verificacion', 'cerrada_pendiente_verificacion'):
        observation = ProposalObservation('2026-1-10-01-03-CL-050508', URL, status, None)
        assert not can_retire_active_cl(record, observation, reference=datetime(2026, 9, 16, 12, 8, tzinfo=PANAMA_TZ))
        assert can_retire_active_cl(record, observation, reference=datetime(2026, 9, 17, 0, 0, tzinfo=PANAMA_TZ))


def test_start_time_does_not_become_unpublished_closing_time():
    record = {'fecha_presentacion_texto': '11-09-2026 08:00 AM a 16-09-2026'}
    observation = ProposalObservation('test', URL, 'cerrada_pendiente_publicacion', None)
    assert not can_retire_active_cl(record, observation, reference=datetime(2026, 9, 16, 12, 8, tzinfo=PANAMA_TZ))


def test_explicit_deadline_and_confirmed_final_result_can_retire():
    observation = ProposalObservation('test', URL, 'error_verificacion', None)
    record = {'fecha_presentacion_texto': '16-09-2026 - 08:00 AM a 11:00 AM'}
    assert can_retire_active_cl(record, observation, reference=datetime(2026, 9, 16, 12, 0, tzinfo=PANAMA_TZ))
    observation.status = 'cerrada_con_propuestas'
    assert can_retire_active_cl({'fecha_presentacion_texto': ''}, observation)


def test_changed_route_for_same_process_is_still_present():
    assert not should_inspect_cl('11-09-2026 a 16-09-2026', URL,
        active_listing_links=[URL.replace('/token', '/other-version')],
        reference=datetime(2026, 9, 16, 12, 0, tzinfo=PANAMA_TZ))


def test_unverified_future_cl_has_no_invented_closed_at():
    record = record_from_mapping({'enlace': URL, 'fecha': '01-01-2099 a 10-01-2099'})
    observation = ProposalObservation(record['numero_cl'], URL, 'cerrada_pendiente_publicacion', None)
    updated, _ = apply_observation(record, observation)
    assert updated['closed_at'] == ''


def test_recovering_verified_open_cl_clears_false_closed_timestamp():
    record = record_from_mapping({'enlace': URL, 'fecha': '11-09-2026 a 16-09-2026'})
    record['closed_at'] = '2026-09-16T12:08:42-05:00'
    observation = ProposalObservation(record['numero_cl'], URL, 'abierta', None, evidence_type='api_estado_abierta')
    updated, _ = apply_observation(record, observation)
    assert updated['estado_derivado'] == 'abierta'
    assert updated['closed_at'] == ''
