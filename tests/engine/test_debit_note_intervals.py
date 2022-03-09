from datetime import datetime, timedelta
import functools
import pytest
import re
import sys
from unittest import mock

from yapapi.engine import _Engine
from yapapi.rest.market import Agreement
from yapapi.rest.payment import DebitNote
from yapapi.strategy import PROP_DEBIT_NOTE_INTERVAL_SEC, PROP_PAYMENT_TIMEOUT_SEC

from tests.factories.rest.market import AgreementFactory
from tests.factories.rest.payment import DebitNoteFactory


def mock_engine(
    agreement: Agreement, debit_note: DebitNote, num_debit_notes=0, num_payable_debit_notes=0
) -> _Engine:
    with mock.patch("yapapi.engine.rest.Configuration"):
        engine = _Engine(budget=0.0, strategy=mock.Mock(), event_consumer=mock.Mock())

    engine._all_agreements[agreement.id] = agreement  # noqa
    engine._num_debit_notes[debit_note.activity_id] = num_debit_notes  # noqa
    engine._num_payable_debit_notes[debit_note.activity_id] = num_payable_debit_notes  # noqa

    return engine


def _is_none(reason):
    return reason is None


def _message_matches(regex):
    def matches(reason, _regex):
        assert reason, "non-empty termination reason expected"
        return re.match(_regex, reason.get("message"))

    return functools.partial(matches, _regex=regex)


@pytest.mark.parametrize(
    "agreement_kwargs, debit_note_kwargs, duration, "
    "num_debit_notes, num_payable_debit_notes, "
    "vdni_condition, vpt_condition",
    [
        ({}, {}, 1, 0, 0, _is_none, _is_none),
        (
            {},
            {"_base__payment_due_date": True},
            1,
            0,
            0,
            _is_none,
            _message_matches("^Payable debit note received when mid-agreement payments inactive"),
        ),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            100,
            0,
            0,
            _is_none,
            _is_none,
        ),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            10,
            0,
            0,
            _message_matches("^Too many debit notes"),
            _is_none,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            100,
            0,
            0,
            _is_none,
            _is_none,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            10,
            0,
            0,
            _is_none,
            _message_matches("^Too many payable debit notes"),
        ),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            300,
            2,
            0,
            _is_none,
            _is_none,
        ),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            300,
            4,
            0,
            _message_matches("^Too many debit notes"),
            _is_none,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            300,
            0,
            2,
            _is_none,
            _is_none,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            300,
            0,
            4,
            _is_none,
            _message_matches("^Too many payable debit notes"),
        ),
    ],
)
def test_verify_debit_note_intervals(
    agreement_kwargs,
    debit_note_kwargs,
    duration,
    num_debit_notes,
    num_payable_debit_notes,
    vdni_condition,
    vpt_condition,
):
    agreement = AgreementFactory(**agreement_kwargs)
    debit_note = DebitNoteFactory(_base__agreement_id=agreement.id, **debit_note_kwargs)
    engine = mock_engine(agreement, debit_note, num_debit_notes, num_payable_debit_notes)

    engine._num_debit_notes[debit_note.activity_id] += 1  # noqa
    if debit_note.payment_due_date:
        engine._num_payable_debit_notes[debit_note.activity_id] += 1  # noqa

    assert vdni_condition(engine._verify_debit_note_interval(agreement, debit_note, duration))
    assert vpt_condition(engine._verify_payment_timeout(agreement, debit_note, duration))


@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "agreement_kwargs, debit_note_kwargs, created_at_cb, "
    "num_debit_notes, num_payable_debit_notes, "
    "debit_note_accepted, agreement_terminated",
    [
        ({}, {}, datetime.now, 0, 0, True, False),
        ({"terminated": True}, {}, datetime.now, 0, 0, False, False),
        ({}, {}, lambda: None, 0, 0, False, False),
        ({}, {"_base__payment_due_date": True}, datetime.now, 0, 0, False, True),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            datetime.now,
            0,
            0,
            False,
            True,
        ),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            lambda: datetime.now() - timedelta(seconds=100),
            0,
            0,
            True,
            False,
        ),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            lambda: datetime.now() - timedelta(seconds=300),
            2,
            0,
            True,
            False,
        ),
        (
            {"details___ref__demand__properties": {PROP_DEBIT_NOTE_INTERVAL_SEC: 100}},
            {},
            lambda: datetime.now() - timedelta(seconds=300),
            3,
            0,
            False,
            True,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            datetime.now,
            0,
            0,
            False,
            True,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            lambda: datetime.now() - timedelta(seconds=100),
            0,
            0,
            True,
            False,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            lambda: datetime.now() - timedelta(seconds=300),
            0,
            2,
            True,
            False,
        ),
        (
            {"details___ref__demand__properties": {PROP_PAYMENT_TIMEOUT_SEC: 100}},
            {"_base__payment_due_date": True},
            lambda: datetime.now() - timedelta(seconds=300),
            0,
            3,
            False,
            True,
        ),
    ],
)
async def test_enforce_debit_note_intervals(
    agreement_kwargs,
    debit_note_kwargs,
    created_at_cb,
    num_debit_notes,
    num_payable_debit_notes,
    debit_note_accepted,
    agreement_terminated,
):
    job = mock.AsyncMock()
    agreement = AgreementFactory(**agreement_kwargs)
    debit_note = DebitNoteFactory(_base__agreement_id=agreement.id, **debit_note_kwargs)
    engine = mock_engine(agreement, debit_note, num_debit_notes, num_payable_debit_notes)
    engine._activity_created_at[debit_note.activity_id] = created_at_cb()

    assert await engine._enforce_debit_note_intervals(job, debit_note) == debit_note_accepted
    assert job.agreements_pool._terminate_agreement.called == agreement_terminated
