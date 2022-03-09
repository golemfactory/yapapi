import functools
import pytest
import re
from unittest import mock

from yapapi.engine import _Engine
from yapapi.rest.payment import DebitNote
from yapapi.strategy import PROP_DEBIT_NOTE_INTERVAL_SEC, PROP_PAYMENT_TIMEOUT_SEC

from tests.factories.rest.market import AgreementFactory
from tests.factories.rest.payment import DebitNoteFactory


def mock_engine(debit_note: DebitNote, num_debit_notes=0, num_payable_debit_notes=0) -> _Engine:
    with mock.patch("yapapi.engine.rest.Configuration"):
        engine = _Engine(budget=0.0, strategy=mock.Mock(), event_consumer=mock.Mock())

    engine._num_debit_notes[debit_note.activity_id] = num_debit_notes + 1  # noqa
    engine._num_payable_debit_notes[debit_note.activity_id] = num_payable_debit_notes  # noqa

    if debit_note.payment_due_date:
        engine._num_payable_debit_notes[debit_note.activity_id] += 1  # noqa

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
def test_debit_notes(
    agreement_kwargs,
    debit_note_kwargs,
    duration,
    num_debit_notes,
    num_payable_debit_notes,
    vdni_condition,
    vpt_condition,
):
    agreement = AgreementFactory(**agreement_kwargs)
    debit_note = DebitNoteFactory(**debit_note_kwargs)
    engine = mock_engine(debit_note, num_debit_notes, num_payable_debit_notes)

    assert vdni_condition(engine._verify_debit_note_interval(agreement, debit_note, duration))
    assert vpt_condition(engine._verify_payment_timeout(agreement, debit_note, duration))
