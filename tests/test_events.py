import pytest

from yapapi.events import (
    AgreementCreated,
    CollectFailed,
    CommandExecuted,
    CommandStarted,
    Event,
    ExecutionInterrupted,
    ProposalReceived,
    ScriptSent,
    ServiceFinished,
    SubscriptionFailed,
    TaskStarted,
)


@pytest.mark.parametrize(
    "event, expected_str",
    [
        (
            SubscriptionFailed(job="a-job", reason="the-reason"),
            "SubscriptionFailed(job='a-job', reason='the-reason')",
        ),
        (
            CollectFailed(job="a-job", subscription="a-sub", reason="the-reason"),
            "CollectFailed(job='a-job', subscription='a-sub', reason='the-reason')",
        ),
        (
            ProposalReceived(job="a-job", proposal="a-prop"),
            "ProposalReceived(job='a-job', proposal='a-prop')",
        ),
        (
            AgreementCreated(job="a-job", agreement="an-agr"),
            "AgreementCreated(job='a-job', agreement='an-agr')",
        ),
        (
            TaskStarted(job="a-job", agreement="an-agr", activity="an-act", task="a-task"),
            "TaskStarted(job='a-job', agreement='an-agr', activity='an-act', task='a-task')",
        ),
        (
            ServiceFinished(
                job="a-job", agreement="an-agr", activity="an-act", service="a-service"
            ),
            "ServiceFinished(job='a-job', agreement='an-agr', activity='an-act', service='a-service')",
        ),
        (
            ScriptSent(job="a-job", agreement="an-agr", activity="an-act", script="a-script"),
            "ScriptSent(job='a-job', agreement='an-agr', activity='an-act', script='a-script')",
        ),
        (
            CommandStarted(
                job="a-job",
                agreement="an-agr",
                activity="an-act",
                script="a-script",
                command="the-command",
            ),
            "CommandStarted(job='a-job', agreement='an-agr', activity='an-act', "
            "script='a-script', command='the-command')",
        ),
        (
            CommandExecuted(
                job="a-job",
                agreement="an-agr",
                activity="an-act",
                script="a-script",
                command="the-command",
                success=True,
                message="the-message",
            ),
            "CommandExecuted(job='a-job', agreement='an-agr', activity='an-act', "
            "script='a-script', command='the-command', success=True, message='the-message', "
            "stdout=None, stderr=None)",
        ),
        (
            ExecutionInterrupted(exc_info=(RuntimeError.__class__, RuntimeError("oops"), None)),
            "ExecutionInterrupted(exception=RuntimeError('oops'))",
        ),
    ],
)
def test_event_to_str(event: Event, expected_str: str):
    assert str(event) == expected_str
    assert repr(event) == str(event)
