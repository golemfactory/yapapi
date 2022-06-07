import statemachine  # type: ignore

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .service import ServiceInstance


class ServiceState(statemachine.StateMachine):
    """State machine describing the state and lifecycle of a :class:`Service` instance."""

    # states
    pending = statemachine.State("pending", initial=True)
    """The service instance has not yet been assigned to a provider."""

    starting = statemachine.State("starting")
    """The service instance is starting on a provider.

    The activity within which the service is running has been created on a provider
    node and now  the service instance's :meth:`~yapapi.services.Service.start`
    handler is active and has not yet finished.
    """

    running = statemachine.State("running")
    """The service instance is running on a provider.

    The instance's :meth:`~yapapi.services.Service.start` handler has finished and
    the :meth:`~yapapi.services.Service.run` handler is active.
    """

    stopping = statemachine.State("stopping")
    """The service instance is stopping on a provider.

    The instance's :meth:`~yapapi.services.Service.run` handler has finished and
    the :meth:`~yapapi.services.Service.shutdown` handler is active.
    """

    terminated = statemachine.State("terminated")
    """The service instance has been terminated and is no longer bound to an activity.

    This means that either the service has been explicitly stopped by the requestor,
    or the activity that the service had been attached-to has been terminated - e.g.
    by a failure on the provider's end or as a result of termination of the agreement
    between the requestor and the provider.
    """
    unresponsive = statemachine.State("unresponsive")

    # transitions
    start: statemachine.Transition = pending.to(starting)
    ready: statemachine.Transition = starting.to(running)
    stop: statemachine.Transition = running.to(stopping)
    terminate: statemachine.Transition = terminated.from_(starting, running, stopping, terminated)
    mark_unresponsive: statemachine.Transition = unresponsive.from_(
        starting, running, stopping, terminated
    )
    restart: statemachine.Transition = pending.from_(
        pending, starting, running, stopping, terminated, unresponsive
    )

    lifecycle = start | ready | stop | terminate
    """Transition performed when handler for the current state terminates normally.
    That is, not due to an error or `ControlSignal.stop`
    """

    error_or_stop = stop | terminate
    """transition performed on error or `ControlSignal.stop`"""

    AVAILABLE = (starting, running, stopping)
    """A helper set of states in which the service instance is bound to an activity
    and can be interacted with."""

    instance: "ServiceInstance"
    """The ServiceRunner's service instance."""

    def on_enter_state(self, state: statemachine.State):
        """Register `state` in the instance's list of visited states."""
        self.instance.visited_states.append(state)
