import statemachine  # type: ignore

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .service import ServiceInstance


class ServiceState(statemachine.StateMachine):
    """State machine describing the state and lifecycle of a :class:`Service` instance."""

    # states
    pending = statemachine.State("pending", initial=True)
    starting = statemachine.State("starting")
    running = statemachine.State("running")
    stopping = statemachine.State("stopping")
    terminated = statemachine.State("terminated")
    unresponsive = statemachine.State("unresponsive")

    # transitions
    start = pending.to(starting)
    ready = starting.to(running)
    stop = running.to(stopping)
    terminate = terminated.from_(starting, running, stopping, terminated)
    mark_unresponsive = unresponsive.from_(starting, running, stopping, terminated)
    restart = pending.from_(pending, starting, running, stopping, terminated, unresponsive)

    # transition performed when handler for the current state terminates normally,
    # that is, not due to an error or `ControlSignal.stop`
    lifecycle = start | ready | stop | terminate

    # transition performed on error or `ControlSignal.stop`
    error_or_stop = stop | terminate

    # just a helper set of states in which the service can be interacted-with
    AVAILABLE = (starting, running, stopping)

    instance: "ServiceInstance"

    def on_enter_state(self, state: statemachine.State):
        """Register `state` in the instance's list of visited states."""
        self.instance.visited_states.append(state)
