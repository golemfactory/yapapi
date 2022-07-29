
**************************
Golem Python API Reference
**************************


GolemNode
=========

.. autoclass:: yapapi.mid.golem_node.GolemNode
    :members: __init__, __aenter__, __aexit__, 
              create_allocation, create_demand, 
              allocation, demand, proposal, agreement, 
              allocations, demands,
              event_bus

Low-level objects
=================

Market API
----------

.. autoclass:: yapapi.mid.market.Demand
    :members: initial_proposals, start_collecting_events, stop_collecting_events, unsubscribe, proposal

.. autoclass:: yapapi.mid.market.Proposal
    :members: initial, draft, rejected, demand,
              respond, responses, reject, create_agreement

.. autoclass:: yapapi.mid.market.Agreement
    :members: confirm, wait_for_approval, terminate

Payment API
-----------

.. autoclass:: yapapi.mid.payment.Allocation
    :members: release

Events
======

.. autoclass:: yapapi.mid.event_bus.EventBus
    :members: listen, resource_listen, emit

.. automodule:: yapapi.mid.events
    :members:
