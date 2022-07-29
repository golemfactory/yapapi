
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

Low-level API
=============

Low-level objects correspond to resources in the Golem Network.
They make no assumptions about any higher-level components that interact with them.
Capabilities of the low-level API should match `yagna` capabilities, i.e. anything you can
do by direct `yagna` interactions should also be possible - and, hopefully, more convinient - 
by performing operations on the low-level objects.

Resource
--------

.. autoclass:: yapapi.mid.resource.Resource
    :members: id, node,
              get_data, data,
              parent, children, child_aiter, 
              events,

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


Mid-level API
=============

Mid-level components are reusable components 


Events
======

.. autoclass:: yapapi.mid.event_bus.EventBus
    :members: listen, resource_listen, emit

.. automodule:: yapapi.mid.events
    :members:
