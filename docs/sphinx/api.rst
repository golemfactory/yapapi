
**************************
Golem Python API Reference
**************************


Golem
=====

.. autoclass:: yapapi.Golem
    :members: __init__, start, stop, execute_tasks, run_service, create_network, add_event_consumer


Task API
========

Task
----

.. autoclass:: yapapi.Task
    :members: __init__, running_time, accept_result, reject_result

Service API
===========

Service
-------

.. autoclass:: yapapi.services.Service
    :members: id, provider_name, state, is_available, start, run, shutdown, reset, send_message, send_message_nowait, receive_message, receive_message_nowait, get_payload, network, network_node

Cluster
-------

.. autoclass:: yapapi.services.Cluster
    :members:

ServiceState
------------

.. autoclass:: yapapi.services.ServiceState
    :members: pending, starting, running, stopping, terminated, AVAILABLE

Network API
===========

Network
-------

.. autoclass:: yapapi.network.Network
    :members: create, owner_ip, network_address, netmask, gateway, nodes_dict, network_id, add_owner_address, add_node,

Node
----

.. autoclass:: yapapi.network.Node
    :members: network, node_id, ip, get_deploy_args

Exceptions
----------

.. autoclass:: yapapi.network.NetworkError

Payload definition
==================

Payload
-------

.. autoclass:: yapapi.payload.Payload

Package
-------

.. autoclass:: yapapi.payload.package.Package


vm.repo
-------

.. automodule:: yapapi.payload.vm
    :members: repo


Execution control
=================

WorkContext
-----------

.. autoclass:: yapapi.WorkContext
    :members: id, provider_name, provider_id, new_script, get_raw_usage, get_usage, get_raw_state, get_cost

Script
------

.. autoclass:: yapapi.script.Script
    :members: __init__, id, add, deploy, start, terminate, run, download_bytes, download_file, download_json, upload_bytes, upload_file, upload_json


Market strategies
==========================

.. autoclass:: yapapi.strategy.MarketStrategy
    :members: decorate_demand, score_offer, respond_to_provider_offer, acceptable_prop_value_range_overrides, acceptable_prop_value_ranges

.. autoclass:: yapapi.strategy.WrappingMarketStrategy
    :members: __init__, base_strategy

.. autoclass:: yapapi.strategy.LeastExpensiveLinearPayuMS
    :members: score_offer

.. autoclass:: yapapi.strategy.DecreaseScoreForUnconfirmedAgreement
    :members: on_event

.. autoclass:: yapapi.strategy.PropValueRange
    :members: __init__, min, max, __contains__, clamp

.. autoclass:: yapapi.strategy.DummyMS

Events
==========

.. automodule:: yapapi.events
    :members:

Exceptions
==========

.. autoexception:: yapapi.NoPaymentAccountError

.. autoexception:: yapapi.rest.activity.BatchTimeoutError

Logging
=======

.. automodule:: yapapi.log
    :members: enable_default_logger, log_summary, SummaryLogger


Utils
=====

.. autofunction:: yapapi.windows_event_loop_fix

.. autofunction:: yapapi.get_version

Yapapi Contrib
==============

.. automodule:: yapapi.contrib

.. automodule:: yapapi.contrib.strategy.provider_filter

.. autoclass:: yapapi.contrib.strategy.ProviderFilter

.. automodule:: yapapi.contrib.service.http_proxy

.. autoclass:: yapapi.contrib.service.http_proxy.LocalHttpProxy
    :members: __init__, run, stop

.. autoclass:: yapapi.contrib.service.http_proxy.HttpProxyService
    :members: __init__, handle_request
