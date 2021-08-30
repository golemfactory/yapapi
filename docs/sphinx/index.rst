*************
API Reference
*************

.. toctree::
    :maxdepth: 4
    :hidden:
    
    Golem Handbook <https://handbook.golem.network/>
    index
    Github <https://github.com/golemfactory/yapapi>
    Pypi <https://pypi.org/project/yapapi/>


Base interface
==============

There are few classes/functions that show up in virtally every Golem-based application.

Common classes/functions
------------------------

Golem
^^^^^

Golem class is the main entrypoint to `yapapi`.
(TODO: move most of the `yapapi.Golem` docstring here).

.. autoclass:: yapapi.Golem
    :members: __init__, execute_tasks, run_service

WorkContext
^^^^^^^^^^^

.. autoclass:: yapapi.WorkContext
    :members: id, provider_name, commit, deploy, start, terminate, run, send_file, send_bytes, send_json, download_file, download_bytes, download_json

vm.repo
^^^^^^^

.. automodule:: yapapi.payload.vm
    :members: repo

Task API
--------

Task
^^^^

.. autoclass:: yapapi.Task
    :members: __init__, running_time, accept_result, reject_result

Service API
-----------

Service
^^^^^^^

.. autoclass:: yapapi.services.Service
    :members: id, provider_name, state, is_available, start, run, shutdown, send_message, send_message_nowait, receive_message, receive_message_nowait, get_payload

Cluster
^^^^^^^

.. autoclass:: yapapi.services.Cluster
    :members:

ServiceState
^^^^^^^^^^^^

.. autoclass:: yapapi.services.ServiceState

Offer selection strategies
==========================

.. automodule:: yapapi.strategy
    :members:

Exceptions
==========

.. autoexception:: yapapi.NoPaymentAccountError

.. autoexception:: yapapi.rest.activity.BatchTimeoutError

Logging
=======

.. automodule:: yapapi.log
    :members: enable_default_logger, log_summary, SummaryLogger


Other
=====

.. autofunction:: yapapi.windows_event_loop_fix

.. autofunction:: yapapi.get_version
