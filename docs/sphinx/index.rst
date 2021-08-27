Yapapi - Golem Python API
==================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Base usage
=================================

This section covers entities that will be found in nearly any `yapapi`-based piece of code.

.. autoclass:: yapapi.Golem
    :members: __init__, execute_tasks, run_service

.. autoclass:: yapapi.WorkContext
    :members: id, provider_name, commit, deploy, start, terminate, run, send_file, send_bytes, send_json, download_file, download_bytes, download_json

.. autoclass:: yapapi.Task
    :members: __init__, running_time, accept_result, reject_result

.. autoclass:: yapapi.services.Service
    :members: id, provider_name, state, is_available, start, run, shutdown, send_message, send_message_nowait, receive_message, receive_message_nowait, get_payload

