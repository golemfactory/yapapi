# Golem Python API

[![Tests - Status](https://img.shields.io/github/workflow/status/golemfactory/yapapi/Continuous%20integration/master?label=tests)](https://github.com/golemfactory/yapapi/actions?query=workflow%3A%22Continuous+integration%22+branch%3Amaster)
[![Docs status](https://readthedocs.org/projects/yapapi/badge/?version=latest)](https://yapapi.readthedocs.io/en/latest/)
![PyPI - Status](https://img.shields.io/pypi/status/yapapi)
[![PyPI version](https://badge.fury.io/py/yapapi.svg)](https://badge.fury.io/py/yapapi)
[![GitHub license](https://img.shields.io/github/license/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/golemfactory/yapapi)](https://github.com/golemfactory/yapapi/issues)

## What's Golem and yapapi?

**[The Golem Network](https://golem.network)** fosters a global group of creators building ambitious software solutions that will shape the technological landscape of future generations by accessing computing resources across the platform. Golem Network is an accessible, reliable, open access and censorship-resistant protocol, democratizing access to digital resources and connecting users through a flexible, open-source platform.

**Yapapi** is the Python high-level API that allows developers to connect to their Golem nodes and manage their distributed, computational loads through Golem Network.

## Golem application development

For a detailed introduction to using Golem and yapapi to run your tasks on Golem and a guide to Golem Network application development in general, [please consult our handbook](https://handbook.golem.network/requestor-tutorials/flash-tutorial-of-requestor-development).

### Installation

`yapapi` is available as a [PyPI package](https://pypi.org/project/yapapi/).

You can install it through `pip`:

```bash
pip install yapapi
```

Or if your project uses [`poetry`](https://python-poetry.org/) you can add it to your dependencies like this:

```bash
poetry add yapapi
```

### API Reference

For a comprehensive API reference, please refer to [our official readthedocs page](https://yapapi.readthedocs.io/).

## Local setup for yapapi developers

### Poetry

`yapapi` uses [`poetry`](https://python-poetry.org/) to manage its dependencies and provide a runner for common tasks.

If you don't have `poetry` available on your system then follow its [installation instructions](https://python-poetry.org/docs/#installation) before proceeding.
Verify your installation by running:

```bash
poetry --version
```

### Project dependencies

To install the project's dependencies run:

```bash
poetry install
```

By default, `poetry` looks for the required Python version on your `PATH` and creates a virtual environment for the project if there's none active (or already configured by Poetry).

All of the project's dependencies will be installed to that virtual environment.

### Running unit tests

`yapapi` uses [Poe the Poet](https://github.com/nat-n/poethepoet) for running tasks.
Declarations of project tasks can be found in `pyproject.toml`.

```bash
poetry run poe tests_unit
```

### Running `goth` integration tests

#### Prerequisites

If you'd like to run the `yapapi` integration test suite locally then you'll need to install an additional set of dependencies separately.

First, install [the dependencies required to run goth](https://github.com/golemfactory/goth#requirements).

Next, [configure goth's GitHub API token](https://github.com/golemfactory/goth#getting-a-github-api-token).

Make sure you have OpenSSH installed and added to path

```bash
ssh -V
```

Now, you can install goth and its additional python requirements:

```bash
poetry install -E integration-tests
```

Finally, generate goth's default assets:

```bash
poetry run poe tests_integration_assets
```

#### Running the tests

Once you have the environment set up, to run all the integration tests, use:

```bash
poetry run poe tests_integration
```

### Contributing

It is recommended to run unit tests and static code analysis before committing changes.

```bash
poetry run poe checks
```

You can clean up the artifacts created during the test runs with:

```bash
poetry run poe clean
```

## See also

* [Golem](https://golem.network), a global, open-source, decentralized supercomputer that anyone can access.
* Learn what you need to know to set-up your Golem requestor node:
  * [Requestor development: a quick primer](https://handbook.golem.network/requestor-tutorials/flash-tutorial-of-requestor-development)
  * [Run first task on Golem](https://handbook.golem.network/requestor-tutorials/flash-tutorial-of-requestor-development/run-first-task-on-golem)
* Have a look at the most important concepts behind any Golem application: [Golem application fundamentals](https://handbook.golem.network/requestor-tutorials/golem-application-fundamentals)
* Learn about preparing your own Docker-like images for the [VM runtime](https://handbook.golem.network/requestor-tutorials/vm-runtime)
* Write your own app with yapapi:
  * [Task Model development](https://handbook.golem.network/requestor-tutorials/task-processing-development)
  * [Service Model development](https://handbook.golem.network/requestor-tutorials/service-development)

## Environment variables

It's possible to set various elements of `yagna` configuration through environment variables.
`yapapi` currently supports the following environment variables:

* `YAGNA_ACTIVITY_URL`, URL to `yagna` activity API, e.g. `http://localhost:7500/activity-api/v1`
* `YAGNA_API_URL`, base URL to `yagna` REST API, e.g. `http://localhost:7500`
* `YAGNA_APPKEY`, `yagna` app key to be used, e.g. `a70facb9501d4528a77f25574ab0f12b`
* `YAGNA_MARKET_URL`, URL to `yagna` market API, e.g. `http://localhost:7500/market-api/v1`
* `YAGNA_PAYMENT_NETWORK`, Ethereum network name for `yagna` to use, e.g. `rinkeby`
* `YAGNA_PAYMENT_DRIVER`, payment driver name for `yagna` to use, e.g. `erc20`
* `YAGNA_PAYMENT_URL`, URL to `yagna` payment API, e.g. `http://localhost:7500/payment-api/v1`
* `YAGNA_NET_URL`, URL to `yagna` net APU, e.g. `http://localhost:7500/net-api/v1`
* `YAGNA_SUBNET`, name of the `yagna` sub network to be used, e.g. `public`
* `YAPAPI_USE_GFTP_CLOSE`, if set to a _truthy_ value (e.g. "1", "Y", "True", "on") then `yapapi`
  will ask `gftp` to close files when there's no need to publish them any longer. This may greatly
  reduce the number of files kept open while `yapapi` is running but requires `yagna`
  0.7.3 or newer, with older versions it will cause errors.
