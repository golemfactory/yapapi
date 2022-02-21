# A minimal web app example for yapapi

This example demonstrates using yapapi's Services API to run a web application composed of two services deployed to separate provider hosts and each one using its own vm image.

## Components

### DB backend: rqlite

For the backend, we decided to use **[rqlite](https://github.com/rqlite/rqlite)** - an "easy-to-use, lightweight, distributed relational database, which uses SQLite as its storage engine."

One reason to use `rqlite` is that it's very easy to set-up and another, more important one is that its clustering capabilities make it extremely useful for distributed deployments on Golem.

### HTTP frontend: a "oneliner" status app

For the frontend, we're using a trivial `Flask` app, connected to the DB with `SQLAlchemy`.

The app uses a single HTML template and allows the user to send a single line of text that gets appended to an existing log of messages and at the same time, presents the list of previous messages to the user.

### Local HTTP proxy

Finally, to allow the user to connect to the HTTP front-end running on a provider's end, we're launching a simple HTTP proxy service that listens on a local HTTP port, forwards all requests to the provider's HTTP instance and all responses back to the local port.
