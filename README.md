<div align="center">
  <h1>kettlingar</h1>
  <p>RPC Kittens! Cute asyncio HTTP-RPC microservices for fun and profit.</p>
</div>

# What's this?

Kettingar is a micro-framework for building Python microservices that
expose an HTTP/1.1 interface. The motivation was to solve the folowing
two use cases:

- Split a complex application into multiple cooperating processes
- Control, inspect and manage simple Python background services

Some features:

- Fully async
- Authenticated and unauthenticated API methods
- Allows generator functions to incrementally provide results
- Supports passing open file descriptors to or from the microservice
- Supports msgpack (preferred) or JSON for RPC request/response
- Serve over TCP/IP and over a local unix domain socket
- Built in CLI for configuring, running and interacting with the service

See below for a bit more discussion about these features.


# Status / TODO:

Status: Useful!

TODO:

- Extend `help` to list defined API methods
- Write automated tests
- Use type hinting for fun and profit
- Improve and document how we do logging
- Improve and document varz and internal stats
- Improve and document how to extend JSON/msgpack for custom data types
- Document the `public_raw_` and `raw_` magic prefixes
- Document error handling / exception propogation

If you think you can help with any of those, feel free to ping me!


# Installation

## From Source

```bash
git clone https://github.com/mailpile/kettlingar.git
cd kettlingar
pip install .
```

## Development Installation

```bash
git clone https://github.com/mailpile/kettlingar.git
cd kettlingar
pip install -e ".[dev]"
```

## Usage

See the [`examples/`](./examples/)
for fully documented versions of the snippets below,
as well as other helpful examples.

This is a kettlingar microservice named MyKitten:

```python
import asyncio

from kettlingar import RPCKitten


class MyKitten(RPCKitten):
    class Configuration(RPCKitten.Configuration):
        APP_NAME = 'mykitten'
        WORKER_NAME = 'Kitty'

    async def public_api_meow(self, method, headers, body):
        return (
            'text/plain',           # Fixed MIME type of `text/plain`
            'Meow world, meow!\n')  # Meow!

    async def api_purr(self, method, headers, body, count=1, purr='purr'):
        _format = self.config.worker_name + ' says %(purr)s'

        count = int(count)              # CLI users will send us strings

        for i in range(count):
            result = {
                'purr': purr * (i + 1), # Purring!
                '_format': _format}     # Formatting rule for CLI interface

            yield (
                None,                   # No MIME-type, let framework choose
                result)                 # Result object
            await asyncio.sleep(1)


if __name__ == '__main__':
    import sys
    MyKitten.Main(sys.argv[1:])
```

This (or something very similar) can be found in the
[examples](examples/) folder, and run like so:

```bash
$ python3 -m examples.kitten help
...

$ python3 -m examples.kitten start --worker-listen-port=12345
...

$ python3 -m examples.kitten ping
Pong via /path/to/mykitten/worker.sock!

$ python3 -m examples.kitten meow
{'mimetype': 'text/plain', 'data': bytearray(b'Meow world, meow!\n')}

$ curl http://127.0.0.1:12345/meow
Meow world, meow
```

This is an app that uses the microservice:

```python
import asyncio
import sys

from .kitten import MyKitten


async def test_function():
    kitty = await MyKitten(args=sys.argv[1:]).connect(auto_start=True)

    print('%s' % kitty)
    print('Our first meow: %s' % (await kitty.meow()))

    async for result in kitty.purr(10):
        print(result['purr'])

    await kitty.quitquitquit()

asyncio.run(test_function())
```

# Writing API endpoints

As illustrated above,
an API endpoint is simply a Python function named using one of the following prefixes:
`public_api_`, or `api_` -
to understand the difference between them, see **Access controls** below.

These functions must always take at least three positional arguments
(`method`, `headers`, `body`),
and must either return a single tuple of `(mime-type, data)`,
or implement a generator which yields such tuples.

The functions should also have docstrings which explain what they do and how to use them.

The functions can access (or modify!) the microservice configuration,
as `self.config.*` (see below for details).

That's all;
`kettlingar` will automatically:
- define HTTP/1.1 paths within the microservice for each method,
- create Pythonic client functions which send requests to the microservice and deserialize the responses, and
- create a command-line (CLI) interface for asking for `help` or invoking the function directly

Note that `kettlingar` microservices are single-threaded,
but achieve concurrency using `asyncio`.
Any API method could block the entire service,
so write them carefully!


## How does it work?

In the above example,
`kettlingar` uses introspection (the `inspect` module) to auto-generate client functions for each of the two API methods:
`MyKitten.meow()` and
`MyKitten.purr()`.

When invoked,
each client function will use `MyKitten.call()`
and then either return the result directly or implement a generator.

Digging even deeper,
`MyKitten.call()` makes an HTTP/1.1 POST request to the running microservice,
using `msgpack` to bundle up all the arguments and deserialized the response.

(JSON is also supported for interfacing with humans or systems lacking `msgpack`.)

In the case of the generator function,
the HTTP response uses
[chunked encoding](https://en.wikipedia.org/wiki/Chunked_transfer_encoding),
with one chunk per yielded purr.


# Is this a web server?

Kinda.
Kettlingar implements a subset of the HTTP/1.1 specification.

This allows us to use tools like `curl` or even a web browser for debugging and should facilitate use of non-Python clients.
But the emphasis was on simplicity (and hopefully performance),
rather than a complete implementation.


# Access controls

A `kettlingar` microservice offers two levels of access control:

- "Public", or unauthenticated methods
- Private methods

Access to private methods is granted by checking for a special token's presence anywhere in the HTTP header.
The common case is for the token to be included in the path part of the HTTP URL,
but you can use any (standard or made up) HTTP header if you prefer.

In the case where the client is running on the same machine,
and is running using the same user-id as the microservice,
credentials (and the unix domain socket, if it exists)
are automatically found at a well defined location in the user's home directory.
Access to them is restricted using Unix file system permissions.


# Configuration and the command line

The `kettlingar` command-line interface aims to solve these tasks:

- Administering the microservice (start, stop, get help)
- Configuring the microservice (including defining a config file format)
- Running functions on the microservice (testing, or real use from the shell)

## Administration

A `kettlingar` microservice will out-of-the-box support these CLI commands:

- `start` - Start the microservice (if it isn't already running)
- `stop` - Stop the microservice
- `restart` - Stop and Start
- `ping` - Check whether the microservice is running
- `help` - Get help on which commands exist and how to use them
- `config` - Display the configuration of the running microservice

## Configuration

Each `RPCKitten` subclass contains a `Configuration` class which defines a set of capitalized constants in all-caps.
This defines
a) the names of the microservice configuration variables and
b) their default values.

For each `VARIABLE_NAME` defined in the configuration class,
the instanciated configuration objects will have a `variable_name` (lower-case) attribute with the current setting.
This can be accessed from within API methods as `self.config.variable_name`.
The value can be set on the command-line
(when starting the service) by passing an argument `--variable-name=value`.

Examples:

```bash
## Tweak the configuration on the command-line
$ python3 -m examples.kitten start --app-name=Doggos --worker-name=Spot
...

## Load settings from a file
$ python3 -m examples.kitten start --worker-config=examples/kitten.cfg
...

## View the current configuration as JSON
$ python3 -m examples.kitten config --json
...
```

Note that the `app_name` and `worker_name` settings influence the location of the authentication files (see **Access controls** above),
so changing either one will allow you to run multiple instances of the same microservice side-by-side.

## API functions as CLI commands

The arguments and keyword arguments specified on the `api_*` functions translate in the "obvious" way to the command-line:
positional arguments are positional,
keyword arguments can be specified using `--key=val`.

Example:

```bash
$ python3 -m examples.kitten purr 2 --purr=rarr
Kitty says rarr
Kitty says rarrrarr
```

Note that if you expect to use your microservice from the command line,
you will need to handle any type conversions (from string representations) yourself within the function.
A future version of `kettlingar` might be able to infer conversions from Python type hints,
but that hasn't yet been implemented.


# Unix domain sockets and passing file descriptors

A `kettlingar` microservice will by default listen on both a TCP/IP socket,
and a unix domain socket.
Processes located on the same machine should use the unix domain socket by default.

This theoretically has lower overhead (better performance),
but also allows the microservice to send and receive open file descriptors
(see [filecat.py](examples/filecat.py) and
[test_filecat.py](examples/test_filecat.py) for a demo).

This can be used in a few ways:

- One process can listen for incoming TCP/IP client connections,
  and then pass the connection to a worker process to finish the job.
- Seamless upgrades of running servers;
  a graceful shutdown command could send all open sockets/file descriptors to the replacement process before terminating.

It's also just neat and I wanted to play with it!


# Development

1. Clone the repository

```bash
git clone https://github.com/mailpile/kettlingar.git
cd python-project-template
```

2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

3. Install development dependencies

```bash
pip install -e ".[dev]"
```

## Running Tests

We really should have some automated tests, shouldn't we?


# Contributing

Contributions are always welcome! Here's how you can help:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add some amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

Please make sure to update tests as appropriate and follow the existing coding style.


# Kettlingar? Huh?

Kettlingar means "kittens" in Icelandic.
This is a spin-off project from
[moggie](https://github.com/mailpile/moggie/) (a moggie is a cat)
and the author is Icelandic.


# License and Credits

[MIT](https://choosealicense.com/licenses/mit/), have fun!

Created by [Bjarni R. Einarsson](https://github.com/BjarniRunar)
for use with
[moggie](https://github.com/mailpile/moggie/)
and probably some other things besides.

Thanks to Aniket Maurya for the
[handy Python project template](https://github.com/aniketmaurya/python-project-template).
