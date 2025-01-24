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
- Built in CLI for starting/stopping/interacting with the service

See below for a bit more discussion about these features.


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
        for i in range(0, int(count)):
            result = {
                'purr': purr * (i + 1),  # Purring!
                '_format': _format}      # Formatting rule for CLI interface

            yield (
                None,                    # No MIME-type, let framework choose
                result)                  # Result object
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


# Unix domain sockets and passing file descriptors

A `kettlingar` microservice will by default listen on both a TCP/IP socket,
and a unix domain socket.
Processes located on the same machine should the unix domain socket by default.

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

```bash
pytest tests/
```


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
