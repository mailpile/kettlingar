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

See the [`examples/`](./examples/) folder for helpful code snippets.

This is a kettlingar microservice named MyKitten:

```python
import asyncio

from kettlingar import RPCKitten


class MyKitten(RPCKitten):
    """mykitten - A sample kettlingar microservice

    This microservice knows how to meow and how to purr. Purring is
    private and requires authentication, and may go on for a while.
    """
    class Configuration(RPCKitten.Configuration):
        APP_NAME = 'mykitten'
        WORKER_NAME = 'Kitty'

    async def public_api_meow(self, method, headers, body):
        """
        This endpoint requires no authentication!
        """
        return 'text/plain', 'Meow world, meow!\n'

    async def api_purr(self, method, headers, body, count):
        """
        Authenticated endpoint taking a single argument. The response
        will be encoded as JSON or using msgpack, depending on what the
        caller requested.

        The generated convenience function, MyKitten.purr() will be an
        async generator yielding results as they are sent over the wire.
        """
        for i in range(0, int(count)):
            yield None, {
                'purr': 'purr' * (i + 1),
                '_format': '%s says %%(purr)s' % self.config.worker_name}
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
