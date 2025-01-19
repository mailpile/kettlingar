<div align="center">
  <h1>kettlingar</h1>
  <p>RPC Kittens! Cute asyncio HTTP-RPC microservices for fun and profit.</p>
</div>

<p align="center">
  <a href="https://github.com/mailpile/kettlingar/actions/workflows/main.yml">
    <img src="https://github.com/mailpile/kettlingar/actions/workflows/main.yml/badge.svg" alt="Tests">
  </a>
  <a href="https://codecov.io/gh/mailpile/kettlingar">
    <img src="https://codecov.io/gh/mailpile/kettlingar/branch/main/graph/badge.svg" alt="codecov">
  </a>
  <a href="https://github.com/psf/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="black">
  </a>
  <a href="https://github.com/mailpile/kettlingar/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/mailpile/kettlingar.svg" alt="license">
  </a>
</p>

<p align="center">
  <a href="https://github.com/codespaces/badge.svg)](https://codespaces.new/mailpile/kettlingar?template=false">
    <img src="https://github.com/codespaces/badge.svg" alt="Open in GitHub Codespaces">
  </a>
</p>

# Features

Kettingar is a micro-framework for building Python microservices that
expose an HTTP/1.1 interface. The motivation was to make it easy to
split a complex application into multiple cooperating processes.

- Fully async
- Authenticated and unauthenticated API methods
- Supports JSON or msgpack for RPC request/response
- Serve over TCP/IP and over a local unix domain socket
  - Supports passing open file descriptors over the socket
- Built in CLI for starting/stopping/interacting with the RPC server

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

This is a kettlingar microservice named MyKitten:

```python
from kettlingar import RPCKitten

class MyKitten(RPCKitten):
    class Configuration(RPCKitten.Configuration):
        pass

    async def pub_meow(self, method, headers, body):
        """
        This endpoint requires no authentication!
        """
        return 'text/plain', 'Meow world, meow!'

    async def api_purr(self, method, headers, body, count):
        """
        Authenticated endpoint taking a single argument. The response
        will be encoded as JSON or using msgpack, depending on what the
        caller requested.
        """
        for i in range(0, int(count)):
            yield None, {
                'purr': 'purr' * i,
                '_format': 'Kitty says %(purr)s'}
            asyncio.sleep(1)

    async def call_meow(self):
        """
        Convenience function for invoking the 'meow' method.
        """
        return await self.call('meow')


if __name__ == '__main__':
    import sys
    MyKitten.Main(sys.argv[1:])
```

This is an app that uses the microservice:

```python
from mykitten import MyKitten

w = MyKitten()

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

Kettlingar means "kittens" in Icelandic. This is a spin-off project from
[moggie](https://github.com/mailpile/moggie/) and the author is Icelandic.


# License and Credits

[MIT](https://choosealicense.com/licenses/mit/) - Feel free to use this template for your projects!

Created by Bjarni R. Einarsson for use with moggie and other things besides.

Thanks to ... for the handy Python project template!
