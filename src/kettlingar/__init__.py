"""
asyncio HTTP-RPC microservices

Kettingar is a micro-framework for building Python microservices that
expose an HTTP/1.1 interface. The motivation was to solve the folowing
two use cases:

  * split a complex application into multiple cooperating processes
  * control, inspect and manage simple Python background services

"""
from .rpckitten import HttpResult, RequestInfo, RPCKitten
from .__version__ import __version__

__all__ = ['HttpResult', 'RequestInfo', 'RPCKitten']
