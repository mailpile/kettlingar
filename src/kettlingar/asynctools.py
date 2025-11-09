"""
Helper functions and classs for working with async code.
"""
import asyncio
import logging
import socket
import time
import traceback


# Pylint doesn't like our lazy-loading, generic exception handling and
# overall low-level meddling.  But these are core features of what we
# are doing here, so...
#
# pylint: disable=import-outside-toplevel
# pylint: disable=broad-exception-caught
# pylint: disable=bare-except
# pylint: disable=too-many-arguments
# pylint: disable=protected-access


class SyncProxy:
    """
    SyncProxy provides a synchronous interface to an object with async
    methods.

    This is done by running an asyncio event loop in a dedicated thread,
    and posting requests and receiving replies using a queue. So there is
    some overhead, but it works quite well.

    Exceptions thrown on the async side are caught and passed over the
    queue, and re-raised there.
    """
    def __init__(self, async_object):
        import threading
        import queue

        self._obj = async_object
        self._wrapped = {}
        self._keep_running = True
        self._jobs = queue.Queue()
        self._loop = threading.Thread(target=self._worker_thread)
        self._loop.daemon = True
        self._loop.start()

    _EOF_RESULT_MARKER = '<EOF-RESULT-MARKER>'
    _EXCEPTION_MARKER = '<EXCEPTION-MARKER>'

    async def _worker_thread_main(self):
        while self._keep_running:
            is_generator, fn, args, kwargs, rq = self._jobs.get()
            try:
                if is_generator:
                    async for result in fn(*args, **kwargs):
                        rq.put(result)
                    rq.put(self._EOF_RESULT_MARKER)
                else:
                    rq.put(await fn(*args, **kwargs))
            except Exception as e:
                rq.put((self._EXCEPTION_MARKER, e))

    def _worker_thread(self):
        asyncio.run(self._worker_thread_main())

    def _check_return_value(self, result):
        if (isinstance(result, tuple) and len(result)
                and result[0] is self._EXCEPTION_MARKER):
            raise result[1]
        if result is self._obj:
            return self
        return result

    def _wrap(self, async_method):
        import inspect
        import queue

        if inspect.isasyncgenfunction(async_method):
            def wrapper(*args, **kwargs):
                rq = queue.Queue()
                self._jobs.put((True, async_method, args, kwargs, rq))
                while True:
                    result = rq.get()
                    if result is self._EOF_RESULT_MARKER:
                        return
                    yield self._check_return_value(result)

        elif inspect.iscoroutinefunction(async_method):
            def wrapper(*args, **kwargs):
                rq = queue.Queue()
                self._jobs.put((False, async_method, args, kwargs, rq))
                return self._check_return_value(rq.get())

        else:
            wrapper = async_method

        return wrapper

    def __getattr__(self, key):
        if key.startswith('_'):
            return super().__getattribute__(key)
        v = self._wrapped.get(key)
        if v is None:
            v = self._wrapped[key] = self._wrap(getattr(self._obj, key))
        return v


class FileWriter:
    """
    Emulate the interface of an asyncio writer, but for a file descriptor.
    """
    def __init__(self, fd):
        self.fd = fd

    def write(self, data):
        """Write data to the file descriptor."""
        return self.fd.write(data)

    async def drain(self):
        """A no-op, for compatibility with asyncio Writer objects."""

    def close(self):
        """Close the file descriptor."""
        self.fd.close()


def log_exceptions(func):
    """
    Decorator/function wrapper to log thrown exceptions and re-raise.
    """
    def wrap_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            logging.exception('Exception in wrapped func: %s', func)
            raise
    return wrap_func


def create_background_task(task):
    """
    Create and schedule an async background task.
    """
    async def wrap_task():
        try:
            await task
        except asyncio.exceptions.CancelledError:
            pass
        except:
            # FIXME: This should probably be a logger?
            traceback.print_exc()
    return asyncio.get_running_loop().create_task(wrap_task())


async def copy_from_queue(q, w, logger):
    """
    A generic asyncio task for copying data from a queue directly
    into a writer. A null/false object in the queue will indicate
    end-of-file and trigger shutdown of the socket being written to.
    """
    try:
        while True:
            data = await q.get()
            if not data:
                break
            w.write(data)
            await w.drain()
    except asyncio.exceptions.CancelledError:
        pass
    except:
        logger.exception('Copy-from-queue loop crashed')
    finally:
        try:
            w._transport._sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass


async def copy_loop(r, w, chunk_size, logger):
    """
    A generic asyncio task for copying data from a reader directly
    into a writer.
    """
    try:
        while True:
            data = await r.read(chunk_size)
            if not data:
                return
            w.write(data)
            await w.drain()
    except asyncio.exceptions.CancelledError:
        pass
    except:
        logger.exception('Copy loop crashed')
    finally:
        try:
            w._transport._sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass


async def gather_and_close(done_cb, done_args, tasks, *conns):
    """
    A generic asyncio task for waiting until a bunch of tasks have
    completed, and then shutting down/closing a bunch of connections
    or file descriptors.

    The `done_cb` function will be called upon completion with the
    given arguments. If an exception was raised, the exception object
    will be passed as well like so: `done_cb(*done_args, exception=e)`.
    """
    try:
        await asyncio.gather(*tasks)
        for conn in conns:
            if hasattr(conn, 'close'):
                conn.close()
            elif hasattr(conn, '_transport'):
                conn._transport._sock.close()
            if hasattr(conn, 'wait_closed'):
                await conn.wait_closed()
        done_cb(*done_args)
    except asyncio.exceptions.CancelledError:
        done_cb(*done_args)
    except Exception as e:
        done_cb(*done_args, exception=e)


async def await_success(func, *args,
        timeout=120,
        sleeptime=0.01,
        sleepstep=0.01,
        sleepmax=0.5,
        retried_exceptions=(IOError,)):
    """
    Repeatedly attempt an operation until it succeeds.
    """
    deadline = (time.time() + timeout) if timeout else None
    while (not deadline) or (time.time() < deadline):
        await asyncio.sleep(sleeptime)
        sleeptime = min(sleepmax, sleeptime + sleepstep)
        try:
            return func(*args)
        except retried_exceptions:
            pass

    raise IOError('Timed out: await_success(%s, %s)' % (func, args))
