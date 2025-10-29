import asyncio
import logging
import socket
import time
import traceback


class SyncProxy:
    def __init__(self, async_object):
        import queue, threading
        self._obj = async_object
        self._wrapped = {}
        self._jobs = queue.Queue()
        self._loop = threading.Thread(target=self._worker_thread)
        self._loop.daemon = True
        self._loop.start()
        self._keep_running = True

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
        elif result is self._obj:
            return self
        return result

    def _wrap(self, async_method):
        import inspect, queue

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
            return super().__getattr__(key)
        v = self._wrapped.get(key)
        if v is None:
            v = self._wrapped[key] = self._wrap(getattr(self._obj, key))
        return v


class FileWriter:
    def __init__(self, fd):
        self.fd = fd

    def write(self, data):
        return self.fd.write(data)

    async def drain(self):
        pass

    def close(self):
        self.fd.close()


def log_exceptions(func):
    def wrap_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            logging.exception('Exception in wrapped func: %s' % func)
            raise
    return wrap_func


def create_background_task(task):
    async def wrap_task():
        try:
            await task
        except asyncio.exceptions.CancelledError:
            pass
        except:
            traceback.print_exc()
    return asyncio.get_running_loop().create_task(wrap_task())


async def copy_from_queue(q, w, logger):
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


async def copy_loop(r, w, nbytes, logger):
    try:
        while True:
            data = await r.read(nbytes)
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
