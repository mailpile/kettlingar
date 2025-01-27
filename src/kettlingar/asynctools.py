import asyncio
import logging
import socket
import traceback


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
