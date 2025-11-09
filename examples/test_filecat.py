"""
Demonstrate how to use FileCat, sending and receiving file descriptors. 
"""
import asyncio
import tempfile
import sys

from .filecat import FileCat


# pylint: disable=broad-exception-caught


async def test_function():
    """
    Test all the things!
    """
    # pylint: disable=no-member

    kitty = await FileCat(args=sys.argv[1:]).connect(auto_start=True)
    print('%s' % kitty)

    print('Reading a remotely opened file descriptor...')
    try:
        for fd in await kitty.read(__file__):
            contents = fd.read()
            print(str(contents, 'utf-8'))
    except Exception as e:
        print('Failed: %s' % e)

    print('Send a file descriptor over and let kitty read it...')
    try:
        with open(__file__, 'rb') as fd:
            contents = await kitty.cat(fd)
            print(str(contents, 'utf-8'))
    except Exception as e:
        print('Failed: %s' % e)

    print('Ask kitty to respond directly to an open file...')
    try:
        tmpfile = tempfile.NamedTemporaryFile()
        await kitty.help('ping',
            call_reply_to=tmpfile.file,
            call_use_json=True)

        await asyncio.sleep(0.2)
        with open(tmpfile.name, 'rb') as fd:
            print(str(fd.read(), 'utf-8'))
    except Exception as e:
        print('Failed: %s' % e)

    await kitty.quitquitquit()


asyncio.run(test_function())
