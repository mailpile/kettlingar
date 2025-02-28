# This is a sample kitten which shows how to serve and interact with an
# web page using the HTMX framework.
#
import asyncio

from .kitten import MyKitten


SINGLE_PAGE_APP = """\
<!DOCTYPE html>
<html><head>

  <title>Testing</title>

  <script src="https://unpkg.com/htmx.org@2.0.4"
    integrity="sha384-HGfztofotfshcF7+8n44JQL2oJmowVChPTg48S+jvZoztPfvwD79OC/LTtG6dMp+"
    crossorigin="anonymous"></script>
  <script src="https://unpkg.com/htmx-ext-sse@2.2.2"
    integrity="sha384-fw+eTlCc7suMV/1w/7fr2/PmwElUIt5i82bi+qTiLXvjRXZ2/FkiTNA/w0MhXnGI"
    crossorigin="anonymous"></script>

</head><body>

  <h1>HTMX Kitten Test</h1>

  <p hx-get='/%(secret)s/meow'>
    Click me!
  </p>

  <form>
    <button hx-post='/%(secret)s/purr' hx-vals='{
      "count": 3,
      "purr": "woof"
    }'>
      Purring works, but is not incremental.
    </button>
  </form>

  <p hx-ext="sse" sse-connect="/%(secret)s/events" sse-swap="hello" sse-close="eom">
    Server-sent events will appear here.
  <p>

</body></html>
"""


class HtmxKitten(MyKitten):
    """htmxkitten - A sample kettlingar HTMX app

    Inheriting from MyKitten, this microservice knows how to meow and
    how to purr. Purring is private and requires authentication, and
    may go on for a while.

    There is also a public HTMX document served at / to demonstrate
    how to use HTMX kettlingar as a back-end for HTMX pages.
    """
    class Configuration(MyKitten.Configuration):
        WORKER_NAME = 'HTMXKitten'

    async def public_api_web_root(self, request_info):
        """/

        A public landing page.

        This implementation leaks our secret and is horribly insecure as a
        result. Don't do this!
        """
        return 'text/html', SINGLE_PAGE_APP % {
            'secret': self.config.worker_secret}

    async def api_events(self, request_info, count=10):
        """/events

        This is a Server Sent Events endpoint which will send a few events
        to the client before cleanly shutting down.
        """
        # Yielding the text/event-stream MIME-type tells kettlingar this
        # is a Server Sent Events endpoint.
        yield 'text/event-stream', {'event': 'startup', 'data': 'Here we go!'}

        # Send some hellos...
        for i in range(count):
            await asyncio.sleep(1)
            yield None, {
                'event': 'hello',
                'data': 'Hello\nworld %d/%d ...' % (i+1, count)}

        # OK, that's enough!
        await asyncio.sleep(1)
        yield None, {'event': 'hello', 'data': 'Goodbye\ncruel\nworld!\n\n'}

        # Without this, the page will reconnect automatically.
        yield None, {'event': 'eom', 'data': 'eom'}


if __name__ == '__main__':
    import sys
    HtmxKitten.Main(sys.argv[1:])
