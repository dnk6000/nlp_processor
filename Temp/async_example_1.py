import asyncio

# Позаимствовано отсюда http://curio.readthedocs.org/en/latest/tutorial.html.
@asyncio.coroutine
def countdown(number, n, s):
    while n > 0:
        print('T-minus', n, '({})'.format(number))
        yield from asyncio.sleep(s)
        n -= 1

loop = asyncio.get_event_loop()
tasks = [
    asyncio.ensure_future(countdown("A", 7, 2.5)),
    asyncio.ensure_future(countdown("B", 10, 1))]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()
