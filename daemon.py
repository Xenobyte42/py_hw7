import os
from threading import Thread
from os.path import join
from aiohttp import web, ClientSession, client_exceptions
from yaml import load, dump
from yaml import Loader, Dumper
from sys import argv
from argparse import ArgumentParser
from asyncio import new_event_loop, get_event_loop


class RequestHandler(Thread):

    def __init__(self, loop, host, port, node, filename):
        super().__init__()
        self._loop = loop
        self._url = 'http://' + host + ':' \
                    + str(port) + '/' + filename \
                    + '?do_not_visit=True'
        self._text = ''
        self._answer = False
        self._node = node

    async def _get_responce(self):
        try:
            async with ClientSession(loop=self._loop) as client:
                async with client.get(self._url) as response:
                    if response.status == 200:
                        self._text = await response.text()
                        self._answer = True
        except client_exceptions.ClientConnectorError:
            print('Connection to ' + self._url + ' failed!')

    def run(self):
        self._task = self._loop.create_task(self._get_responce())

    async def join(self):
        await self._task
        await self._loop.run_in_executor(None, Thread.join, self)
        return (self._answer, self._text, self._node)


class Writer(Thread):

    def __init__(self, directory, filename, text):
        super().__init__()
        self._filename = filename
        self._directory = directory
        self._text = text

    def run(self):
        with open(join(self._directory, self._filename), 'w') as f:
            f.write(self._text)


class Reader(Thread):

    def __init__(self, directory, filename):
        super().__init__()
        self._filename = filename
        self._directory = directory
        self._answer = False
        self._text = ''

    def run(self):
        try:
            with open(join(self._directory, self._filename), 'r') as f:
                for line in f:
                    self._text += line
        except FileNotFoundError:
            return
        self._answer = True

    def join(self):
        Thread.join(self)
        return (self._answer, self._text)
        

class Daemon:

    def __init__(self, params):
        with open(params.config, 'r') as f:
            self._data = load(f, Loader=Loader)
        self._host = self._data['host']
        self._port = self._data['port']
        self._directory = self._data['directory']
        self._nodes = self._data['other_nodes']
        self._save = self._data['save']
        self._app = web.Application()

    async def _async_delete_file(self, filename):
        os.remove(os.path.join(self._directory, filename))

    def _delete_file(self, loop, filename):
        loop.create_task(self._async_delete_file(filename))

    async def _query_for_other_nodes(self, loop, filename):
        answer = None
        text = ""
        save = False
        for node in self._nodes:
            host = self._nodes[node]['host']
            port = self._nodes[node]['port']
            request_thread = RequestHandler(loop, host, port, node, filename)
            request_thread.start()
            answer, text, node = await request_thread.join()
        need_to_save = self._nodes[node]['save']
        return (answer, text, need_to_save, node)

    async def _file_handler(self, request):
        query = request.rel_url.query
        filename = request.match_info['filename']
        read_thread = Reader(self._directory, filename)
        loop = get_event_loop()
        read_thread.start()
        answer, text = await loop.run_in_executor(None, read_thread.join)
        if not answer:
            if not query.get('do_not_visit', None):
                answer, text, save, node = await self._query_for_other_nodes(loop, filename)
            if not answer:
                raise web.HTTPNotFound
            if self._save and save:
                write_thread = Writer(self._directory, filename, text)
                write_thread.start()
                time = self._nodes[node].get('time', None) or self._nodes['time']
                loop.call_at(loop.time() + time, self._delete_file, loop, filename)
        return web.Response(text=text)

    async def _post_file(self, request):
        data = await request.post()
        filename = data['file'].filename
        up_file = data['file'].file
        text = up_file.read().decode()
        write_thread = Writer(self._directory, filename, text)
        write_thread.start()
        
        return web.Response(text='OK')

    async def _setup_routing(self):
        self._app.add_routes([
            web.route('*', '/add', self._post_file, name='post_file'),
            web.route('*', '/{filename}', self._file_handler, name='file_handler'),
            ])

    async def _create_app(self):
        self._app['dir'] = self._directory
        await self._setup_routing()
        return self._app

    def run(self):
        web.run_app(self._create_app(), host=self._host, port=self._port)

def parse_args(args):
    parser = ArgumentParser(description='This is a server parser')
    parser.add_argument(
        '-c',
        action='store',
        dest='config',
        type=str,
        default='config.yml',
        help='Daemon config'
    )
    return parser.parse_args(args)

if __name__ == '__main__':
    params = parse_args(argv[1:])
    d = Daemon(params)
    d.run()
