from threading import Thread
from os.path import join
from aiohttp import web, ClientSession, client_exceptions
from yaml import load, dump
from yaml import Loader, Dumper
from sys import argv
from argparse import ArgumentParser
from asyncio import new_event_loop


class RequestHandler(Thread):

    def __init__(self, host, port, filename):
        super().__init__()
        self._url = 'http://' + host + ':' + str(port) + '/' + filename + '?redirect=True'
        self._text = ''
        self._answer = False

    async def _get_responce(self, loop):
        try:
            async with ClientSession(loop=loop) as client:
                async with client.get(self._url) as response:
                    if response.status == 200:
                        self._text = await response.text()
                        self._answer = True
        except client_exceptions.ClientConnectorError:
            print('Connection to ' + self._url + ' failed!')

    def run(self):
        loop = new_event_loop()
        loop.run_until_complete(self._get_responce(loop))

    def join(self):
        Thread.join(self)
        return (self._answer, self._text)


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

    async def _interview_other_nodes(self, filename):
        for node in self._nodes:
            host = self._nodes[node]['host']
            port = self._nodes[node]['port']
            request_thread = RequestHandler(host, port, filename)
            request_thread.start()
            answer, text = request_thread.join()
        return (answer, text)

    async def _file_handler(self, request):
        query = request.rel_url.query
        filename = request.match_info['filename']
        read_thread = Reader(self._directory, filename)
        read_thread.start()
        answer, text = read_thread.join()
        if not answer:
            if not query.get('redirect', None):
                answer, text = await self._interview_other_nodes(filename)
            if not answer:
                raise web.HTTPNotFound
            if self._save:
                write_thread = Writer(self._directory, filename, text)
                write_thread.start()
        return web.Response(text=text)

    async def _setup_routing(self):
        routes = [('*', '/{filename}', self._file_handler, 'file_handler'),]
        for route in routes:
            self._app.router.add_route(route[0],
                                       route[1],
                                       route[2],
                                       name=route[3])

    async def _create_app(self):
        self._app['dir'] = self._directory
        await self._setup_routing()
        return self._app

    def run(self):
        web.run_app(self._create_app(), host=self._host, port=self._port)

def parse_args(args):
    parser = ArgumentParser(description='This is a server parser')
    parser.add_argument(
        '-с',
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
