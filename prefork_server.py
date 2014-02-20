#! /usr/bin/env python
#
# A simple python preforking server
#
# @author: Sreejith K


import os
import sys
import socket
import logging
import logging.handlers
import signal
import select
from multiprocessing import Process, Queue
from multiprocessing.reduction import reduce_handle, rebuild_handle
from curses.ascii import ctrl


log = logging.getLogger(__name__)


def setup_logging(path, level):
    """Setup application logging.
    """
    log_handler = logging.handlers.RotatingFileHandler(path,
                                                      maxBytes=1024*1024,
                                                      backupCount=2)
    root_logger = logging.getLogger('')
    root_logger.setLevel(level)
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format)
    log_handler.setFormatter(formatter)
    root_logger.addHandler(log_handler)


class SocketHandler(object):
    """ Class for handling incoming socket connections. Used by workers.
    """
    def __init__(self, conn_queue):
        self.conn_queue = conn_queue
        self.request_handler = None   

    def start_conn_handling(self):
        log.debug('Waiting for connection....')
        serialized_socket = self.conn_queue.get()
        fd = rebuild_handle(serialized_socket)
        connection = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)

        log.info('Handling connection: %r' % connection)
        if self.request_handler:
            self.request_handler(connection)
        else:
            self.echo_handler(connection)

        log.info('Handler: Finished successfully.')

    def register_handler(self, handler):
        """ Register a callback for handing incoming packets.
        """
        if callable(handler):
            self.request_handler = handler
        else:
            raise Exception('%r should be calable' % handler)

    def echo_handler(self, connection):
        """ Fallback handler incase of no handlers. Echos the requests.
        """
        while True:
            request = connection.recv(1024)
            if 'end' in request:
                break
            elif request:
                connection.send(request)

        connection.send(ctrl(']'))
        connection.close()


class Server:
    """ Socket server which spawns multiple processes that handles new connections accepted
    by the server. Uses Apache style preforking.
    """
    def __init__(self, addr, procs=5, logdir=os.path.dirname(__file__), loglevel=logging.DEBUG):
        """ Initialize the server.
        
        Args:
            addr: address inwhich the server to listen on
            procs: number of processes it should fork
        """
        self.addr = addr
        self.procs = procs
        self.logdir = logdir
        self.loglevel = loglevel
        self.proc_pool = {}
        self.conn_queue = Queue(self.procs)

    def spawn_worker_processes(self):
        """ Spawn all the worker processes
        """
        for i in range(self.procs):
            proc = Worker(self.conn_queue, self.logdir, self.loglevel, name='Worker-%d' %i)
            self.proc_pool[proc.name] = proc
            proc.start()
            log.debug('Process %s started' %proc.name)

    def serve_forever(self):
        """ Accepts connection from multiple clients.
        """
        # register SIGTERM for graceful exit.
        def stop_gracefully(signum, frame):
            self.stop_server()
        
        signal.signal(signal.SIGINT, stop_gracefully)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.addr)
        log.debug('%s listening on %s' % (type(self).__name__, self.addr))

        # start listening on this port
        sock.listen(5)
        # sock.setblocking(0)

        # spawn worker processes
        self.spawn_worker_processes()

        try:
            while True:
                # start accepting connections
                log.debug('Main: Waiting for incoming connections')
                connection, address = sock.accept()
                # connection.setblocking(0)

                serialized_socket = reduce_handle(connection.fileno())
                self.conn_queue.put(serialized_socket)
        except socket.error:
            log.warning('Interrupt: Stopping main process')
            self.stop_server()
        finally:
            sock.close()

    def stop_server(self):
        """ Stop the server. Stop all the spawned processes.
        """
        log.info('Stopping all worker processes')
        for procname, proc in self.proc_pool.iteritems():
            if proc.is_alive():
                log.info('Stopping process %r' %proc.name)
                proc.terminate()


class Worker(Process, SocketHandler):
    """ A worker process which handles the client request.
    """
    def __init__(self, conn_queue, logdir, loglevel, group=None, target=None, name=None, args=(), kwargs={}):
        """
        Args:
            sock: shared server socket
        """
        self.conn_queue = conn_queue
        self.logdir = logdir
        self.loglevel = loglevel
        self.log = logging.getLogger(name or self.__class__.__name__)
        Process.__init__(self, group, target, name, args, kwargs)
        SocketHandler.__init__(self, conn_queue)

    def run(self):
        """ Listen on the new socket given by the server process.
        NOTE: This will be a seperate process.
        """
        # setup logging for workers
        logpath = os.path.join(self.logdir, '%s.log' %self.name)
        setup_logging(logpath, self.loglevel)
        # start connection handling
        while True:
            self.start_conn_handling()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    server = Server(('localhost', 4545))
    server.serve_forever()
    
