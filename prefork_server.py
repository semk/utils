#! /usr/bin/env python
#
# preforking server using epoll
# reference: http://scotdoyle.com/python-epoll-howto.html
#
# @author: Sreejith K


import os
import sys
import socket
import logging
import logging.handlers
import signal
import select
from multiprocessing import Process


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


class AsyncSocketHandler(object):
    """ Abstract class for Asynchronus socket handling
    """
    def __init__(self, sock):
        self.sock = sock
        self.request_buffer = {}
        self.response_buffer = {}
        self.__running = False

    def break_connection(self):
        """ Accepts no more socket connections.
        """
        self.__running = False
        raise KeyboardInterrupt

    def start_epoll(self):
        epoll = select.epoll()
        epoll.register(self.sock.fileno(), select.EPOLLIN)
        connections = {}; requests = {}; responses = {}
        self.__running = True
        while self.__running:
            try:
                events = epoll.poll(1)
                for fileno, event in events:
                    if fileno == self.sock.fileno():
                        log.info('Main: Accepting connections')
                        connection, address = self.sock.accept()
                        connection.setblocking(0)
                        epoll.register(connection.fileno(), select.EPOLLIN)
                        connections[connection.fileno()] = connection
                    elif event & select.EPOLLIN:
                        end_packet = self.handle_request(connections[fileno])
                        if end_packet:
                            epoll.modify(fileno, select.EPOLLOUT)
                    elif event & select.EPOLLOUT:
                        sent = self.handle_response(connections[fileno])
                        if sent:
                            epoll.modify(fileno, 0)
                            connections[fileno].shutdown(socket.SHUT_RDWR)
                    elif event & select.EPOLLHUP:
                        epoll.unregister(fileno)
                        connections[fileno].close()
                        del connections[fileno]
            except KeyboardInterrupt:
                log.warning('Main: Exiting gracefully')
                break
            #except Exception, ex:
            #    log.error('Error handling this request: %s' %ex)
        epoll.unregister(self.sock.fileno())
        epoll.close()
        self.sock.close()
        log.info('Main Process stopped gracefully')

    def handle_request(self, sock):
        """ Handle incoming packets. Return 'True' on retrieving complete packet
        """
        self.request_buffer[sock.fileno()] += sock.recv(8192)
        try:
            request = None#decode_request(self.request_buffer[sock.fileno()])
        except:
            return False
        else:
            self.response_buffer[sock.fileno()] = None#do_server_side_func(request)
            return True

    def handle_response(self, sock):
        """ Handle outgoing packets. Return 'True' once the response is sent
        """
        sock.send(self.response_buffer[sock.fileno()][:8192])
        self.response_buffer[sock.fileno()] = self.response_buffer[sock.fileno()][8192:]
        if len(self.response_buffer[self.fileno()]) == 0:
            return True
        return False


class Server(AsyncSocketHandler):
    """ RPC server which spawns multiple processes that listens on the same
    port. Uses Apache style preforking and uses epoll asynchronous sockets.
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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(addr)
        log.debug('%s listening on %s' % (type(self).__name__, addr))
        self.proc_pool = {}
        self.__running = False
        super(Server, self).__init__(sock)

    def spawn_worker_processes(self):
        """ Spawn all the worker processes
        """
        for i in range(self.procs):
            proc = Worker(self.sock, self.logdir, self.loglevel, name='Worker-%d' %i)
            self.proc_pool[proc.name] = proc
            proc.start()
            log.debug('Process %s started' %proc.name)

    def serve_forever(self):
        """ Accepts connection from multiple clients.
        """
        # register SIGTERM for graceful exit.
        def stop_gracefully(signum, frame):
            self.stop_server()
        
        signal.signal(signal.SIGTERM, stop_gracefully)

        # start listening on this port
        self.sock.listen(5)
        self.sock.setblocking(0)
        # spawn worker processes
        self.spawn_worker_processes()
        # start epoll
        self.start_epoll()

    def callback(self, object):
        log.info('Callback: %s' %object)
    
    def stop_server(self):
        """ Stop the server. Stop all the spawned processes.
        """
        log.info('Stopping all worker processes')
        for procname, proc in self.proc_pool.iteritems():
            if proc.is_alive():
                log.info('Stopping process %r' %proc.name)
                proc.terminate()
        self.break_connection()


class Worker(Process, AsyncSocketHandler):
    """ A worker process which listens on the same socket as its parent.
    """
    def __init__(self, sock, logdir, loglevel, group=None, target=None, name=None, args=(), kwargs={}):
        """
        Args:
            sock: shared server socket
        """
        self.logdir = logdir
        self.loglevel = loglevel
        self.log = logging.getLogger(name or self.__class__.__name__)
        Process.__init__(self, group, target, name, args, kwargs)
        AsyncSocketHandler.__init__(self, sock)

    def run(self):
        """ Listen on the socket given by the server process.
        NOTE: This will be a seperate process.
        """
        # register SIGTERM for graceful exit.
        def stop_gracefully(signum, frame):
            self.break_connection()
        
        signal.signal(signal.SIGTERM, stop_gracefully)

        # setup logging for workers
        logpath = os.path.join(self.logdir, '%s.log' %self.name)
        setup_logging(logpath, self.loglevel)
        # start epoll
        self.start_epoll()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    server = Server(('localhost', 4545))
    server.serve_forever()
    
