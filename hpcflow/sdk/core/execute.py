import asyncio
import os
import queue
import struct
import threading

import zmq


class Executor:
    _app_attr = "app"

    def __init__(self, cmd, env, package_name):

        # TODO: make zmq_server optional (but required if action is abortable, or if
        #       `script_data_in`/`out`` is "zeromq")

        self.cmd = cmd
        self.env = env
        self.package_name = package_name

        # initialise a global ZeroMQ context for use in all threads:
        zmq.Context()

        self._q = None  # queue for inter-thread communication

        # assigned by `start_zmq_server`:
        self.port_number = None
        self.server_thread = None

        # assigned on (non-aborted) completion of the subprocess via `_subprocess_runner`:
        self.return_code = None

    @property
    def q(self):
        if not self._q:
            self._q = queue.Queue()
        return self._q

    @property
    def zmq_context(self):
        return zmq.Context.instance()

    def _zmq_server(self):
        """Start a ZeroMQ server on a random port.

        This method is invoked in a separate thread via `start_zmq_server`.

        """
        socket = self.zmq_context.socket(zmq.REP)
        port_number = socket.bind_to_random_port("tcp://*")
        self.app.logger.info(f"zmq_server: started on port {port_number}")

        # send port number back to main thread:
        self.q.put(port_number)  # TODO: can just set via `self.port_number`?

        # TODO: exception handling

        while True:
            message = socket.recv_string()
            self.app.logger.info(f"zmq_server: received request: {message}")

            # Check if the received message is a shutdown signal
            if message in ("shutdown", "abort"):
                self.q.put(message)
                socket.send_string("shutting down the server")
                break

            else:
                socket.send_string(f"received request: {message}")

        socket.close()
        self.app.logger.info("zmq_server: server stopped")

    def start_zmq_server(self) -> int:

        # start the server thread
        server_thread = threading.Thread(target=self._zmq_server)
        server_thread.start()

        # block until port number received:
        port_number = self.q.get()
        self.app.logger.info(f"received port number from child thread: {port_number}")

        self.port_number = port_number
        self.server_thread = server_thread

        return port_number

    def stop_zmq_server(self):

        # send a shutdown signal to the server:
        socket = self.zmq_context.socket(zmq.REQ)
        address = f"tcp://localhost:{self.port_number}"
        socket.connect(address)
        self.app.logger.info(
            f"stop_zmq_server: about to send shutdown message to server: {address!r}"
        )
        socket.send_string("shutdown")
        send_shutdown_out = socket.recv()
        self.app.logger.info(f"stop_zmq_server: received reply: {send_shutdown_out!r}")
        socket.close()

        # wait for the server thread to finish:
        self.app.logger.info(f"stop_zmq_server: joining server thread")
        self.server_thread.join()

        self.app.logger.info(f"stop_zmq_server: terminating ZMQ context")
        self.zmq_context.term()
        if self.server_thread.is_alive():
            raise RuntimeError("Server thread is still alive!")

    def run(self):
        """Launch the subprocess to execute the commands, and once complete, stop the
        ZMQ server. Kill the subprocess if a "shutdown" or "abort" message is sent to the
        server."""
        asyncio.run(self._run())
        return self.return_code

    def _receive_stop(self):
        """Wait until the queue receives a shutdown message from the server"""
        while True:
            if self.q.get() in ("shutdown", "abort"):
                return

    async def _subprocess_runner(self):
        app_caps = self.package_name.upper()
        env = {**self.env, f"{app_caps}_RUN_PORT": str(self.port_number)}
        try:
            process = await asyncio.create_subprocess_exec(*self.cmd, env=env)
            self.app.logger.info(f"_subprocess_runner: started subprocess: {process=!r}.")
            self.return_code = await process.wait()

        except asyncio.CancelledError:
            process.kill()

    async def _run(self) -> int:

        # create tasks for the subprocess and a synchronous Queue.get retrieval:
        try:
            wait_abort_thread = asyncio.to_thread(self._receive_stop)
        except AttributeError:
            # Python 3.8
            from hpcflow.sdk.core.utils import to_thread

            wait_abort_thread = to_thread(self._receive_stop)

        wait_abort_task = asyncio.create_task(wait_abort_thread)
        subprocess_task = asyncio.create_task(self._subprocess_runner())

        # wait for either: subprocess to finish, or a stop signal from the server:
        _, pending = await asyncio.wait(
            [wait_abort_task, subprocess_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # TODO: test we can SIGTERM and SIGINT the subprocess successfully?
        #   - add an API for sending signals to the process via the server?

        if pending == {wait_abort_task}:
            # subprocess completed; need to shutdown the server
            self.app.logger.info(f"_run: subprocess completed; stopping zmq server")
            self.stop_zmq_server()

        else:
            # subprocess still running but got a stop request; need to kill subprocess:
            self.app.logger.info(f"_run: stop request; killing subprocess")
            subprocess_task.cancel()

        if self.return_code and os.name == "nt":
            # Windows return codes are defined as 32-bit unsigned integers, but
            # some programs might still return negative numbers, so convert to a
            # signed 32-bit integer:
            self.return_code = struct.unpack("i", struct.pack("I", self.return_code))[0]

    @classmethod
    def send_abort(cls, hostname, port_number):
        """Send an abort message to a running server."""
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        address = f"tcp://{hostname}:{port_number}"
        socket.connect(address)
        cls.app.logger.info(
            f"send_abort: about to send abort message to server: {address!r}"
        )
        socket.send_string("abort")
        abort_rep = socket.recv()
        cls.app.logger.info(f"send_abort: received reply: {abort_rep!r}")
        socket.close()
        context.term()
