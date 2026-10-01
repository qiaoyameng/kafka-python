import faulthandler
import os
import socket
from unittest.mock import patch

import pytest

from .mock_broker import MockBroker
from kafka.cluster import ClusterMetadata
from kafka.net.compat import KafkaNetClient
from kafka.net.manager import KafkaConnectionManager
from kafka.net.backend.selector import NetworkSelector
from kafka.protocol.metadata import MetadataResponse


# Independent watchdog at slightly under pytest-timeout's deadline (see pytest.ini)
faulthandler.enable()
faulthandler.dump_traceback_later(280, exit=False, repeat=False)


@pytest.fixture
def cross_thread_wakeup():
    """Ensure the selector backend can wake its IO thread cross-thread.

    Some restricted sandboxes allow AF_UNIX socketpair traffic only within a
    single thread (a write from another thread raises EPERM). The selector
    backend's wakeup channel is precisely a cross-thread socketpair write, so
    full KafkaProducer/KafkaConsumer lifecycle tests hang on close() there.
    os.pipe() supports the cross-thread write; substitute it *only* for the
    NetworkSelector module's own socketpair reference. Test code and other
    modules that use socket.socketpair() for peer-transport mocks are left
    untouched. On normal machines the probe succeeds and the fixture is a
    no-op.
    """
    import threading
    probe_r, probe_w = socket.socketpair()
    probe_error = []
    try:
        probe_r.setblocking(False)
        probe_w.setblocking(False)

        def _send():
            try:
                probe_w.send(b'\x00')
            except OSError as exc:
                probe_error.append(exc)

        waiter = threading.Thread(target=_send)
        waiter.start()
        waiter.join(5)
        needs_shim = bool(probe_error) or waiter.is_alive()
    finally:
        probe_r.close()
        probe_w.close()

    if not needs_shim:
        yield
        return

    class _PipeSocket:
        def __init__(self, fd):
            self._fd = fd

        def send(self, data, flags=0):
            return os.write(self._fd, bytes(data))

        def sendall(self, data, flags=0):
            return self.send(data, flags)

        def recv(self, n, flags=0):
            return os.read(self._fd, n)

        def setblocking(self, flag):
            os.set_blocking(self._fd, flag)

        def fileno(self):
            return self._fd

        def close(self):
            try:
                os.close(self._fd)
            except OSError:
                pass

        def setsockopt(self, *args):
            pass

        def getsockopt(self, *args):
            return 0

    def _pair():
        r, w = os.pipe()
        return _PipeSocket(r), _PipeSocket(w)

    from kafka.net.backend import selector as selector_module
    with patch.object(selector_module.socket, 'socketpair', _pair):
        yield


@pytest.fixture
def metrics():
    from kafka.metrics import Metrics

    metrics = Metrics()
    try:
        yield metrics
    finally:
        metrics.close()


@pytest.fixture
def broker(request):
    """
    To set broker version, use indirect parametrize:

    @pytest.mark.parametrize("broker", [(2, 3)], indirect=True)
    """
    broker_version = getattr(request, 'param', (4, 2))
    return MockBroker(broker_version=broker_version)


@pytest.fixture
def multi_broker(broker):
    Broker = MetadataResponse.MetadataResponseBroker
    broker.set_metadata(brokers=[
        Broker(node_id=0, host=broker.host, port=broker.port, rack=None),
        Broker(node_id=1, host=broker.host, port=broker.port, rack=None),
    ])
    return broker


@pytest.fixture
def client(net, manager, broker):
    cli = KafkaNetClient(net=net, manager=manager)
    try:
        yield cli
    finally:
        cli.close()


@pytest.fixture
def net():
    backend = NetworkSelector()
    try:
        yield backend
    finally:
        backend.close()


@pytest.fixture(params=['selector', 'asyncio'])
def all_net(request):
    """A *started* backend, parametrized over selector + asyncio, for curated
    both-backends end-to-end tests.

    Unlike the default ``net`` fixture (an unstarted NetworkSelector that tests
    drive inline), this runs the loop on its own IO thread -- required for the
    asyncio backend, whose ``run()`` has no inline no-IO-thread fallback. Pair
    it with a MockCluster (stateful): a started loop runs background heartbeat /
    metadata-refresh coroutines that fire unscripted requests, which MockCluster
    answers and a scripted MockBroker would not.

    The manager does not own a passed-in instance, so it neither starts nor
    closes it -- this fixture owns the lifecycle and closes it on teardown.
    """
    from kafka.net.backend import resolve_backend
    backend = resolve_backend(request.param, {})
    backend.start()
    try:
        yield backend
    finally:
        backend.close()


@pytest.fixture
def manager(net, broker):
    broker.attach(net)
    manager = KafkaConnectionManager(
        net,
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        request_timeout_ms=5000,
    )
    try:
        yield manager
    finally:
        manager.close()


@pytest.fixture
def cluster():
    return ClusterMetadata()
