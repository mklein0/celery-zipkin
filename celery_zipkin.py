#
"""
http://docs.celeryproject.org/en/latest/userguide/signals.html
"""
from __future__ import absolute_import

import collections
import platform as _platform
from socket import gethostname
import threading
import urlparse

from py_zipkin import zipkin

from kombu.utils.encoding import safe_str

from celery import signals
from celery._state import current_app


"""
def http_transport(encoded_span):
    # The collector expects a thrift-encoded list of spans. Instead of
    # decoding and re-encoding the already thrift-encoded message, we can just
    # add header bytes that specify that what follows is a list of length 1.
    body = '\x0c\x00\x00\x00\x01' + encoded_span
    requests.post(
        'http://localhost:9411/api/v1/spans',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )
"""

ZIPKIN_THRIFT_PREAMBLE = '\x0c\x00\x00\x00\x01'.encode()


_ZipkinState = collections.namedtuple('_ZipkinState', 'call rpc run')

_local = threading.local()


def get_zipkin_state():
    # type: () -> _ZipkinState
    global _local
    if not hasattr(_local, 'zipkin_state'):
        setattr(_local, 'zipkin_state', _ZipkinState(*({} for i in range(len(_ZipkinState._fields)))))

    return _local.zipkin_state


def get_task_from_app(app, name):
    # type: (celery.app.Celery, str) -> celery.app.task.Task
    """
    Get task definition to see if task ignores result
    """
    try:
        task = app.tasks[name]

    except KeyError:
        # Some times current app loses its name???
        name = '__main__.' + name.split('.', 1)[1]
        task = app.tasks[name]

    return task


def get_task_backend_name(task, is_eager=False):
    # type: (celery.app.task.Task, bool) -> str
    if is_eager:
        backend = 'is_eager'

    else:
        try:
            backend = urlparse.urlparse(task.backend.url)
            if backend.port:
                netloc = '{}:{}'.format(backend.hostname, backend.port)
            else:
                netloc = backend.hostname
            backend = urlparse.urlunsplit((backend.scheme, netloc, backend.url, backend.query, backend.fragment))

        except AttributeError:
            backend = repr(type(task.backend))

    return backend


class CeleryAppZipkinInstrumentation(object):

    def __init__(self, http_transport, sample_rate=None):
        self.transport_handler = http_transport
        self.sample_rate = sample_rate

        self.started = False
        self._has_result_event = False

        self._platform = safe_str(_platform.platform())
        self._hostname = safe_str(gethostname())

    def start(self):
        if not self.started:
            self._start()

    def _start(self):
        signals.before_task_publish.connect(weak=False)(self.task_sending_handler)
        signals.after_task_publish.connect(weak=False)(self.task_sent_handler)
        signals.task_prerun.connect(weak=False)(self.task_prerun_handler)
        signals.task_postrun.connect(weak=False)(self.task_postrun_handler)

        try:
            signals.after_result_received.connect(weak=False)(self.result_received_handler)

            self._has_result_event = True
        except AttributeError:
            pass

        self.started = True

    def stop(self):
        pass

    def _start_call_span(self, app, sender, task_id):
        # type: (celery.app.Celery, str, str) -> py_zipkin.zipkin.server_span

        zipkin_attrs = zipkin.get_zipkin_attrs()  # type: py_zipkin.zipkin.ZipkinAttrs
        """:type: py_zipkin.zipkin.ZipkinAttrs"""

        if zipkin_attrs:
            zipkin_attrs = zipkin.ZipkinAttrs(
                trace_id=zipkin_attrs.trace_id,
                span_id=zipkin.generate_random_64bit_string(),
                parent_span_id=zipkin_attrs.span_id,
                flags=zipkin_attrs.flags,
                is_sampled=zipkin_attrs.is_sampled,
            )

        else:
            zipkin_attrs = zipkin.create_attrs_for_span(self.sample_rate)

        # Get task definition to see if task ignores result
        task = get_task_from_app(app, sender)

        # Is Call Complete at this point.
        if task.ignore_result or not self._has_result_event:
            call_type = 'async'
            backend = None

        else:
            call_type = 'call'
            backend = get_task_backend_name(task, is_eager)

        span = zipkin.zipkin_server_span(
            service_name='celery.{}'.format(sender.split('.',1)[0]),
            span_name='task.{}:{}'.format(call_type, sender),
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        print span.service_name
        print span.span_name
        print span.zipkin_attrs

        span.start()
        span.update_binary_annotations_for_root_span({
            'celery.task.name': sender,
            'celery.task.type': call_type,
            'celery.task.backend': backend,
            'celery.machine': self._hostname,
            'celery.platform': self._platform,
        })

        return span

    def _start_rpc_span(self, sender, zipkin_attrs):
        # type: (str, py_zipkin.zipkin.ZipkinAttrs) -> py_zipkin.zipkin.client_span

        zipkin_attrs = zipkin.ZipkinAttrs(
            trace_id=zipkin_attrs.trace_id,
            span_id=zipkin.generate_random_64bit_string(),
            parent_span_id=zipkin_attrs.span_id,
            flags=zipkin_attrs.flags,
            is_sampled=zipkin_attrs.is_sampled,
        )

        span = zipkin.zipkin_client_span(
            service_name='celery.{}'.format(sender.split('.',1)[0]),
            span_name='task.rpc.{}'.format(sender),
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        span.start()
        return span

    def _start_taskrun_span(self, sender, task, span_id=None, trace_id=None, parent_id=None, flags=None, is_sampled=None):
        # type: (str, str, str, str, str, str) -> py_zipkin.zipkin.server_span

        zipkin_attrs = zipkin.get_zipkin_attrs()  # type: py_zipkin.zipkin.ZipkinAttrs
        """:type: py_zipkin.zipkin.ZipkinAttrs"""

        if not zipkin_attrs:
            zipkin_attrs = zipkin.create_attrs_for_span(self.sample_rate)
        zipkin_attrs = zipkin.ZipkinAttrs(
            trace_id=trace_id or zipkin_attrs.trace_id,
            span_id=span_id or zipkin.generate_random_64bit_string(),
            parent_span_id=parent_id or zipkin_attrs.span_id,
            flags=flags or zipkin_attrs.flags,
            is_sampled=zipkin_attrs.is_sampled if is_sampled is None else is_sampled,
        )

        span = zipkin.zipkin_server_span(
            service_name='celery.{}'.format(sender.split('.',1)[0]),
            span_name='task.run:{}'.format(sender),
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        backend = None
        context = task.request
        is_eager = context.get('is_eager', False)
        if task.ignore_result or not self._has_result_event:
            call_type = 'async'
        else:
            call_type = 'call'
            if is_eager:
                backend = 'is_eager'

            else:
                backend = get_task_backend_name(task, is_eager)

        span.start()
        span.update_binary_annotations_for_root_span({
            'celery.task.name': sender,
            'celery.task.type': call_type,
            'celery.task.backend': backend,
            'celery.machine': context.get('hostname', self._hostname),
            'celery.is_eager': context.get('is_eager', False),
            'celery.platform': self._platform,

        })

        return span

    def task_sending_handler(self, sender=None, headers=None, **kwargs):
        """
        Instrument the Celery message for a wire send. Would be nice to do a client send as well.
        """
        # information about task are located in headers for task messages
        # using the task protocol version 2.
        # http://docs.celeryproject.org/en/latest/internals/protocol.html
        if 'task' not in headers:
            # Protocol 1
            raise NotImplementedError('Celery Task Protocol 1 not supported')

        # Else, Protocol 2
        task_id = headers['id']  # task_id

        app = current_app
        zipkin_state = get_zipkin_state()

        # No information on if ignore result is set.
        call_span = self._start_call_span(app, sender=sender, task_id=task_id)
        zipkin_state.call[task_id] = call_span
        zipkin_attrs = call_span.zipkin_attrs

        # RPC Call
        # No information on if ignore result is set.
        rpc_span = self._start_rpc_span(sender=sender, zipkin_attrs=zipkin_attrs)
        zipkin_fields = {
            'x_b3_traceid': rpc_span.zipkin_attrs.trace_id,
            'x_b3_spanid': rpc_span.zipkin_attrs.span_id,
            'x_b3_parentspanid': rpc_span.zipkin_attrs.parent_span_id,
            'x_b3_flags': rpc_span.zipkin_attrs.flags,
            'x_b3_sampled': rpc_span.zipkin_attrs.is_sampled,
        }
        headers.update(zipkin_fields)
        zipkin_state.rpc[task_id] = rpc_span

    def task_sent_handler(self, sender=None, headers=None, **kwargs):
        """
        Complete wire send for celery task.

        :param sender:
        :param headers:
        :param body:
        :param kwargs:
        :return:
        """
        # information about task are located in headers for task messages
        # using the task protocol version 2.
        # http://docs.celeryproject.org/en/latest/internals/protocol.html
        if 'task' not in headers:
            # Protocol 1
            raise NotImplementedError('Celery Task Protocol 1 not supported')

        # Else, Protocol 2
        task_id = headers['id']  # task_id

        app = current_app
        zipkin_state = get_zipkin_state()

        # Get task definition to see if task ignores result
        task = get_task_from_app(app, sender)

        # Is Call Complete at this point.
        if task.ignore_result or not self._has_result_event:
            # If no result event available, treat all calls as an async event
            rpc_span = zipkin_state.rpc.pop(task_id, None)
            if rpc_span:
                rpc_span.stop()

            call_span = zipkin_state.call.pop(task_id, None)
            if call_span:
                call_span.stop()

        print zipkin_state

    def task_prerun_handler(self, task_id=None, task=None, **kwargs):
        """
        This is a server receive of the work request
        """
        context = task.request  # type: celery.app.task.Context
        """:type: celery.app.task.Context"""

        task_span = self._start_taskrun_span(
            task=task,
            sender=task.name,
            trace_id=context.get('x_b3_traceid'),
            parent_id=context.get('x_b3_parentspanid'),
            span_id=context.get('x_b3_spanid'),
            flags=context.get('x_b3_flags'),
            is_sampled=context.get('x_b3_sampled'),
        )

        if task is None:
            zipkin_state = get_zipkin_state()
            zipkin_state.run[task_id] = task_span

        else:
            task.zipkin_span = task_span

    def task_postrun_handler(self, task_id=None, task=None, state=None, **kwargs):
        """
        This is a server send of the work request.  There may be a retry of this request.
        """
        if task is not None:
            task_span = getattr(task, 'zipkin_span', None)
            if task_span is not None:
                task_span.stop()
                return

        # Could not find zipkin span on task.
        zipkin_state = get_zipkin_state()

        task_span = zipkin_state.run.pop(task_id, None)
        if task_span:
            task_span.update_binary_annotations_for_root_span({
                'celery.task.status': state,
            })
            task_span.stop()

    def result_received_handler(self, task_id=None, payload=None, **kwargs):
        import ipdb; ipdb.set_trace()
        zipkin_state = get_zipkin_state()

        task_span = zipkin_state.rpc.pop(task_id, None)
        if task_span:
            task_span.stop()

        task_span = zipkin_state.call.pop(task_id, None)
        if task_span:
            task_span.update_binary_annotations_for_root_span({
                'celery.task.status': payload.get('status'),
            })
            task_span.stop()


__version__ = '0.1.0+dev0'
