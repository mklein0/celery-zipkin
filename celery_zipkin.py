#
"""
http://docs.celeryproject.org/en/latest/userguide/signals.html
"""
from __future__ import absolute_import

import collections

from py_zipkin import zipkin

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


_ZipkinState = collections.namedtuple('_ZipkinState', 'call run wire results')


def get_app_zipkin_state(app):
    # type: (celery.app.Celery) -> _ZipkinState
    if not hasattr(app, 'zipkin_state'):
        app.zipkin_state = _ZipkinState({}, {}, {}, {})

    return app.zipkin_state


class CeleryAppZipkinInstrumentation(object):

    def __init__(self, http_transport, sample_rate=None):
        self.transport_handler = http_transport
        self.sample_rate = sample_rate

        self.started = False
        self._has_result_event = False

    def start(self):
        if not self.started:
            self._start()

    def _start(self):
        signals.before_task_publish.connect(weak=False)(self.task_sending_handler)
        signals.after_task_publish.connect(weak=False)(self.task_sent_handler)
        signals.task_prerun.connect(weak=False)(self.task_prerun_handler)
        signals.task_postrun.connect(weak=False)(self.task_postrun_handler)

        try:
            signals.before_result_publish.connect(weak=False)(self.result_sending_handler)
            signals.after_result_publish.connect(weak=False)(self.result_sent_handler)
            signals.after_result_received.connect(weak=False)(self.result_received_handler)

            self._has_result_event = True
        except AttributeError:
            pass

        self.started = True

    def stop(self):
        pass

    def _start_rpc_span(self, app, task_id):
        # type: (celery.app.Celery, str, str, str) -> py_zipkin.zipkin.client_span

        task_id = task_id.replace('-', '')
        # Can not reuse task id in zipkin.

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

        span = zipkin.zipkin_client_span(
            service_name='celery.{}'.format(app.main),
            span_name='task.call',
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        span.start()

        return span

    def _start_wire_send(self, app, span_name, task_id):
        # type: (celery.app.Celery, str, str, str, str) -> py_zipkin.zipkin.client_span

        task_id = task_id.replace('-', '')
        # Can not reuse task id in zipkin.

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

        span = zipkin.zipkin_client_span(
            service_name='celery.{}'.format(app.main),
            span_name=span_name,
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        span.start()

        return span

    def _start_taskrun_span(self, app, span_id=None, trace_id=None, parent_id=None, flags=None, is_sampled=None):
        # type: (celery.app.Celery, str, str, str, str, str) -> py_zipkin.zipkin.client_span

        # Parameters should be from task header

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

        span = zipkin.zipkin_client_span(
            service_name='celery.{}'.format(app.main),
            span_name='task.run',
            transport_handler=self.transport_handler,
            sample_rate=self.sample_rate,
            zipkin_attrs=zipkin_attrs,
        )

        span.start()

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
        zipkin_state = get_app_zipkin_state(app)

        # Get task definition to see if task ignores result
        try:
            task = app.tasks[sender]

        except KeyError:
            # Some times current app loses its name???
            sender = '__main__.' + sender.split('.', 1)[1]
            task = app.tasks[sender]

        # RPC Call
        if self._has_result_event and not task.ignore_result:
            # No information on if ignore result is set.
            rpc_span = self._start_rpc_span(app, task_id=task_id)
            zipkin_fields = {
                'x_b3_traceid': rpc_span.zipkin_attrs.trace_id,
                'x_b3_spanid': rpc_span.zipkin_attrs.span_id,
                'x_b3_parentspanid': rpc_span.zipkin_attrs.parent_span_id,
                'x_b3_flags': rpc_span.zipkin_attrs.flags,
                'x_b3_sampled': rpc_span.zipkin_attrs.is_sampled,
            }
            headers.update(zipkin_fields)
            zipkin_state.call[task_id] = rpc_span

        wire_span = self._start_wire_send(app, 'task.send', task_id=task_id)
        zipkin_state.wire[task_id] = wire_span

    def task_sent_handler(self, headers=None, **kwargs):
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
        zipkin_state = get_app_zipkin_state(app)

        wire_span = zipkin_state.wire.pop(task_id, None)
        if wire_span:
            wire_span.stop()

    def task_prerun_handler(self, task_id=None, task=None, **kwargs):
        """
        This is a server receive of the work request
        """
        context = task.request  # type: celery.app.task.Context
        """:type: celery.app.task.Context"""

        app = current_app
        task_span = self._start_taskrun_span(
            app,
            trace_id=context.get('x_b3_traceid'),
            parent_id=context.get('x_b3_parentspanid'),
            span_id=context.get('x_b3_spanid'),
            flags=context.get('x_b3_flags'),
            is_sampled=context.get('x_b3_sampled'),
        )

        if task is None:
            zipkin_state = get_app_zipkin_state(app)
            zipkin_state.run[task_id] = task_span

        else:
            task.zipkin_span = task_span

    def task_postrun_handler(self, task_id=None, task=None, **kwargs):
        """
        This is a server send of the work request.  There may be a retry of this request.
        """
        if task is not None:
            task_span = getattr(task, 'zipkin_span', None)
            if task_span is not None:
                task_span.stop()
                return

        # Could not find zipkin span on task.
        app = current_app
        zipkin_state = get_app_zipkin_state(app)

        task_span = zipkin_state.run.pop(task_id, None)
        if task_span:
            task_span.stop()

    def result_sending_handler(self, task_id=None, **kwargs):
        # information about task are located in headers for task messages
        # using the task protocol version 2.
        # http://docs.celeryproject.org/en/latest/internals/protocol.html
        app = current_app
        zipkin_state = get_app_zipkin_state(app)

        wire_span = self._start_wire_send(app, 'task.result', task_id=task_id)
        zipkin_state.result[task_id] = wire_span

    def result_sent_handler(self, task_id=None, **kwargs):
        app = current_app
        zipkin_state = get_app_zipkin_state(app)

        task_span = zipkin_state.result.pop(task_id, None)
        if task_span:
            task_span.stop()

    def result_received_handler(self, task_id=None, **kwargs):
        app = current_app
        zipkin_state = get_app_zipkin_state(app)

        task_span = zipkin_state.call.pop(task_id, None)
        if task_span:
            task_span.stop()
