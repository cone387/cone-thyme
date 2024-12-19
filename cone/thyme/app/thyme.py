import queue
from calendar import timegm
from collections import namedtuple
from threading import Event, Thread

from billiard import ensure_multiprocessing
from billiard.context import Process
from celery.schedules import Callable
from apscheduler import *
from typing import List, Dict
from celery.apps.beat import Beat
from celery.bin.beat import beat
import traceback
import copy
import sys
import os
import time
from functools import total_ordering
from logging import debug
from datetime import datetime, timezone, tzinfo, timedelta
from dateutil import tz as dateutil_tz
# from celery.app.amqp import AMQP
import heapq
from dateutil import parser as date_parser

from kombu.utils import cached_property

MP_MAIN_FILE = os.environ.get('MP_MAIN_FILE')
event_t = namedtuple('event_t', ('time', 'priority', 'entry'))

schedule_state = namedtuple('schedule_state', ('is_due', 'next'))


_task_stack = []


class Task:

    #: Execution strategy used, or the qualified name of one.
    Strategy = 'celery.worker.strategy:default'

    #: Request class used, or the qualified name of one.
    Request = 'celery.worker.request:Request'

    #: The application instance associated with this task class.
    _app = None

    #: Name of the task.
    name = None

    #: Enable argument checking.
    #: You can set this to false if you don't want the signature to be
    #: checked when calling the task.
    #: Defaults to :attr:`app.strict_typing <@Celery.strict_typing>`.
    typing = None

    #: Maximum number of retries before giving up.  If set to :const:`None`,
    #: it will **never** stop retrying.
    max_retries = 3

    #: Default time in seconds before a retry of the task should be
    #: executed.  3 minutes by default.
    default_retry_delay = 3 * 60

    #: Rate limit for this task type.  Examples: :const:`None` (no rate
    #: limit), `'100/s'` (hundred tasks a second), `'100/m'` (hundred tasks
    #: a minute),`'100/h'` (hundred tasks an hour)
    rate_limit = None

    #: If enabled the worker won't store task state and return values
    #: for this task.  Defaults to the :setting:`task_ignore_result`
    #: setting.
    ignore_result = None

    #: If enabled the request will keep track of subtasks started by
    #: this task, and this information will be sent with the result
    #: (``result.children``).
    trail = True

    #: If enabled the worker will send monitoring events related to
    #: this task (but only if the worker is configured to send
    #: task related events).
    #: Note that this has no effect on the task-failure event case
    #: where a task is not registered (as it will have no task class
    #: to check this flag).
    send_events = True

    #: When enabled errors will be stored even if the task is otherwise
    #: configured to ignore results.
    store_errors_even_if_ignored = None

    #: The name of a serializer that are registered with
    #: :mod:`kombu.serialization.registry`.  Default is `'json'`.
    serializer = None

    #: Hard time limit.
    #: Defaults to the :setting:`task_time_limit` setting.
    time_limit = None

    #: Soft time limit.
    #: Defaults to the :setting:`task_soft_time_limit` setting.
    soft_time_limit = None

    #: The result store backend used for this task.
    backend = None

    #: If enabled the task will report its status as 'started' when the task
    #: is executed by a worker.  Disabled by default as the normal behavior
    #: is to not report that level of granularity.  Tasks are either pending,
    #: finished, or waiting to be retried.
    #:
    #: Having a 'started' status can be useful for when there are long
    #: running tasks and there's a need to report what task is currently
    #: running.
    #:
    #: The application default can be overridden using the
    #: :setting:`task_track_started` setting.
    track_started = None

    #: When enabled messages for this task will be acknowledged **after**
    #: the task has been executed, and not *right before* (the
    #: default behavior).
    #:
    #: Please note that this means the task may be executed twice if the
    #: worker crashes mid execution.
    #:
    #: The application default can be overridden with the
    #: :setting:`task_acks_late` setting.
    acks_late = None

    #: When enabled messages for this task will be acknowledged even if it
    #: fails or times out.
    #:
    #: Configuring this setting only applies to tasks that are
    #: acknowledged **after** they have been executed and only if
    #: :setting:`task_acks_late` is enabled.
    #:
    #: The application default can be overridden with the
    #: :setting:`task_acks_on_failure_or_timeout` setting.
    acks_on_failure_or_timeout = None

    reject_on_worker_lost = None
    throws = ()

    #: Default task expiry time.
    expires = None

    #: Default task priority.
    priority = None

    #: Max length of result representation used in logs and events.
    resultrepr_maxsize = 1024

    #: Task request stack, the current request will be the topmost.
    request_stack = None

    #: Some may expect a request to exist even if the task hasn't been
    #: called.  This should probably be deprecated.
    _default_request = None

    #: Deprecated attribute ``abstract`` here for compatibility.
    abstract = True

    _exec_options = None

    __bound__ = False

    from_config = (
        ('serializer', 'task_serializer'),
        ('rate_limit', 'task_default_rate_limit'),
        ('priority', 'task_default_priority'),
        ('track_started', 'task_track_started'),
        ('acks_late', 'task_acks_late'),
        ('acks_on_failure_or_timeout', 'task_acks_on_failure_or_timeout'),
        ('reject_on_worker_lost', 'task_reject_on_worker_lost'),
        ('ignore_result', 'task_ignore_result'),
        ('store_eager_result', 'task_store_eager_result'),
        ('store_errors_even_if_ignored', 'task_store_errors_even_if_ignored'),
    )

    _backend = None  # set by backend property.

    # - Tasks are lazily bound, so that configuration is not set
    # - until the task is actually used

    def __call__(self, *args, **kwargs):
        _task_stack.push(self)
        self.push_request(args=args, kwargs=kwargs)
        try:
            return self.run(*args, **kwargs)
        finally:
            self.pop_request()
            _task_stack.pop()

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError('Tasks must define the run method.')

    def delay(self, *args, **kwargs):
        """Star argument version of :meth:`apply_async`.

        Does not support the extra options enabled by :meth:`apply_async`.

        Arguments:
            *args (Any): Positional arguments passed on to the task.
            **kwargs (Any): Keyword arguments passed on to the task.
        Returns:
            celery.result.AsyncResult: Future promise.
        """
        return self.apply_async(args, kwargs)

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None,
                    link=None, link_error=None, shadow=None, **options):
        """Apply tasks asynchronously by sending a message.

        Arguments:
            args (Tuple): The positional arguments to pass on to the task.

            kwargs (Dict): The keyword arguments to pass on to the task.

            countdown (float): Number of seconds into the future that the
                task should execute.  Defaults to immediate execution.

            eta (~datetime.datetime): Absolute time and date of when the task
                should be executed.  May not be specified if `countdown`
                is also supplied.

            expires (float, ~datetime.datetime): Datetime or
                seconds in the future for the task should expire.
                The task won't be executed after the expiration time.

            shadow (str): Override task name used in logs/monitoring.
                Default is retrieved from :meth:`shadow_name`.

            connection (kombu.Connection): Re-use existing broker connection
                instead of acquiring one from the connection pool.

            retry (bool): If enabled sending of the task message will be
                retried in the event of connection loss or failure.
                Default is taken from the :setting:`task_publish_retry`
                setting.  Note that you need to handle the
                producer/connection manually for this to work.

            retry_policy (Mapping): Override the retry policy used.
                See the :setting:`task_publish_retry_policy` setting.

            time_limit (int): If set, overrides the default time limit.

            soft_time_limit (int): If set, overrides the default soft
                time limit.

            queue (str, kombu.Queue): The queue to route the task to.
                This must be a key present in :setting:`task_queues`, or
                :setting:`task_create_missing_queues` must be
                enabled.  See :ref:`guide-routing` for more
                information.

            exchange (str, kombu.Exchange): Named custom exchange to send the
                task to.  Usually not used in combination with the ``queue``
                argument.

            routing_key (str): Custom routing key used to route the task to a
                worker server.  If in combination with a ``queue`` argument
                only used to specify custom routing keys to topic exchanges.

            priority (int): The task priority, a number between 0 and 9.
                Defaults to the :attr:`priority` attribute.

            serializer (str): Serialization method to use.
                Can be `pickle`, `json`, `yaml`, `msgpack` or any custom
                serialization method that's been registered
                with :mod:`kombu.serialization.registry`.
                Defaults to the :attr:`serializer` attribute.

            compression (str): Optional compression method
                to use.  Can be one of ``zlib``, ``bzip2``,
                or any custom compression methods registered with
                :func:`kombu.compression.register`.
                Defaults to the :setting:`task_compression` setting.

            link (Signature): A single, or a list of tasks signatures
                to apply if the task returns successfully.

            link_error (Signature): A single, or a list of task signatures
                to apply if an error occurs while executing the task.

            producer (kombu.Producer): custom producer to use when publishing
                the task.

            add_to_parent (bool): If set to True (default) and the task
                is applied while executing another task, then the result
                will be appended to the parent tasks ``request.children``
                attribute.  Trailing can also be disabled by default using the
                :attr:`trail` attribute

            ignore_result (bool): If set to `False` (default) the result
                of a task will be stored in the backend. If set to `True`
                the result will not be stored. This can also be set
                using the :attr:`ignore_result` in the `app.task` decorator.

            publisher (kombu.Producer): Deprecated alias to ``producer``.

            headers (Dict): Message headers to be included in the message.

        Returns:
            celery.result.AsyncResult: Promise of future evaluation.

        Raises:
            TypeError: If not enough arguments are passed, or too many
                arguments are passed.  Note that signature checks may
                be disabled by specifying ``@task(typing=False)``.
            kombu.exceptions.OperationalError: If a connection to the
               transport cannot be made, or if the connection is lost.

        Note:
            Also supports all keyword arguments supported by
            :meth:`kombu.Producer.publish`.
        """
        if self.typing:
            try:
                check_arguments = self.__header__
            except AttributeError:  # pragma: no cover
                pass
            else:
                check_arguments(*(args or ()), **(kwargs or {}))

        if self.__v2_compat__:
            shadow = shadow or self.shadow_name(self(), args, kwargs, options)
        else:
            shadow = shadow or self.shadow_name(args, kwargs, options)

        preopts = self._get_exec_options()
        options = dict(preopts, **options) if options else preopts

        options.setdefault('ignore_result', self.ignore_result)
        if self.priority:
            options.setdefault('priority', self.priority)

        app = self._get_app()
        if app.conf.task_always_eager:
            with app.producer_or_acquire(producer) as eager_producer:
                serializer = options.get('serializer')
                if serializer is None:
                    if eager_producer.serializer:
                        serializer = eager_producer.serializer
                    else:
                        serializer = app.conf.task_serializer
                body = args, kwargs
                content_type, content_encoding, data = serialization.dumps(
                    body, serializer,
                )
                args, kwargs = serialization.loads(
                    data, content_type, content_encoding,
                    accept=[content_type]
                )
            with denied_join_result():
                return self.apply(args, kwargs, task_id=task_id or uuid(),
                                  link=link, link_error=link_error, **options)
        else:
            return app.send_task(
                self.name, args, kwargs, task_id=task_id, producer=producer,
                link=link, link_error=link_error, result_cls=self.AsyncResult,
                shadow=shadow, task_type=self,
                **options
            )

    def apply(self, args=None, kwargs=None,
              link=None, link_error=None,
              task_id=None, retries=None, throw=None,
              logfile=None, loglevel=None, headers=None, **options):
        """Execute this task locally, by blocking until the task returns.

        Arguments:
            args (Tuple): positional arguments passed on to the task.
            kwargs (Dict): keyword arguments passed on to the task.
            throw (bool): Re-raise task exceptions.
                Defaults to the :setting:`task_eager_propagates` setting.

        Returns:
            celery.result.EagerResult: pre-evaluated result.
        """
        # trace imports Task, so need to import inline.
        from celery.app.trace import build_tracer

        app = self._get_app()
        args = args or ()
        kwargs = kwargs or {}
        task_id = task_id or uuid()
        retries = retries or 0
        if throw is None:
            throw = app.conf.task_eager_propagates

        # Make sure we get the task instance, not class.
        task = app._tasks[self.name]

        request = {
            'id': task_id,
            'task': self.name,
            'retries': retries,
            'is_eager': True,
            'logfile': logfile,
            'loglevel': loglevel or 0,
            'hostname': gethostname(),
            'callbacks': maybe_list(link),
            'errbacks': maybe_list(link_error),
            'headers': headers,
            'ignore_result': options.get('ignore_result', False),
            'delivery_info': {
                'is_eager': True,
                'exchange': options.get('exchange'),
                'routing_key': options.get('routing_key'),
                'priority': options.get('priority'),
            }
        }
        if 'stamped_headers' in options:
            request['stamped_headers'] = maybe_list(options['stamped_headers'])
            request['stamps'] = {
                header: maybe_list(options.get(header, [])) for header in request['stamped_headers']
            }

        tb = None
        tracer = build_tracer(
            task.name, task, eager=True,
            propagate=throw, app=self._get_app(),
        )
        ret = tracer(task_id, args, kwargs, request)
        retval = ret.retval
        if isinstance(retval, ExceptionInfo):
            retval, tb = retval.exception, retval.traceback
            if isinstance(retval, ExceptionWithTraceback):
                retval = retval.exc
        if isinstance(retval, Retry) and retval.sig is not None:
            return retval.sig.apply(retries=retries + 1)
        state = states.SUCCESS if ret.info is None else ret.info.state
        return EagerResult(task_id, retval, state, traceback=tb, name=self.name)

    def push_request(self, *args, **kwargs):
        self.request_stack.push(Context(*args, **kwargs))

    def pop_request(self):
        self.request_stack.pop()

    @property
    def __name__(self):
        return self.__class__.__name__


class BaseSchedule:
    name = None

    IS_END = -1

    # def next_time(self, base_time=None) -> datetime | None:
    #     raise NotImplementedError
    #
    # def next_time_and_interval(self, base_time=None):
    #     next_time = self.next_time(base_time)
    #     return next_time, 1

    def is_due(self, last_run_at: datetime):
        """Return tuple of ``(is_due, next_time_to_check)``.

                Notes:
                    - next time to check is in seconds.

                    - ``(True, 20)``, means the task should be run now, and the next
                        time to check is in 20 seconds.

                    - ``(False, 12.3)``, means the task is not due, but that the
                      scheduler should check again in 12.3 seconds.

                    - ``(True | False, -1)``, means the task is end, ant the schedule_entry
                      can be deleted
        """
        raise NotImplementedError


def _safe_str(s, errors='replace', file=None):
    if isinstance(s, str):
        return s
    try:
        return str(s)
    except Exception as exc:
        return '<Unrepresentable {!r}: {!r} {!r}>'.format(
            type(s), exc, '\n'.join(traceback.format_stack()))


def _safe_repr(o, errors='replace'):
    """Safe form of repr, void of Unicode errors."""
    try:
        return repr(o)
    except Exception:
        return _safe_str(o, errors)


def reprkwargs(kwargs, sep=', ', fmt='{0}={1}'):
    return sep.join(fmt.format(k, _safe_repr(v)) for k, v in kwargs.items())


def reprcall(name, args=(), kwargs=None, sep=', '):
    kwargs = {} if not kwargs else kwargs
    return '{}({}{}{})'.format(
        name, sep.join(map(_safe_repr, args or ())),
        (args and kwargs) and sep or '',
        reprkwargs(kwargs, sep),
    )


@total_ordering
class ScheduleEntry:
    """An entry in the scheduler.

    Arguments:
        name (str): see :attr:`name`.
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        args (Tuple): see :attr:`args`.
        kwargs (Dict): see :attr:`kwargs`.
        options (Dict): see :attr:`options`.
        last_run_at (~datetime.datetime): see :attr:`last_run_at`.
        total_run_count (int): see :attr:`total_run_count`.
        relative (bool): Is the time relative to when the server starts?
    """

    #: The task name
    name = None

    #: The schedule (:class:`~celery.schedules.schedule`)
    schedule = None

    #: Positional arguments to apply.
    args = None

    #: Keyword arguments to apply.
    kwargs = None

    #: Task execution options.
    options = None

    #: The time and date of when this task was last scheduled.
    last_run_at = None

    last_scheduled_at = None

    #: Total number of times this task has been scheduled.
    total_run_count = 0

    def __init__(self, name=None, task=None, last_run_at=None, skip_missed=True,
                 total_run_count=None, schedule: BaseSchedule = None, args=(), kwargs=None,
                 options=None, app=None):
        self.app = app
        self.name = name
        self.task = task
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.options = options if options else {}
        self.schedule = schedule
        self.last_run_at = last_run_at or self.default_now()
        self.total_run_count = total_run_count or 0
        self.skip_missed = skip_missed

    def default_now(self):
        return datetime.now()
    _default_now = default_now  # compat

    def _next_instance(self, last_run_at=None):
        """Return new instance, with date and count fields updated."""
        return self.__class__(**dict(
            self,
            last_run_at=last_run_at,
            total_run_count=self.total_run_count + 1,
        ))
    __next__ = next = _next_instance  # for 2to3

    def __reduce__(self):
        return self.__class__, (
            self.name, self.task, self.last_run_at, self.total_run_count,
            self.schedule, self.args, self.kwargs, self.options,
        )

    def update(self, other):
        """Update values from another entry.

        Will only update "editable" fields:
            ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        self.__dict__.update({
            'task': other.task, 'schedule': other.schedule,
            'args': other.args, 'kwargs': other.kwargs,
            'options': other.options,
        })

    def is_due(self):
        """See :meth:`~celery.schedules.schedule.is_due`."""
        base_time = self.last_run_at if self.skip_missed else self.last_scheduled_at
        return self.schedule.is_due(base_time)

    def __iter__(self):
        return iter(vars(self).items())

    def __repr__(self):
        return '<{name}: {0.name} {call} {0.schedule}'.format(
            self,
            call=reprcall(self.task, self.args or (), self.kwargs or {}),
            name=type(self).__name__,
        )

    def __lt__(self, other):
        if isinstance(other, ScheduleEntry):
            # How the object is ordered doesn't really matter, as
            # in the scheduler heap, the order is decided by the
            # preceding members of the tuple ``(time, priority, entry)``.
            #
            # If all that's left to order on is the entry then it can
            # just as well be random.
            return id(self) < id(other)
        return NotImplemented

    def editable_fields_equal(self, other):
        for attr in ('task', 'args', 'kwargs', 'options', 'schedule'):
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __eq__(self, other):
        """Test schedule entries equality.

        Will only compare "editable" fields:
        ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        return self.editable_fields_equal(other)


class SchedulingError(Exception):
    """An error occurred while scheduling a task."""


class BeatLazyFunc:
    """A lazy function declared in 'beat_schedule' and called before sending to worker.

    Example:

        beat_schedule = {
            'test-every-5-minutes': {
                'task': 'test',
                'schedule': 300,
                'kwargs': {
                    "current": BeatCallBack(datetime.datetime.now)
                }
            }
        }

    """

    def __init__(self, func, *args, **kwargs):
        self._func = func
        self._func_params = {
            "args": args,
            "kwargs": kwargs
        }

    def __call__(self):
        return self.delay()

    def delay(self):
        return self._func(*self._func_params["args"], **self._func_params["kwargs"])


def _evaluate_entry_args(entry_args):
    if not entry_args:
        return []
    return [
        v() if isinstance(v, BeatLazyFunc) else v
        for v in entry_args
    ]


def _evaluate_entry_kwargs(entry_kwargs):
    if not entry_kwargs:
        return {}
    return {
        k: v() if isinstance(v, BeatLazyFunc) else v
        for k, v in entry_kwargs.items()
    }


class Scheduler:
    max_interval = 5 * 60

    _last_sync = None
    _tasks_since_sync = 0

    #: How often to sync the schedule (3 minutes by default)
    sync_every = 3 * 60

    #: How many tasks can be called before a sync is forced.
    sync_every_tasks = None

    def __init__(self, app: 'Thyme'):
        self._schedules: Dict[str, ScheduleEntry] = {}
        self._heap = []
        self.old_schedulers = None
        self.app = app

    def add_schedule_entry(self, schedule_entry: ScheduleEntry):
        heapq.heappush(self._heap, schedule_entry)
        self._schedules[schedule_entry.name] = schedule_entry

    @staticmethod
    def adjust(n, drift=-0.010):
        if n and n > 0:
            return n + drift
        return n

    def is_due(self, entry):
        return entry.is_due()

    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop, heappush=heapq.heappush):
        """Run a tick - one iteration of the scheduler.

                Executes one due task per call.

                Returns:
                    float: preferred delay in seconds for next call.
                """
        adjust = self.adjust
        max_interval = self.max_interval

        schedules = self._schedules

        if (schedules is None or
                not self.schedules_equal(self.old_schedulers, self._schedules)):
            self.old_schedulers = copy.copy(self._schedules)
            self.populate_heap()

        if not schedules:
            return max_interval

        H = self._heap
        event = H[0]
        entry = event[2]
        is_due, next_time_to_run = self.is_due(entry)
        if is_due:
            verify = heappop(H)
            if verify is event:
                next_entry = self.reserve(entry)
                self.apply_entry(entry)
                heappush(H, event_t(self._when(next_entry, next_time_to_run), event[1], next_entry))
                return 0
            else:
                heappush(H, verify)
                return min(verify[0], max_interval)
        adjusted_next_time_to_run = adjust(next_time_to_run)
        return min(adjusted_next_time_to_run, max_interval)

    def apply_entry(self, entry):
        print('Scheduler: Sending due task %s (%s)' % (entry.name, entry.task))
        try:
            result = self.apply_async(entry)
        except Exception as exc:  # pylint: disable=broad-except
            print('Message Error: %s\n%s' % (exc, traceback.format_stack()))
        else:
            if result and hasattr(result, 'id'):
                debug('%s sent. id->%s', entry.task, result.id)
            else:
                debug('%s sent.', entry.task)

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        # Update time-stamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry) if advance else entry
        task = self.app.tasks.get(entry.task)

        # try:
        entry_args = _evaluate_entry_args(entry.args)
        entry_kwargs = _evaluate_entry_kwargs(entry.kwargs)
        if task:
            return task.apply_async(entry_args, entry_kwargs, producer=producer, **entry.options)
        else:
            return self.app.send_task(entry.task, entry_args, entry_kwargs, **entry.options)
        # except Exception as exc:  # pylint: disable=broad-except
        #     raise SchedulingError("Couldn't apply scheduled task {0.name}: {exc}".format(entry, exc=exc))
        # finally:
        #     self._tasks_since_sync += 1
        #     if self.should_sync():
        #         self._do_sync()

    def send_task(self, task):
        self.app.send_task(task)

    def _when(self, entry, next_time_to_run, mktime=timegm):
        """Return a utc timestamp, make sure heapq in correct order."""
        adjust = self.adjust

        as_now = entry.default_now()

        return (mktime(as_now.utctimetuple()) +
                as_now.microsecond / 1e6 +
                (adjust(next_time_to_run) or 0))

    @staticmethod
    def schedules_equal(old_schedules, new_schedules):
        if old_schedules is new_schedules is None:
            return True
        if old_schedules is None or new_schedules is None:
            return False
        if set(old_schedules.keys()) != set(new_schedules.keys()):
            return False
        for name, old_entry in old_schedules.items():
            new_entry = new_schedules.get(name)
            if not new_entry:
                return False
            if new_entry != old_entry:
                return False
        return True

    def reserve(self, entry):
        new_entry = self._schedules[entry.name] = next(entry)
        return new_entry

    def populate_heap(self, event_t=event_t, heapify=heapq.heapify):
        """Populate the heap with the data contained in the schedule."""
        priority = 5
        self._heap = []
        for entry in self._schedules.values():
            is_due, next_call_delay = entry.is_due()
            self._heap.append(event_t(
                self._when(
                    entry,
                    0 if is_due else next_call_delay
                ) or 0,
                priority, entry
            ))
        heapify(self._heap)

    def should_sync(self):
        return (
            (not self._last_sync or (time.monotonic() - self._last_sync) > self.sync_every) or
            (self.sync_every_tasks and
             self._tasks_since_sync >= self.sync_every_tasks)
        )

    def _do_sync(self):
        try:
            debug('beat: Synchronizing schedule...')
            self.sync()
        finally:
            self._last_sync = time.monotonic()
            self._tasks_since_sync = 0

    def sync(self):
        pass


class Service:
    scheduler_cls = Scheduler

    def __init__(self, app, max_interval=None, scheduler_cls=None):
        self.app = app
        # self.max_interval = (max_interval or app.conf.beat_max_loop_interval)
        self.scheduler_cls = scheduler_cls or self.scheduler_cls
        self._is_shutdown = Event()
        self._is_stopped = Event()
        self.scheduler = self.load_scheduler()

    def load_scheduler(self) -> 'Scheduler':
        return self.scheduler_cls(self.app)

    def run(self):
        # try:
        while not self._is_shutdown.is_set():
            interval = self.scheduler.tick()
            if interval and interval > 0.0:
                debug('beat: Waking up %s.', humanize_seconds(interval, prefix='in '))
                time.sleep(interval)
                if self.scheduler.should_sync():
                    self.scheduler._do_sync()
        # except (KeyboardInterrupt, SystemExit):
        #     self._is_shutdown.set()
        # finally:
        #     # 为什么最后要同步一次？
        #     self.sync()

    def start(self, embedded_process=False):
        self.run()

    def sync(self):
        pass

    def stop(self, wait=False):
        pass


class _Threaded(Thread):
    """Embedded task scheduler using threading."""

    def __init__(self, app, **kwargs):
        super().__init__()
        self.app = app
        self.service = Service(app, **kwargs)
        self.daemon = True
        self.name = 'Beat'

    def run(self):
        self.app.set_current()
        self.service.start()

    def stop(self):
        self.service.stop(wait=True)


try:
    ensure_multiprocessing()
except NotImplementedError:     # pragma: no cover
    _Process = None
else:
    class _Process(Process):

        def __init__(self, app, **kwargs):
            super().__init__()
            self.app = app
            self.service = Service(app, **kwargs)
            self.name = 'Beat'

        def run(self):
            # reset_signals(full=False)
            # platforms.close_open_fds([
            #     sys.__stdin__, sys.__stdout__, sys.__stderr__,
            # ] + list(iter_open_logger_fds()))
            self.app.set_default()
            self.app.set_current()
            self.service.start(embedded_process=True)

        def stop(self):
            self.service.stop()
            self.terminate()


class EmbeddedService(Scheduler):

    def __new__(cls, app, max_interval=None, threaded=False):
        if threaded or _Process is None:
            # Need short max interval to be able to stop thread
            # in reasonable time.
            return _Threaded(app, max_interval=max_interval)
        return _Process(app, max_interval=max_interval)


TIME_UNITS = (
    ('day', 60 * 60 * 24.0, lambda n: format(n, '.2f')),
    ('hour', 60 * 60.0, lambda n: format(n, '.2f')),
    ('minute', 60.0, lambda n: format(n, '.2f')),
    ('second', 1.0, lambda n: format(n, '.2f')),
)


def pluralize(n: float, text: str, suffix: str = 's') -> str:
    """Pluralize term when n is greater than one."""
    if n != 1:
        return text + suffix
    return text


def humanize_seconds(
        secs, prefix: str = '', sep: str = '', now: str = 'now',
        microseconds: bool = False) -> str:
    """Show seconds in human form.

    For example, 60 becomes "1 minute", and 7200 becomes "2 hours".

    Arguments:
        prefix (str): can be used to add a preposition to the output
            (e.g., 'in' will give 'in 1 second', but add nothing to 'now').
        now (str): Literal 'now'.
        microseconds (bool): Include microseconds.
    """
    secs = float(format(float(secs), '.2f'))
    for unit, divider, formatter in TIME_UNITS:
        if secs >= divider:
            w = secs / float(divider)
            return '{}{}{} {}'.format(prefix, sep, formatter(w),
                                      pluralize(w, unit))
    if microseconds and secs > 0.0:
        return '{prefix}{sep}{0:.2f} seconds'.format(secs, sep=sep, prefix=prefix)
    return now


def gen_task_name(app, name, module_name):
    """Generate task name from name/module pair."""
    module_name = module_name or '__main__'
    try:
        module = sys.modules[module_name]
    except KeyError:
        # Fix for manage.py shell_plus (Issue #366)
        module = None

    if module is not None:
        module_name = module.__name__
        # - If the task module is used as the __main__ script
        # - we need to rewrite the module part of the task name
        # - to match App.main.
        if MP_MAIN_FILE and module.__file__ == MP_MAIN_FILE:
            # - see comment about :envvar:`MP_MAIN_FILE` above.
            module_name = '__main__'
    if module_name == '__main__' and app.main:
        return '.'.join([app.main, name])
    return '.'.join(p for p in (module_name, name) if p)


_app = None


def get_default_app():
    global _app
    if _app is None:
        _app = Thyme()
    return _app


def _schedule(*args, **kwargs):
    app = get_default_app()
    return app.schedule(*args, **kwargs)


def _task(*args, **kwargs):
    return get_default_app().task(*args, **kwargs)


class InvalidScheduleConfigType(Exception):
    pass


class FixedTime(BaseSchedule):

    def __init__(self, datetimes: List[datetime] | List[str]):
        self.datetimes = [date_parser.parse(x) if isinstance(x, str) else x for x in datetimes]
        self.datetimes.sort()

    def next_time(self, base_time=None):
        pass

    def is_due(self, base_time):
        now = datetime.now()
        deltas = []
        for i, x in enumerate(self.datetimes):
            delta = (x - now).total_seconds()
            deltas.append(delta)
        for i, delta in enumerate(deltas):
            if delta > 0:
                if delta < 5:
                    return True, self.IS_END if i == len(deltas) - 1 else deltas[i + 1]
                else:
                    return False, delta - 0.1
        return False, self.IS_END


class Crontab(BaseSchedule):

    def __init__(self, exp):
        self.exp = exp

    def next_time(self, base_time=None):
        pass


class WeeklyDays(BaseSchedule):
    def __init__(self, days):
        pass

    def next_time(self, base_time=None):
        pass


class MonthlyDays(BaseSchedule):

    def __init__(self, sentence):
        self.sentence = sentence


class Interval(BaseSchedule):

    def __init__(self, hours=0, minutes=0, seconds=0):
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)

    def is_due(self, last_run_at: datetime):
        now = datetime.now()
        delta = now - last_run_at
        if delta >= self.delta:
            return True, delta.seconds
        return False, (self.delta - delta).seconds

    def __str__(self):
        return 'Interval(hours=%s, minutes=%s, seconds=%s)' % (self.hours, self.minutes, self.seconds)

    __repr__ = __str__


class Prompt(BaseSchedule):

    def __init__(self, prompt):
        self.prompt = prompt

    def next_time(self, base_time=None):
        pass

    def is_due(self, base_time: datetime):
        return False

    def __str__(self):
        return self.prompt

    __repr__ = __str__


class ScheduleLoader:
    name = None

    def load(self, obj):
        raise NotImplementedError


class JsonScheduleLoader(ScheduleLoader):

    def __init__(self):
        self._schedules: Dict[str, BaseSchedule] = {}
        for parser_cls in BaseSchedule.__subclasses__():
            self.install_schedules(parser_cls)

    def install_schedules(self, *parsers):
        parsers: List[BaseSchedule]
        for parser in parsers:
            self._schedules[parser.name or parser.__class__.__name__.lower()] = parser

    def uninstall_schedules(self, *parsers):
        parsers: List[BaseSchedule]
        for parser in parsers:
            self._schedules[parser.name or parser.__class__.__name__.lower()] = parser

    def load(self, obj) -> BaseSchedule:
        assert isinstance(obj, dict), 'obj必须是json类型'
        s = self._schedules.get(obj.get('type', None))
        if s is None:
            raise InvalidScheduleConfigType("不支持的计划类型：%s" % obj.get('type'))
        return s


class Schedule:
    json_loader = JsonScheduleLoader()

    def __init__(self):
        self._loaders: Dict[str, ScheduleLoader] = {}
        self.install_loaders(self.json_loader)

    def install_loaders(self, *loaders):
        loaders: List[ScheduleLoader]
        for loader in loaders:
            self._loaders[loader.name or loader.__class__.__name__.lower()] = loader

    def uninstall_loaders(self, *loaders):
        loaders: List[ScheduleLoader]
        for loader in loaders:
            self._loaders[loader.name or loader.__class__.__name__.lower()] = loader

    @classmethod
    def from_json(cls, config) -> 'BaseSchedule':
        return cls.json_loader.load(config)

    def __call__(self, exp: str | BaseSchedule) -> BaseSchedule:
        if isinstance(exp, str):
            return Prompt(exp)
        return exp

    def __str__(self):
        return 'Schedule(loaders=[%s])' % (','.join([x.__class__.__name__ for x in self._loaders.values()]))

    __repr__ = __str__


schedule = Schedule()


TaskMessage = namedtuple('TaskMessage', ('headers', 'properties', 'body'))


class Producer:

    def __init__(self):
        self.queue = queue.Queue()

    def publish(self, body):
        self.queue.put(body)


class Consumer:

    def __init__(self):
        pass

    def consume(self):
        pass


class AMQP:

    Producer = Producer
    Consumer = Consumer

    def __init__(self):
        self.producer = Producer()

    @cached_property
    def create_task_message(self):
        return self.as_task

    def as_task(self, task_id, name, args=None, kwargs=None, eta=None, expires=None, retries=0,
                   callbacks=None, errbacks=None, reply_to=None, time_limit=None, ignore_result=False):

        args = args or ()
        kwargs = kwargs or {}
        headers = {
            'lang': 'py',
            'task': name,
            'id': task_id,
            'eta': eta,
            'expires': expires,
            'retries': retries,
            'timelimit': [time_limit],
            'ignore_result': ignore_result,
        }

        return TaskMessage(
            headers=headers,
            properties={
                'correlation_id': task_id,
                'reply_to': reply_to or '',
            },
            body=(
                args, kwargs, {
                    'callbacks': callbacks,
                    'errbacks': errbacks,
                },
            )
        )

    def send_task_message(self, task):
        self.producer.publish(task)


class Worker:
    Consumer = Consumer

    def __init__(self):
        pass

    def consume(self):
        pass

    def start(self):
        self.consume()


class Thyme:
    SchedulerService = EmbeddedService
    worker_cls = Worker
    amqp_cls = AMQP

    def __init__(self, embedded_service=False):
        self.embedded_service = embedded_service
        self.tasks = {}
        self.schedules = {}
        self.main = None
        self.scheduler_service = self.SchedulerService(self)
        self.amqp = self.amqp_cls()
        self.worker_pools = {}

    @property
    def conf(self):
        return {}

    def send_task(self, name, args, kwargs, **options):
        self.amqp.send_task_message(task=name)

    @cached_property
    def Task(self):
        return Task

    def task(self, *args, name=None, task_cls=None, overridable=False, **kwargs):
        def inner_create_task_cls(func):
            task_name = name or gen_task_name(self, func.__name__, func.__module__)
            base = task_cls or self.Task
            new_task = type(func.__name__, (base,), dict({
                'app': self,
                'name': task_name,
                'run': func,
                '_decorated': True,
                '__doc__': func.__doc__,
                '__module__': func.__module__,
                '__annotations__': func.__annotations__,
                # '__header__': app.type_checker(fun, bound=bind),
                '__wrapped__': func
            }, **kwargs))()
            # for some reason __qualname__ cannot be set in type()
            # so we have to set it here.
            try:
                new_task.__qualname__ = func.__qualname__
            except AttributeError:
                pass

            if task_name in self.tasks and not overridable:
                if overridable:
                    self.tasks[task_name] = new_task
                else:
                    raise ValueError("task defined already")
            else:
                self.tasks[task_name] = new_task
            return new_task

        if len(args) == 1:
            if callable(args[0]):
                return inner_create_task_cls(*args)
            raise TypeError('argument 1 to @task() must be a callable')
        if args:
            raise TypeError(
                '@task() takes exactly 1 argument ({} given)'.format(
                    sum([len(args), len(kwargs)])))
        return inner_create_task_cls

    def schedule(self, exp):
        def inner_create_schedule_cls(func):
            if not isinstance(func, self.Task):
                pass
            self.scheduler_service.scheduler.add_schedule_entry(ScheduleEntry(
                task=func, schedule=schedule(exp), app=self
            ))
            return func
        return inner_create_schedule_cls

    def start(self):
        self.scheduler_service.start()
        if self.embedded_service:
            self.worker_cls


thyme = Thyme()


@thyme.schedule(Interval(seconds=10))
@thyme.task(name=1)
def run1():
    print("hello world")


if __name__ == '__main__':
    thyme.start()
