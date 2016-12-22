import six
import sys
import uuid

from kazoo.retry import (
    ForceRetryError,
    KazooRetry,
    RetryFailedError
)

from kazoo.exceptions import (
    CancelledError,
    KazooException,
    LockTimeout,
    NoNodeError,
    NotEmptyError
)


class EntityLock(object):
    """Kazoo Lock

    Example usage with a :class:`~kazoo.client.KazooClient` instance:

    .. code-block:: python

        zk = KazooClient()
        lock = zk.Lock("/lockpath", "my-identifier")
        with lock:  # blocks waiting for lock acquisition
            # do something with the lock

    Note: This lock is not *re-entrant*. Repeated calls after already
    acquired will block.

    """
    _NODE_NAME = '__lock__'

    def __init__(self, zk_client, project, keys, identifier=None):
        """Create a Kazoo lock.

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The lock path to use.
        :param identifier: Name to use for this lock contender. This
                           can be useful for querying to see who the
                           current lock contenders are.

        """
        paths = []
        for key in keys:
            if key.name_space():
                namespace = key.name_space()
            else:
                namespace = ':default'
            element = key.path().element_list()[0]
            group = element.type()
            if element.has_id():
                group += ':' + str(element.id())
            else:
                group += '::' + element.name()
            project_lock = '/appscale/apps/{}/locks/{}'.format(
                project, namespace)
            paths.append(project_lock + '/' + group)

        self.zk_client = zk_client
        self.paths = paths
        self.project = project

        # some data is written to the node. this can be queried via
        # contenders() to see who is contending for the lock
        self.data = str(identifier or "")

        self.wake_event = zk_client.handler.event_object()

        # props to Netflix Curator for this trick. It is possible for our
        # create request to succeed on the server, but for a failure to
        # prevent us from getting back the full path name. We prefix our
        # lock name with a uuid and can check for its presence on retry.
        self.prefix = uuid.uuid4().hex + self._NODE_NAME

        self.create_paths = [path + '/' + self.prefix for path in self.paths]

        self.create_tried = False
        self.is_acquired = False
        self.assured_path = False
        self.cancelled = False
        self._retry = KazooRetry(max_tries=None,
                                 sleep_func=zk_client.handler.sleep_func)
        self._lock = zk_client.handler.lock_object()

    def _ensure_path(self):
        for path in self.paths:
            self.zk_client.ensure_path(path)
        self.assured_path = True

    def cancel(self):
        """Cancel a pending lock acquire."""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True, timeout=None):
        """
        Acquire the lock. By defaults blocks and waits forever.

        :param blocking: Block until lock is obtained or return immediately.
        :type blocking: bool
        :param timeout: Don't wait forever to acquire the lock.
        :type timeout: float or None

        :returns: Was the lock acquired?
        :rtype: bool

        :raises: :exc:`~kazoo.exceptions.LockTimeout` if the lock
                 wasn't acquired within `timeout` seconds.

        .. versionadded:: 1.1
            The timeout option.
        """

        def _acquire_lock():
            got_it = self._lock.acquire(False)
            if not got_it:
                raise ForceRetryError()
            return True

        retry = self._retry.copy()
        retry.deadline = timeout

        # Ensure we are locked so that we avoid multiple threads in
        # this acquistion routine at the same time...
        locked = self._lock.acquire(False)
        if not locked and not blocking:
            return False
        if not locked:
            # Lock acquire doesn't take a timeout, so simulate it...
            try:
                locked = retry(_acquire_lock)
            except RetryFailedError:
                return False
        already_acquired = self.is_acquired
        try:
            gotten = False
            try:
                gotten = retry(self._inner_acquire,
                               blocking=blocking, timeout=timeout)
            except RetryFailedError:
                if not already_acquired:
                    self._best_effort_cleanup()
            except KazooException:
                # if we did ultimately fail, attempt to clean up
                exc_info = sys.exc_info()
                if not already_acquired:
                    self._best_effort_cleanup()
                    self.cancelled = False
                six.reraise(exc_info[0], exc_info[1], exc_info[2])
            if gotten:
                self.is_acquired = gotten
            if not gotten and not already_acquired:
                self._delete_nodes(self.nodes)
            return gotten
        finally:
            self._lock.release()

    def _watch_session(self, state):
        self.wake_event.set()
        return True

    def _resolve_deadlocks(self, children_list):
        current_txid = int(self.data)
        for index, children in enumerate(children_list):
            our_index = children.index(self.nodes[index])
            if self.acquired_lock(children, our_index):
                continue

            # Get transaction IDs for earlier contenders.
            for child in children[:our_index - 1]:
                try:
                    data, _ = self.zk_client.get(
                        self.paths[index] + '/' + child)
                except NoNodeError:
                    continue

                # If data is not set, it doesn't belong to a cross-group
                # transaction.
                if not data:
                    continue

                child_txid = int(data)
                # As an arbitrary rule, require later transactions to
                # resolve deadlocks.
                if current_txid > child_txid:
                    # TODO: Implement a more graceful deadlock detection.
                    self.zk_client.retry(self._delete_nodes(self.nodes))
                    raise ForceRetryError()

    def _inner_acquire(self, blocking, timeout):

        # wait until it's our chance to get it..
        if self.is_acquired:
            if not blocking:
                return False
            raise ForceRetryError()

        # make sure our election parent node exists
        if not self.assured_path:
            self._ensure_path()

        nodes = [None for _ in self.paths]
        if self.create_tried:
            nodes = self._find_nodes()
        else:
            self.create_tried = True

        for index, node in enumerate(nodes):
            if node is not None:
                continue

            for _ in range(5):
                try:
                    node = self.zk_client.create(
                        self.create_paths[index], self.data, ephemeral=True,
                        sequence=True)
                    break
                except NoNodeError:
                    self.zk_client.ensure_path(self.paths[index])
                    continue

            # strip off path to node
            node = node[len(self.paths[index]) + 1:]
            nodes[index] = node

        self.nodes = nodes

        while True:
            self.wake_event.clear()

            # bail out with an exception if cancellation has been requested
            if self.cancelled:
                raise CancelledError()

            children_list = self._get_sorted_children()

            predecessors = []
            for index, children in enumerate(children_list):
                try:
                    our_index = children.index(nodes[index])
                except ValueError:  # pragma: nocover
                    # somehow we aren't in the children -- probably we are
                    # recovering from a session failure and our ephemeral
                    # node was removed
                    raise ForceRetryError()

                if not self.acquired_lock(children, our_index):
                    predecessors.append(
                        self.paths[index] + "/" + children[our_index - 1])

            if not predecessors:
                return True

            if not blocking:
                return False

            if len(nodes) > 1:
                self._resolve_deadlocks(children_list)

            # otherwise we are in the mix. watch predecessor and bide our time
            # TODO: Listen for all at the same time.
            for index, predecessor in enumerate(predecessors):
                self.zk_client.add_listener(self._watch_session)
                try:
                    if self.zk_client.exists(predecessor, self._watch_predecessor):
                        self.wake_event.wait(timeout)
                        if not self.wake_event.isSet():
                            error = 'Failed to acquire lock on {} after {} '\
                              'seconds'.format(self.paths, timeout * (index + 1))
                            raise LockTimeout(error)
                finally:
                    self.zk_client.remove_listener(self._watch_session)

    def acquired_lock(self, children, index):
        return index == 0

    def _watch_predecessor(self, event):
        self.wake_event.set()

    def _get_sorted_children(self):
        children = [self.zk_client.get_children(path) for path in self.paths]
        children = []
        for path in self.paths:
            try:
                children.append(self.zk_client.get_children(path))
            except NoNodeError:
                children.append([])

        # can't just sort directly: the node names are prefixed by uuids
        lockname = self._NODE_NAME
        for child_list in children:
            child_list.sort(key=lambda c: c[c.find(lockname) + len(lockname):])
        return children

    def _find_nodes(self):
        nodes = []
        for path in self.paths:
            try:
                children = self.zk_client.get_children(path)
            except NoNodeError:
                children = []

            node = None
            for child in children:
                if child.startswith(self.prefix):
                    node = child
            nodes.append(node)
        return nodes

    def _delete_nodes(self, nodes):
        for index, node in enumerate(nodes):
            if node is None:
                continue
            self.zk_client.delete(self.paths[index] + "/" + node)

    def _best_effort_cleanup(self):
        try:
            nodes = self._find_nodes()
            self._delete_nodes(nodes)
        except KazooException:  # pragma: nocover
            pass

    def release(self):
        """Release the lock immediately."""
        self.zk_client.retry(self._inner_release)
        # Try to clean up the group lock path.
        for path in self.paths:
            try:
                self.zk_client.delete(path)
            except NotEmptyError:
                pass
        return

    def _inner_release(self):
        if not self.is_acquired:
            return False

        try:
            self._delete_nodes(self.nodes)
        except NoNodeError:  # pragma: nocover
            pass

        self.is_acquired = False
        self.nodes = [None for _ in self.paths]
        return True

    def __enter__(self):
        self.acquire(timeout=10)

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
