from functools import partial
from typing import Callable
from django.core.cache import cache

from celery import shared_task
from celery.app.task import Task
from celery.utils.log import get_task_logger


from django.conf import settings
from django.core.files.storage import Storage
from .signals import file_transferred
from .utils import import_attribute


MODULE_NAME = __name__
logger = get_task_logger(name=MODULE_NAME)


class TransferFailedException(Exception):
    """Custom exception to throw when the transfer failed.

     Used to signal we want celery to retry the task."""
    pass


def transfer_func_caller(
        name: str,
        cache_key: str,
        local_path: str,
        remote_path: str,
        local_options: dict,
        remote_options: dict,
        *,
        transfer_func: Callable[[Storage, str, Storage], bool]
):
    """
    Calls the specified transfer_func passing in the local and remote storage backends and path string as given
    with the parameters.

    :param name: name of the file to transfer
    :type name: str
    :param local_path: local storage class to transfer from
    :type local_path: str
    :param local_options: options of the local storage class
    :type local_options: dict
    :param remote_path: remote storage class to transfer to
    :type remote_path: str
    :param remote_options: options of the remote storage class
    :type remote_options: dict
    :param cache_key: cache key to set after a successful transfer
    :type cache_key: str
    :rtype: task result
    """
    # TODO: This could be substituted for context managers.
    local: Storage = import_attribute(local_path)(**local_options)
    remote: Storage = import_attribute(remote_path)(**remote_options)

    transfer_result = transfer_func(local, name, remote)

    # Todo: This can be simplified since the TransferFailedException is now being caught by the task decorator
    # TODO: provide a nicer sender name that reflects using custom transfer_func instead of the default.
    if transfer_result is True:
        cache.set(cache_key, True)
        file_transferred.send(
            sender=f"queued_storage.tasks.transfer_func_caller", name=name, local=local, remote=remote
        )
    elif transfer_result is False:
        raise TransferFailedException()
    else:
        raise ValueError(f"Task 'queued_storage.tasks.transfer_func_caller' did not return True/False but {transfer_result}")
    return transfer_result


def _transfer_file(local: Storage, name: str, remote: Storage):
    """
    Transfers the file with the given name from the local storage backend to the remote storage backend.
    """
    try:
        remote.save(name, local.open(name))
        return True
    except Exception as e:
        logger.error(f"Unable to save '{name}' to remote storage. About to retry.")
        logger.exception(e)
        return False


def _transfer_file_and_delete(local: Storage, name: str, remote: Storage):
    """
    Transfers the file with the given name from the local storage backend to the remote storage backend.
    """
    try:
        remote.save(name, local.open(name))
        # TODO: This should probably make sure the file was successfully transferred before deleting it.
        local.delete(name)
        return True
    except Exception as e:
        logger.error(f"Unable to save '{name}' to remote storage. About to retry.")
        logger.exception(e)
        return False


@shared_task(
    autoretry_for=(TransferFailedException,),
    default_retry_delay=settings.QUEUED_STORAGE_RETRY_DELAY,
    retry_kwargs={'max_retries': settings.QUEUED_STORAGE_RETRIES}
)
def transfer(name: str, cache_key: str, local_path: str, remote_path: str, local_options: dict, remote_options: dict):
    """
    Calls the transfer method with the local and remote storage backends as given with the parameters.
    """
    return transfer_func_caller(
        name=name,
        cache_key=cache_key,
        local_path=local_path,
        remote_path=remote_path,
        local_options=local_options,
        remote_options=remote_options,
        transfer_func=_transfer_file
    )


# TODO: setup equivalent functions to transfer_func_caller and simple_transfer for transfer_and_delete
@shared_task(
    autoretry_for=(TransferFailedException,),
    default_retry_delay=settings.QUEUED_STORAGE_RETRY_DELAY,
    retry_kwargs={'max_retries': settings.QUEUED_STORAGE_RETRIES}
)
def transfer_and_delete(name: str, cache_key: str, local_path: str, remote_path: str, local_options: dict, remote_options: dict):
    """
    Calls the transfer method with the local and remote storage backends as given with the parameters.
    """
    return transfer_func_caller(
        name=name,
        cache_key=cache_key,
        local_path=local_path,
        remote_path=remote_path,
        local_options=local_options,
        remote_options=remote_options,
        transfer_func=_transfer_file_and_delete
    )


# region Original Code

class Transfer(Task):
    """
    The default task. Transfers a file to a remote location.
    The actual transfer is implemented in the remote backend.

    To use a different task, pass it into the backend:

    .. code-block:: python

        from queued_storage.backends import QueuedS3BotoStorage

        s3_delete_storage = QueuedS3BotoStorage(
            task='queued_storage.tasks.TransferAndDelete')

        # later, in model definition:
        image = models.ImageField(storage=s3_delete_storage)


    The result should be ``True`` if the transfer was successful,
    or ``False`` if unsuccessful. In the latter case the task will be
    retried.

    You can subclass the :class:`~queued_storage.tasks.Transfer` class
    to customize the behaviour, to do something like this:

    .. code-block:: python

        from queued_storage.tasks import Transfer

        class TransferAndNotify(Transfer):
            def transfer(self, *args, **kwargs):
                result = super(TransferAndNotify, self).transfer(*args, **kwargs)
                if result:
                    # call the (imaginary) notify function with the result
                    notify(result)
                return result

    """
    #: The number of retries if unsuccessful (default: see
    #: :attr:`~queued_storage.conf.settings.QUEUED_STORAGE_RETRIES`)
    max_retries = settings.QUEUED_STORAGE_RETRIES

    #: The delay between each retry in seconds (default: see
    #: :attr:`~queued_storage.conf.settings.QUEUED_STORAGE_RETRY_DELAY`)
    default_retry_delay = settings.QUEUED_STORAGE_RETRY_DELAY

    def run(self, name, cache_key,
            local_path, remote_path,
            local_options, remote_options, **kwargs):
        """
        The main work horse of the transfer task. Calls the transfer
        method with the local and remote storage backends as given
        with the parameters.

        :param name: name of the file to transfer
        :type name: str
        :param local_path: local storage class to transfer from
        :type local_path: str
        :param local_options: options of the local storage class
        :type local_options: dict
        :param remote_path: remote storage class to transfer to
        :type remote_path: str
        :param remote_options: options of the remote storage class
        :type remote_options: dict
        :param cache_key: cache key to set after a successful transfer
        :type cache_key: str
        :rtype: task result
        """
        local = import_attribute(local_path)(**local_options)
        remote = import_attribute(remote_path)(**remote_options)
        result = self.transfer(name, local, remote, **kwargs)

        if result is True:
            cache.set(cache_key, True)
            file_transferred.send(sender=self.__class__,
                                  name=name, local=local, remote=remote)
        elif result is False:
            args = [name, cache_key, local_path,
                    remote_path, local_options, remote_options]
            self.retry(args=args, kwargs=kwargs)
        else:
            raise ValueError("Task '%s' did not return True/False but %s" %
                             (self.__class__, result))
        return result

    def transfer(self, name, local, remote, **kwargs):
        """
        Transfers the file with the given name from the local to the remote
        storage backend.

        :param name: The name of the file to transfer
        :param local: The local storage backend instance
        :param remote: The remote storage backend instance
        :returns: `True` when the transfer succeeded, `False` if not. Retries
                  the task when returning `False`
        :rtype: bool
        """
        try:
            remote.save(name, local.open(name))
            return True
        except Exception as e:
            logger.error("Unable to save '%s' to remote storage. "
                         "About to retry." % name)
            logger.exception(e)
            return False


class TransferAndDelete(Transfer):
    """
    A :class:`~queued_storage.tasks.Transfer` subclass which deletes the
    file with the given name using the local storage if the transfer
    was successful.
    """
    def transfer(self, name, local, remote, **kwargs):
        result = super(TransferAndDelete, self).transfer(name, local,
                                                         remote, **kwargs)
        if result:
            local.delete(name)
        return result

# endregion