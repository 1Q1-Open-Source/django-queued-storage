from queued_storage.tasks import (
    Transfer,
    transfer_func_caller,
    _transfer_file,
    TransferFailedException
)
from queued_storage.utils import import_attribute
from celery import shared_task
from queued_storage.conf import settings


from .models import TestModel


def test_task(name, cache_key,
              local_path, remote_path,
              local_options, remote_options):
    local = import_attribute(local_path)(**local_options)
    remote = import_attribute(remote_path)(**remote_options)
    remote.save(name, local.open(name))


def delay(*args, **kwargs):
    test_task(*args, **kwargs)

test_task.delay = delay


class NoneReturningTask(Transfer):
    def transfer(self, *args, **kwargs):
        return None


@shared_task(
    autoretry_for=(TransferFailedException,),
    default_retry_delay=settings.QUEUED_STORAGE_RETRY_DELAY,
    retry_kwargs={'max_retries': settings.QUEUED_STORAGE_RETRIES}
)
def none_returning_task(
        name,
        cache_key,
        local_path,
        remote_path,
        local_options,
        remote_options
):
    return transfer_func_caller(
        name=name,
        cache_key=cache_key,
        local_path=local_path,
        remote_path=remote_path,
        local_options=local_options,
        remote_options=remote_options,
        transfer_func=lambda _local, _name, _remote: None,
    )


class RetryingTask(Transfer):
    def transfer(self, *args, **kwargs):
        if TestModel.retried:
            return True
        else:
            TestModel.retried = True
            return False


def _retrying_task(*args, **kwargs):
    if TestModel.retried:
        return True
    else:
        TestModel.retried = True
        return False


@shared_task(
    autoretry_for=(TransferFailedException,),
    default_retry_delay=settings.QUEUED_STORAGE_RETRY_DELAY,
    retry_kwargs={'max_retries': settings.QUEUED_STORAGE_RETRIES}
)
def retrying_task(
        name,
        cache_key,
        local_path,
        remote_path,
        local_options,
        remote_options
):
    return transfer_func_caller(
        name=name,
        cache_key=cache_key,
        local_path=local_path,
        remote_path=remote_path,
        local_options=local_options,
        remote_options=remote_options,
        transfer_func=_retrying_task,
    )


@shared_task
def basic_transfer_without_retry(name, cache_key, local_path, remote_path, local_options, remote_options):
    return transfer_func_caller(
        name=name,
        cache_key=cache_key,
        local_path=local_path,
        remote_path=remote_path,
        local_options=local_options,
        remote_options=remote_options,
        transfer_func=_transfer_file
    )


@shared_task
def transfer_and_delete_without_retry(name, cache_key, local_path, remote_path, local_options, remote_options):
    return transfer_func_caller(
        name=name,
        cache_key=cache_key,
        local_path=local_path,
        remote_path=remote_path,
        local_options=local_options,
        remote_options=remote_options,
        transfer_func=_transfer_file
    )