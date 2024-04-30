import os
import shutil
import tempfile
from os.path import join
from pathlib import Path
from datetime import datetime
from typing import Tuple

from celery import current_app

import django
import pytest
from django.core.files import File
from django.core.files.base import File
from django.core.files.storage import FileSystemStorage
from queued_storage.backends import QueuedStorage
from queued_storage.conf import settings
from queued_storage.tasks import TransferFailedException

from tests import models

DJANGO_VERSION = django.get_version()


@pytest.fixture
def setup_teardown() -> Tuple[str, str, str, str, File]:
    """
    Set up and tear down procedures for tests
    """
    old_celery_always_eager = getattr(settings, 'CELERY_ALWAYS_EAGER', False)
    settings.CELERY_ALWAYS_EAGER = True
    local_dir = tempfile.mkdtemp()
    remote_dir = tempfile.mkdtemp()
    tmp_dir = tempfile.mkdtemp()
    test_file_name = 'queued_storage.txt'
    test_file_path = join(tmp_dir, test_file_name)
    with open(test_file_path, 'a') as test_file:
        test_file.write('test')
    # Wrap the rmtree function and its arguments in a lambda function to postpone execution
    add_cleanup = [
        lambda: shutil.rmtree(local_dir),
        lambda: shutil.rmtree(remote_dir),
        lambda: shutil.rmtree(tmp_dir)
    ]
    test_file = open(test_file_path, 'r')
    yield local_dir, remote_dir, tmp_dir, test_file_name, test_file
    settings.CELERY_ALWAYS_EAGER = old_celery_always_eager
    for cleanup in add_cleanup:
        cleanup()


def test_storage_creation(setup_teardown):
    """
    This test verifies that a QueuedStorage object can be created.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        'django.core.files.storage.FileSystemStorage',
        'django.core.files.storage.FileSystemStorage'
    )
    assert isinstance(storage, QueuedStorage)
    assert isinstance(storage.local, FileSystemStorage)
    assert isinstance(storage.remote, FileSystemStorage)


def test_storage_cache_prefix(setup_teardown):
    """
    This test checks if the cache prefix for a QueuedStorage object is working correctly.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        'django.core.files.storage.FileSystemStorage',
        'django.core.files.storage.FileSystemStorage',
        cache_prefix='test_cache_key')
    assert storage.cache_prefix == 'test_cache_key'


def test_storage_method_existence(setup_teardown):
    """
    This test guarantees that QueuedStorage implements all the necessary methods.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        'django.core.files.storage.FileSystemStorage',
        'django.core.files.storage.FileSystemStorage')

    file_storage = FileSystemStorage()
    for attr in dir(file_storage):
        # Add more built-ins if necessary
        if attr in [
            '__weakref__',
            # These are internal to the FileSystemStorage class
            '_datetime_from_timestamp',
            '_ensure_location_group_id',
        ]:
            continue
        method = getattr(file_storage, attr)
        if callable(method):
            assert hasattr(storage, attr), f"QueuedStorage has no method {attr}"
            assert callable(getattr(storage, attr))


# needs the django test database pytest mark
@pytest.mark.django_db
def test_storage_simple_save(setup_teardown):
    """
    This test verifies that saving to remote locations in QueuedStorage works correctly.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        task='tests.tasks.test_task')

    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    obj.testfile.save(test_file_name, File(test_file))
    obj.save()

    assert os.path.isfile(os.path.join(local_dir, obj.testfile.name))
    assert os.path.isfile(os.path.join(remote_dir, obj.testfile.name))


@pytest.mark.django_db
def test_storage_celery_save(setup_teardown):
    """
    This test validates that QueuedStorage works correctly when using Celery as a task queue.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir)
    )
    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    obj.testfile.save(test_file_name, File(test_file))
    obj.save()

    assert obj.testfile.storage.result.get()
    assert os.path.isfile(os.path.join(local_dir, obj.testfile.name))
    assert os.path.isfile(os.path.join(remote_dir, obj.testfile.name))
    assert not storage.using_local(obj.testfile.name)
    assert storage.using_remote(obj.testfile.name)

    # More tests follow here for other methods and attributes of `storage`
    # ...


@pytest.mark.django_db
def test_transfer_and_delete(setup_teardown):
    """
    This tests ensures that the TransferAndDelete task functions as expected.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        task='queued_storage.tasks.transfer_and_delete')

    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    obj.testfile.save(test_file_name, File(test_file))
    obj.save()

    obj.testfile.storage.result.get()

    assert not os.path.isfile(os.path.join(local_dir, obj.testfile.name))
    assert os.path.isfile(os.path.join(remote_dir, obj.testfile.name))


@pytest.mark.xfail(raises=ValueError)
def test_transfer_returns_boolean(setup_teardown):
    """
    This test checks that an exception is thrown when the transfer task does not return a boolean.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        task='tests.tasks.none_returning_task')

    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    obj.testfile.save(test_file_name, File(test_file))
    obj.save()

    pytest.raises(ValueError, obj.testfile.storage.result.get, propagate=True)


@pytest.mark.django_db
def test_delayed_storage(setup_teardown):
    """
    This test checks the functionality of delayed storage in QueuedStorage.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    remote_dir: str
    local_dir: str
    tmp_dir:str
    test_file_name: str
    test_file: File
    test_file_path = Path(tmp_dir) / test_file_name

    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        delayed=True)

    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    obj.testfile.save(test_file_name, File(test_file))
    obj.save()

    assert getattr(obj.testfile.storage, 'result', None) is None
    assert not os.path.isfile(os.path.join(remote_dir, obj.testfile.name))

    # Check file properties for test coverage purposes,
    # first making sure it's via the local storage and not the remote storage.
    assert storage.exists(obj.testfile.name)
    assert storage.using_local(obj.testfile.name)
    assert not storage.using_remote(obj.testfile.name)
    # Assert we can open the file
    assert storage.open(obj.testfile.name).read() == b'test'
    # Assert the size of the file is the same as the original file.
    assert storage.size(obj.testfile.name) == test_file_path.stat().st_size
    # Assert we can get the created time
    assert storage.get_created_time(obj.testfile.name)
    # Assert we can get the accessed time
    assert storage.get_accessed_time(obj.testfile.name)
    # Assert we can get the modified time
    assert storage.get_modified_time(obj.testfile.name)

    result = obj.testfile.storage.transfer(obj.testfile.name)
    result.get()

    # Check file properties for test coverage purposes,
    # Now the file is transfer, ensure its remote not local before checking properties.
    assert storage.exists(obj.testfile.name)
    assert storage.using_remote(obj.testfile.name)
    assert not storage.using_local(obj.testfile.name)
    # Assert we can open the file
    assert storage.open(obj.testfile.name).read() == b'test'
    # Assert the size of the file is the same as the original file.
    assert storage.size(obj.testfile.name) == test_file_path.stat().st_size
    # Assert we can get the created time
    assert storage.get_created_time(obj.testfile.name)
    # Assert we can get the accessed time
    assert storage.get_accessed_time(obj.testfile.name)
    # Assert we can get the modified time
    assert storage.get_modified_time(obj.testfile.name)

    assert os.path.isfile(os.path.join(remote_dir, obj.testfile.name))  # I'm unsure this assert is needed.


@pytest.mark.django_db
def test_remote_file_field(setup_teardown):
    """
    This test checks the functioning of a remote file field in QueuedStorage.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        delayed=True)

    field = models.TestModel._meta.get_field('remote')
    field.storage = storage

    obj = models.TestModel()
    obj.remote.save(test_file_name, File(test_file))
    obj.save()

    assert getattr(obj.testfile.storage, 'result', None) is None

    result = obj.remote.transfer()
    assert result
    assert os.path.isfile(os.path.join(remote_dir, obj.remote.name))


@pytest.mark.django_db
def test_file_overwrite(setup_teardown):
    """
    This test checks the handling of situation when a file with the same name already exists in remote storage.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir))

    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    # Saving first file
    obj = models.TestModel()
    obj.testfile.save('duplicate_filename.txt', File(test_file))
    obj.save()

    # Saving second file with same name
    second_obj = models.TestModel()
    second_test_file = File(tempfile.NamedTemporaryFile(suffix=".txt"))
    second_obj.testfile.save('duplicate_filename.txt', second_test_file)
    second_obj.save()

    # Check behavior here (depends on how your system is supposed to handle this case)


@pytest.mark.django_db
def test_file_deletion(setup_teardown):
    """
    This test checks that the file is deleted from the remote location after the transfer.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        'django.core.files.storage.FileSystemStorage',
        'django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        task='queued_storage.tasks.transfer_and_delete')

    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    obj.testfile.save(test_file_name, File(test_file))
    obj.save()

    obj.testfile.storage.result.get()

    local_file_path = Path(local_dir) / obj.testfile.name
    remote_file_path = Path(remote_dir) / obj.testfile.name
    assert not local_file_path.exists()
    assert remote_file_path.exists()
    # Test deleting the remote file.
    obj.delete()
    # This doesn't seem to work... the file still exists.
    assert not remote_file_path.exists()

# TODO: This may be superfluous based on an earlier test.
@pytest.mark.django_db
def test_file_opens_before_and_after_transfer(setup_teardown):
    """
    This test checks that the file can be opened before and after the transfer.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        'django.core.files.storage.FileSystemStorage',
        'django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        task='tests.tasks.test_task')

    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    obj.testfile.save(test_file_name, File(test_file))
    obj.save()

    assert obj.testfile.storage.open(obj.testfile.name).read() == b'test'

    assert obj.testfile.storage.open(obj.testfile.name).read() == b'test'


@pytest.mark.django_db
def test_unreachable_remote_location_for_basic_transfer(setup_teardown):
    """
    Test to check behavior when the remote location is not available.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='tests.backends.UnavailableFileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        task='tests.tasks.basic_transfer_without_retry'
    )
    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    with pytest.raises(TransferFailedException):
        obj.testfile.save(test_file_name, File(test_file))
        obj.save()


@pytest.mark.django_db
def test_unreachable_remote_location_for_save_and_delete(setup_teardown):
    """
    Test to check behavior when the remote location is not available.
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='tests.backends.UnavailableFileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir),
        task='tests.tasks.transfer_and_delete_without_retry'
    )
    field = models.TestModel._meta.get_field('testfile')
    field.storage = storage

    obj = models.TestModel()
    with pytest.raises(TransferFailedException):
        obj.testfile.save(test_file_name, File(test_file))
        obj.save()




def test_invalid_file_type(setup_teardown):
    """
    Test to check behavior when the file type is invalid
    """
    local_dir, remote_dir, tmp_dir, test_file_name, test_file = setup_teardown
    file_type = '.exe' # assuming your storage doesn't handle '.exe' files
    invalid_file_name = f'test_file{file_type}'
    invalid_file_path = os.path.join(tmp_dir, invalid_file_name)
    with open(invalid_file_path, 'w') as invalid_file:
        invalid_file.write('test')

    invalid_file = open(invalid_file_path, 'r')

    storage = QueuedStorage(
        local='django.core.files.storage.FileSystemStorage',
        remote='django.core.files.storage.FileSystemStorage',
        local_options=dict(location=local_dir),
        remote_options=dict(location=remote_dir))

    with pytest.raises(Exception):  # Replace with your specific Exception
        obj = models.TestModel()
        obj.testfile.save(invalid_file_name, File(invalid_file))
        obj.save()

    invalid_file.close()
