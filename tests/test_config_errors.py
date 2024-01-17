import pytest
from django.core.exceptions import ImproperlyConfigured

from queued_storage.backends import QueuedStorage
# import the setup_teardown fixture


def test_non_existent_storage_backend():
    """
    This test checks that an error is raised when trying to import a storage class that doesnt exist.
    """
    with pytest.raises(ImproperlyConfigured):
        storage = QueuedStorage(
            local='django.core.files.storage.FileSystemStorage',
            remote='django.core.files.storage.NonExistentStorage'
        )


def test_incorrect_type_storage_backend():
    """
    This test checks that an error is raised when trying to import a storage class that doesnt exist.
    """
    with pytest.raises(ImproperlyConfigured):
        # noinspection PyTypeChecker
        storage = QueuedStorage(
            local=120,
            remote=120
        )


def test_missing_storage_backend():
    """
    This test checks that an error is raised when trying to import a storage class that doesnt exist.
    """
    with pytest.raises(ImproperlyConfigured):
        # noinspection PyTypeChecker
        storage = QueuedStorage(
            local=None,
            remote=None,
        )
