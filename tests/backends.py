from django.core.files.storage import FileSystemStorage


class UnavailableFileSystemStorage(FileSystemStorage):
    """
    A FileSystemStorage that raises an exception when trying to save a file.
    """
    def save(self, name, content, max_length=None):
        raise Exception("This storage is unavailable.")