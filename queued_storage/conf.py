from django.conf import settings
from appconf import AppConf


class QueuedStorageConf(AppConf):
    RETRIES = 5
    RETRY_DELAY = 60
    CACHE_PREFIX = 'queued_storage'

    class Meta:
        prefix = 'queued_storage'