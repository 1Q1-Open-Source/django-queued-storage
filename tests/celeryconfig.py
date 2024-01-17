BROKER_TRANSPORT = "memory"

CELERY_ALWAYS_EAGER = True
CELERY_IGNORE_RESULT = True
CELERY_IMPORTS = [
    'queued_storage.tasks',
]
