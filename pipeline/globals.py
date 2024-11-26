
from dataclasses import dataclass
import os
import redis

from . import celeryconfig

from celery import Celery

RHOST = os.environ.get("REDIS_URL", "localhost")
PORT = os.environ.get("REDIS_PORT", "6379")

REDIS_URL = f"{RHOST}:{PORT}"

celeryconfig.broker_url = f'redis://{REDIS_URL}/0'
celeryconfig.result_backend = f'redis://{REDIS_URL}/0'


def make_celery():
    celery = Celery(
        "models_pipeline_app",
        config_source=celeryconfig
    )

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            # with app.app_context():
            return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery


@dataclass(init=False)
class InitServices:
    redis_db: None
    models_folder: str
    celery: None

    def __init__(self):
        self.celery = make_celery()
        self.models_folder = os.environ.get('MODELS_FOLDER', 'models')
        self.models_folder_path = os.path.join(os.getcwd(), self.models_folder)
        self.data_folder = os.environ.get('DATA_FOLDER', 'data')
        self.data_folder_path = os.path.join(os.getcwd(), self.data_folder)
        self.redis_db = redis.StrictRedis(host=RHOST, port=PORT, db=0)
        # self.redis_db.flushall()

        # self.redis_db.save()
        # print("*")

    def services(self):
        return self.redis_db, self.celery


GLOBALS = InitServices()
