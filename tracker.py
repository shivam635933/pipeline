import json
from .globals import GLOBALS

redis_db, celery = GLOBALS.services()


class Tracker:
    def __init__(self, pipeline, task_list):
        self.name = f"__{pipeline}__status_track"
        self._task_list = task_list

    def setTrack(self, id):
        for ts in self._task_list:
            self.updateStatus(id, task_id=ts)

    def updateStatus(self, id, task_id, processing=False, done=False):
        id = f"{id}__{task_id}"

        if processing:
            # print("Status updated processing"+id)
            redis_db.hset(self.name, id, 0)
        elif done:
            # print("Status updated done"+id)
            redis_db.hset(self.name, id, 1)
        else:
            redis_db.hset(self.name, id, -1)
        # self.updateStatusTrack()
        return redis_db.hget(self.name, id)

    def get(self, id, task_id):
        id = f"{id}__{task_id}"
        if redis_db.hexists(self.name, id):
            return redis_db.hget(self.name, id)
        return self.updateStatus(id, task_id=task_id)

    def getTaskStatus(self, id):
        for ts in self._task_list:
            # print(f"Status :: {ts}, {self.get(id, ts)}")
            if self.get(id, ts) in (-1, 0):
                return False
        return True
