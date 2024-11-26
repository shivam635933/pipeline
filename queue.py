import json
from .globals import GLOBALS

redis_db, celery = GLOBALS.services()


class Queue:
    def __init__(self, id, keep=False):
        self.id = id
        self._qid = f'Queue-{id}'
        self.keep = keep
        # if not redis_db.llen(f"Queue-${id}"):
        #     redis_db.hset("Queue",id, pickle.dumps([]))
        self.cursor_index = -1

    def get(self, index=-1):
        if index !=-1 and self.len():
            return json.loads(redis_db.lindex(self._qid, index))
        return json.loads(redis_db.lpop(self._qid))
        
    def set(self, ob, index=-1):
        if index !=-1 and self.len():
            redis_db.lset(self._qid, json.dumps(ob))
            return
        redis_db.rpush(self._qid, json.dumps(ob))

    def len(self):
        return redis_db.llen(self._qid)

    def next(self, keep=False):
        if not self.len() or self.len() == self.cursor_index + 1:
            return None

        if self.keep or keep:
            self.cursor_index+=1
            return self.get(index=self.cursor_index)
        return self.get()

    def reset_index(self):
        self.cursor_index=-1

    def filter(self, **kwargs):
        for ind in range(self.len()):
            if self.keep:
                i = self.get(ind)
            else:
                i = self.get()
            if kwargs:
                if all( i.get(k) == v for k,v in kwargs.items() ):
                    yield i
            else:
                yield i

    def add(self, obj):
        # ob = self.get()
        # obj["__id"] = str(uuid4())
        # obj["result_sent"] = False
        # ob.append(obj)
        self.set(obj)

    def update(self, obj, _id=None,index=None):
        # ob = self.get()
        if index:
            if self.len() < index-1:
                return False
        else:
            for ind in range(self.len()):
                obz = self.get(ind)
                if obz["__id"] == _id:
                    self.set(obj, ind)
                    break
    
    def update_by_attr(self, obj, **kwargs):
        indx = 0
        for i in self.filter():
            if not kwargs or all( i.get(k) == v for k,v in kwargs.items() ):
                self.update(obj, index=indx)
            indx+=1
    
    def clear(self):
        redis_db.delete(self._qid)
