from .utils import MetaClass
from .globals import GLOBALS
from .queue import Queue
from daveservice.redis_model import Model
import logging
import time
from os import environ
from .tracker import Tracker


redis_db, celery = GLOBALS.services()


LOG_FILE = environ.get("LOG_FILE", "outputs.log")
logger = logging.getLogger(__name__)
# logger.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(LOG_FILE)
c_handler.setLevel(logging.NOTSET)
# f_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
# _format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
_format = logging.Formatter(
    '[%(asctime)s] p%(process)s {%(module)s:%(filename)10s:%(lineno)d} %(levelname)s - %(message)s', '%m-%d %H:%M:%S')
c_handler.setFormatter(_format)
f_handler.setFormatter(_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


def set_logger(lg):
    if isinstance(lg, (logging.Handler, logging.StreamHandler, logging.FileHandler)):
        logger = lg
    return logger


def get_logger():
    return logger


class Pipeline(metaclass=MetaClass):

    @staticmethod
    def register_pipeline(name: str, pipeline: object, celery_queue_name="low_priority"):
        m = Model("pipeline")
        m.delete(name)
        pipeline = {**pipeline, **{"pipeline_id": name,
                                   "name": name, "celery_queue_name": celery_queue_name}}
        po = m.post(pipeline)
        return Pipeline(name, json_=po, celery_queue_name=celery_queue_name)

    @staticmethod
    def get_pipeline(name: str):
        m = Model("pipeline")
        po = m.get(name)
        if not po:
            return None

        return Pipeline(name, json_=po)

    def __init__(self, name, json_=None, pipeline=None, celery_queue_name=None, **kwargs):
        self.id = name
        self.name = json_.get("name", name)
        self._queue_name = json_.get(
            "celery_queue_name", celery_queue_name) or "low_priority"
        self.pipeline_obj = json_
        self.action = json_.get("action", None)
        # self.queue_id = queue_id if queue_id else name
        # self.queue = Queue(self.queue_id, False)
        self.pipeline = []
        self._pipeline = pipeline

        # import pdb; pdb.set_trace()
        if pipeline:
            self._traker = pipeline._traker
        else:
            self._traker = Tracker(name, [])

        tasks = json_.pop("tasks", [])
        if self.action == "tasks":
            self.setup_task(tasks)
        elif self.action == "pipeline":
            self.setup_pipeline(json_.pop("pipeline", None))
        else:
            tasks.append(json_)
            self.setup_task(tasks)

    def get_task(self, name, ele):
        self._traker._task_list.append(name)
        return Task(name, json_=ele, parent_=self, pipeline=self._pipeline or self)

    def setup_task(self, tasks):
        if not tasks:
            return False

        tasks = tasks or []
        for ind, ele in enumerate(tasks):
            id = f"{self.name}_task_{ind}"
            tsk = self.get_task(id, ele)
            self.pipeline.append(tsk)

    def setup_pipeline(self, tasks):
        if not tasks:
            return False

        tasks = tasks or []
        for ind, ele in enumerate(tasks):
            id = f"{self.name}_task_{ind}"
            tsk = self.get_task(ele.get("name", id), ele)
            self.pipeline.append(tsk)
            if len(self.pipeline) > 1:
                self.pipeline[-1].prev = self.pipeline[-2]
                self.pipeline[-2].next = tsk

    def add_result(self, auth_id=None, result=None):
        id = f'_{auth_id}' if auth_id else ""
        id = f"{self.name}{id}"
        # print(result)
        q = Queue(id)
        q.add(result)

    def read_results(self, auth_id=None):
        id = f'_{auth_id}' if auth_id else ""
        id = f"{self.name}{id}"

        q = Queue(id)
        return list(q.filter())

    def run(self, auth_id=None, *args, **kwargs):
        # print("Initiating the pipeline run")
        if self._pipeline:
            return _run(self.name, auth_id=auth_id, _ref=self, *args, **kwargs)
        else:
            self._traker.setTrack(auth_id)
            _run.apply_async(queue=self._queue_name, args=(
                self.name, auth_id), kwargs=kwargs)

            # _run(self.name, auth_id=auth_id, _ref=self, *args, **kwargs)


def pip_obj(name, t=5):
    z = Pipeline.get_pipeline(name=name)
    if z or t == 0:
        return z
    time.sleep(0.5)
    return pip_obj(name=name, t=t-1)


@celery.task(name="execute_task")
def _run(name, auth_id=None, _ref=None, *args, **kwargs):
    logger.info("Pipeline trying to run %s", name)
    p = _ref or pip_obj(name)

    if p._pipeline:
        logger.info("Getting pipleine object %s %s", p.name, p.pipeline_obj)
        if p.action == "tasks":
            logger.info("Action :: tasks %s, ", p.name)
            if p.pipeline:
                ress = []
                for i in p.pipeline:
                    res = i.run(auth_id, *args, **kwargs)
                    ress.append(res)
                return ress
            return []
        else:
            logger.info("Action :: pipeline %s,", p.name)
            if p.pipeline:
                return p.pipeline[0].run(auth_id, *args, **kwargs)
    else:
        rz = []
        if p.action == "tasks":
            logger.info("Action :: tasks %s,", p.name)

            if p.pipeline:
                for i in p.pipeline:
                    rz.append(i.run(auth_id, *args, **kwargs))
        else:
            logger.info("Action :: pipeline %s,", p.name)
            if p.pipeline:
                rz.append(p.pipeline[0].run(auth_id, *args, **kwargs))

        ln = len(rz)
        for _in in range(ln):
            ts, r = rz[_in]
            z = {
                "pipeline_name": name,
                "task_name": ts,
                "result": r
            }
            if not kwargs.pop("_exclude_inputs", True):
                z["input"] = kwargs

            if _in == ln-1 and p._traker.getTaskStatus(auth_id):
                z["is_final"] = True
            else:
                z["is_final"] = False

            p.add_result(auth_id, z)


class Task(metaclass=MetaClass):
    def __init__(self, name, json_=None, parent_=None, pipeline=None, **kwargs):
        self.parent_ = parent_
        self._pipeline_ref = pipeline
        self.id = name
        self.name = json_.get("name", name)
        self.task = json_
        self.action = self.task.get("action") or "Task"

        self.pipeline = None
        self.next = None
        self.prev = None

        self.init_pipline()

    def get_Pipeline(self):
        return Pipeline(f"{self.name}_pipeline", json_=self.task, pipeline=self._pipeline_ref)

    def init_pipline(self):
        if self.action not in ("pipeline", "tasks"):
            return
        # Pipeline(f"{self.id}_pipeline", self.task, queue=self.queue_id)
        self.pipeline = self.get_Pipeline()

    def _run(self, task_ob, auth_id=None, *args, **kwargs):
        # logger.info("Task _run function")
        # p = Pipeline(name=name)
        # p = self._pipeline_ref
        # _self = Task(task_ob["name"], json_=task_ob)
        # _self=self

        res = []

        f = None
        # _all_default_run_func
        fn = [
            f"_{self._pipeline_ref.name}_{self.name}_run_func",
            f"_{self._pipeline_ref.name}_default_run_func",
            f"_all_{self.name}_run_func",
            f"_all_default_run_func"
        ]
        # logger.info("Task run function name is :: %s", fn)

        for n in fn:
            if n in dir(self):
                logger.info("Checking for :: %s", n)
                f = getattr(self, n)
                break

        # if f"{self.name}_run_func" in dir(self):
        #     logger.info("Checking for :: %s", f"{self.name}_run_func")
        #     f = getattr(self, f"{self.name}_run_func")
        # elif f"_run_func" in dir(self):
        #     logger.info("Checking for :: %s", f"_run_func")
        #     f = getattr(self, f"_run_func")

        if f:
            res = f(*args, **{"pipeline_name": self.name, **task_ob, **kwargs})
        return res

    def run(self, auth_id=None, *args, **kwargs):
        self._pipeline_ref._traker.updateStatus(
            auth_id, self.id, processing=True)
        # logger.info("Task run function")
        if self.action == "pipeline":
            res = self.pipeline.run(auth_id, *args, **kwargs)
        elif self.action == "tasks":
            res = self.pipeline.run(auth_id, *args, **kwargs)
        else:
            res = self._run(self.task, auth_id, *args, **kwargs)

        kwargs.pop("_prev_task_result", None)
        self._pipeline_ref._traker.updateStatus(auth_id, self.id, done=True)
        if self.next:
            return self.next.run(auth_id, _prev_task_result=res, *args, **kwargs)
        # else:
        #   rez = []
        #     for r in res:
        #         z = {
        #             "pipeline_name": self._pipeline_ref.name,
        #             "task_name": self.name,
        #             "result": r,
        #             **kwargs
        #         }
        #         # yield z
        #         rez.append(z)
        #         self._pipeline_ref.add_result(auth_id, z)
        return self.name, res


def pipeline_task_function(task_name="default", pipeline_name="all"):
    def task_function(func):
        # _all_default_run_func
        n = f'_{pipeline_name}_{task_name}_run_func'
        setattr(Task, n, func)
        # if task_name:
        #     setattr(Task, f"{task_name}_run_func", func)
        # else:
        #     setattr(Task, "_run_func", func)
        return func
    return task_function
