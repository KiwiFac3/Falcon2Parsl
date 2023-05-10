import logging
import pika
import zmq

from functools import partial
from parsl.app.app import python_app
from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)

get_input = getattr(__builtins__, 'raw_input', input)


def _get_falcon_provider(dfk, executor_label):
    if executor_label is None:
        raise ValueError("executor_label is mandatory")
    executor = dfk.executors[executor_label]
    if not hasattr(executor, "storage_access"):
        raise ValueError("specified executor does not have storage_access attribute")
    for provider in executor.storage_access:
        if isinstance(provider, FalconStaging):
            return provider

    raise Exception('No suitable Falcon endpoint defined for executor {}'.format(executor_label))


def get_falcon():
    Falcon.init()
    return Falcon()


class Falcon(object):
    # connection1 = None
    # channel1 = None

    @classmethod
    def init(cls):
        pass

    @classmethod
    def transfer_file(cls, path, netloc):
        context = zmq.Context()

        print("Connecting to hello world server…")
        socket = context.socket(zmq.REQ)
        print('connected')
        socket.connect("tcp://localhost:5555")

        print("Sending request [ %s ]…" % path)
        socket.send_string(path)

        #  Get the reply.
        message = socket.recv()
        print("Received reply [ %s ]" % message)
        # print(path, netloc)
        # connection1 = pika.BlockingConnection(
        #     pika.ConnectionParameters(host=netloc))
        # channel1 = connection1.channel()
        #
        # channel1.queue_declare(queue='task_queue', durable=True)
        #
        # channel1.basic_publish(
        #     exchange='',
        #     routing_key='task_queue',
        #     body=path,
        #     properties=pika.BasicProperties(
        #         delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        #     ))
        # print(" [x] Sent %r" % path)
        # connection1.close()


class FalconStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        logger.debug("Falcon checking file {}".format(repr(file)))
        return file.scheme == 'falcon'

    def can_stage_out(self, file):
        logger.debug("Falcon checking file {}".format(repr(file)))
        return file.scheme == 'falcon'

    def stage_in(self, dm, executor, file, parent_fut):
        falcon_provider = _get_falcon_provider(dm.dfk, executor)
        # falcon_provider._update_local_path(file, executor, dm.dfk)
        stage_in_app = falcon_provider._falcon_stage_in_app(executor=executor, dfk=dm.dfk)
        app_fut = stage_in_app(outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]

    def stage_out(self, dm, executor, file, app_fu):
        falcon_provider = _get_falcon_provider(dm.dfk, executor)
        # falcon_provider._update_local_path(file, executor, dm.dfk)
        stage_out_app = falcon_provider._falcon_stage_out_app(executor=executor, dfk=dm.dfk)
        return stage_out_app(app_fu, _parsl_staging_inhibit=True, inputs=[file])

    def __init__(self):
        self.falcon = None

    def _falcon_stage_in_app(self, executor, dfk):
        executor_obj = dfk.executors[executor]
        f = partial(_falcon_stage_in, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    def _falcon_stage_out_app(self, executor, dfk):
        executor_obj = dfk.executors[executor]
        f = partial(_falcon_stage_out, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    # could this happen at __init__ time?
    def initialize_falcon(self):
        if self.falcon is None:
            self.falcon = get_falcon()


def _falcon_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    file = outputs[0]
    provider.initialize_falcon()
    provider.falcon.transfer_file(file.path, file.netloc)


def _falcon_stage_out(provider, executor, app_fu, inputs=[], _parsl_staging_inhibit=True):
    file = inputs[0]
    print('staging out for ' + file.path + ' is done')
    # provider.initialize_falcon()
    # provider.falcon.transfer_file(file.path, file.netloc)
