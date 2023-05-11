import parsl
from parsl import python_app, File, MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging

working_dir= '/home/mabughosh/mabughosh/data/receive'

@python_app
def convert(inputs=[], outputs=[]):
    with open('/home/mabughosh/mabughosh/data/receive/'+inputs.filename, 'r') as f:
        f.read()
        return inputs.filename

config = Config(
    executors=[
        HighThroughputExecutor(
            working_dir=working_dir,
            storage_access=[NoOpFileStaging(), FTPSeparateTaskStaging(),
                            HTTPSeparateTaskStaging()],
        ),
    ],
)

parsl.load(config)

inputs = []
inputs = File('http://134.197.95.132/data44.txt')

f = convert(inputs)

print(f.result())
