import parsl
from parsl import python_app, File
from parsl.config import Config
from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
from parsl.executors import HighThroughputExecutor
import time

import sys

sys.path.insert(0, '/data/mabughosh/Falcon2Parsl')
from data_provider.falcon import FalconStaging

# set the working directory and host for the receiver
working_dir = '/data/mabughosh/files/'


# define the conversion function
@python_app
def convert(inputs=[]):
    file = '/data/mabughosh/Falcon2Parsl/data/' + inputs.filename
    with open(file, 'r') as f:
        f.read()
        return file


# set up Parsl config
config = Config(
    executors=[
        HighThroughputExecutor(
            storage_access=[FalconStaging(), NoOpFileStaging(), FTPSeparateTaskStaging(),
                            HTTPSeparateTaskStaging()],
            max_workers=8
        ),
    ],
)

# load the Parsl config
parsl.load(config)

# start a timer to record the elapsed time
start_time = time.time()

# set up the inputs and outputs for the conversion
inputs = []
for x in range(0, 100):
    inputs.append(File('falcon://134.197.113.70' + working_dir + 'largefile' + str(x) + '.txt'))

convert_tasks = []

# convert the input files and save the outputs
for name in inputs:
    task = convert(name)
    convert_tasks.append(task)

results = [task.result() for task in convert_tasks]

# stop the timer and print the elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Code ran in {elapsed_time:.2f} seconds")
