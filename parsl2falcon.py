import parsl
from parsl import python_app, File, MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
from falcon_parsl.falcon2 import FalconStaging
import config_sender as config
import time

# set the root directory and host for the receiver
ROOT_DIR = config.configurations["data_dir"]
HOST = config.configurations["receiver"]["host"]

# set the names of the files to be converted
FILE_NAMES = ['data.txt', 'data1.txt', 'data2.txt', 'data3.txt']

# set up Parsl config
config = Config(
    executors=[
        HighThroughputExecutor(
            storage_access=[FalconStaging(), NoOpFileStaging(), FTPSeparateTaskStaging(),
                            HTTPSeparateTaskStaging()],
        ),
    ],
)

# load the Parsl config
parsl.load(config)

# start a timer to record the elapsed time
start_time = time.time()

# define the conversion function
@python_app
def convert(inputs=[]):
    file='/home/mabughosh/mabughosh/data/receive/'+inputs[0].filename
    with open(file, 'r') as f:
        f.read()
        return file

# set up the inputs and outputs for the conversion
inputs = []
for name in FILE_NAMES:
    inputs.append(File('falcon://127.0.0.1' + ROOT_DIR + name))

inputs.append(File('falcon://134.197.95.132' + ROOT_DIR + 'data44.txt'))

# convert the input files and save the outputs
for name in inputs:
    f = convert(inputs=[name])
    print(f.result())

# stop the timer and print the elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Code ran in {elapsed_time:.2f} seconds")
