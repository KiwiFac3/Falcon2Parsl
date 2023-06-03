import parsl
from parsl import python_app, File
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
import time

working_dir = '/data/mabughosh/Falcon2Parsl/data'

@python_app
def convert(input_file):
    with open(input_file, 'r') as f:
        f.read()
        return input_file

config = Config(
    executors=[
        HighThroughputExecutor(
            working_dir=working_dir,
            storage_access=[NoOpFileStaging(), FTPSeparateTaskStaging(), HTTPSeparateTaskStaging()],
            max_workers=8  # Adjust the number of workers based on available resources
        ),
    ],
)

parsl.load(config)

start_time = time.time()

inputs = []
for x in range(0, 100):
    inputs.append(File('ftp://134.197.113.70/largefile' + str(x) + '.txt'))

convert_tasks = []
for name in inputs:
    task = convert(name)
    convert_tasks.append(task)

results = [task.result() for task in convert_tasks]

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Code ran in {elapsed_time:.2f} seconds")