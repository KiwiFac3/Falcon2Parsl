import parsl
from parsl import python_app, File, MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
from falcon_parsl.falcon2 import FalconStaging
import config_sender as config
import time

ROOT_DIR = config.configurations["data_dir"]
HOST = config.configurations["receiver"]["host"]

# FILE_NAMES = ['1MB_file.txt', '5MB_file.txt']
# FILE_NAMES = ['1g.bin', '1g_1.bin', '1g_2.bin', '1g_3.bin', '1g_4.bin']
FILE_NAMES = ['data.txt', 'data1.txt', 'data2.txt', 'data3.txt']
# FILE_NAMES = ['1g.bin', '1g_1.bin', '1g_2.bin', '1g_3.bin', '1g_4.bin', '1g_5.bin', '1g_6.bin', '1g_7.bin', '1g_8.bin',
#               '1g_9.bin']

config = Config(
    executors=[
        HighThroughputExecutor(
            storage_access=[FalconStaging(), NoOpFileStaging(), FTPSeparateTaskStaging(),
                            HTTPSeparateTaskStaging()],
        ),
    ],
)

parsl.load(config)
start_time = time.time()

@python_app
def convert(inputs=[], outputs=[]):
    file='/home/mabughosh/mabughosh/data/receive/'+inputs[0].filename
    with open(file, 'r') as f:
        content = f.read()
        return file


inputs = []
# for name in FILE_NAMES:
#     inputs.append(File('falcon://127.0.0.1' + ROOT_DIR + name))

inputs.append(File('falcon://134.197.95.132' + ROOT_DIR + 'data44.txt'))

outputs = File('file:///home/mabughosh/mabughosh/data/ABCD.txt')


f = convert(inputs, outputs=[outputs])

for name in inputs:
    f = convert(inputs=[name], outputs=[outputs])
    print(f.result())

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Code ran in {elapsed_time:.2f} seconds")