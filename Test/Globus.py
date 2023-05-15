import parsl
from parsl import python_app, File
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
from parsl.data_provider.globus import GlobusStaging

ROOT_DIR = '/home/mabughosh/mabughosh/data/receive'

config = Config(
    executors=[
        HighThroughputExecutor(
            working_dir=ROOT_DIR,
            storage_access=[NoOpFileStaging(), FTPSeparateTaskStaging(),
                            GlobusStaging(
                                endpoint_uuid="be5bfa16-dfc4-11ed-9a5f-83ef71fbf0ae",
                                endpoint_path=ROOT_DIR,
                                local_path=ROOT_DIR
                            ),
                            HTTPSeparateTaskStaging()],
        ),
    ],
)

parsl.load(config)

@python_app
def read_file(url):
    with open('/home/mabughosh/mabughosh/data/receive/data44.txt', 'r') as f:
        content = f.read()
        return content


remote_file = File("globus://5fe6f79e-dfae-11ed-9a5f-83ef71fbf0ae/home/mabughosh/mabughosh/data/send/data44.txt")

f = read_file(remote_file)
# print(f.result())
