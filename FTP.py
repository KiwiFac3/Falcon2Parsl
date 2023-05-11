import parsl
from parsl import python_app, File, MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging

working_dir= '/home/mabughosh/mabughosh/data/receive'

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


@python_app
def read_file(url):
    with open('/home/mabughosh/mabughosh/data/receive/data44.txt', 'r') as f:
        content = f.read()
        return content


url = "ftp://134.197.95.132/data44.txt"
file = File(url)

content = read_file(file)
print(content.result())