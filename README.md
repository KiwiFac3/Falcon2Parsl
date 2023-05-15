# Falcon: Online File Transfers Optimization

## Usage

1. Please create virtual environments on both source and destination server. For exmaple: run `python3 -m venv <venv_dir>`
2. Activate virtual environment: run `source <venv_dir>/bin/activate`
3. Install required python packages: `pip3 install -r requirements.txt`
4. On the destination server, please edit `Falcon/config_receiver.py` and run `python3 receiver.py`
5. On the source server, please edit `Falcon/config_sender.py` and run `python3 sender.py` 
6. On the destenation server, please edit `Test/Falcon.py` and run it


Note: You can have multiple sources to the destination server.
