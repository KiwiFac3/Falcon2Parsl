# Falcon: Online File Transfers Optimization

## Usage

1. Please create virtual environments on both source and destination server. For exmaple: run `python3 -m venv <venv_dir>`
2. Activate virtual environment: run `source <venv_dir>/bin/activate`
3. Install required python packages: `pip3 install -r requirements.txt`
4. Install falcon-datamover python package using `pip install falcon-datamover`
5. On the destination server, please edit and run `python3 Test/Falcon.py`
6. On the source server, please run `falcon sender --host [IP address of the destination server] --port 5000 --data_dir [folder intented to be sent] --method probe` with the suitable arguments
   
