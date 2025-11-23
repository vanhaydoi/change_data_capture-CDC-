import  os
import sys


os.environ["PYSPARK_PYTHON"] = "/home/prime/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/prime/anaconda3/bin/python3"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))