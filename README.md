# glofox-challenge
Data engineering case.

## Setup used
Python 3.7.11 
Spark 3.1.2 

    pip install -r requirements.txt


## How to run

    python generateDictonary.py
    python generateReversedIndex.py

The default spark configuration takes 4g and all local cores.  
To change the SparkConf, update the function createSparkSession() in utils.py
