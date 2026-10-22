"""Minimal reproduction script for Apache Spark SPARK-53759.

Issue Summary:
-------------
On Windows with Python 3.12+, Python workers fail with:
    org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
    Caused by: java.io.EOFException

Supported Environments:
-----------------------
- Python <= 3.11 (e.g. test-py311 conda environment): Native RDD executes with code 0.
- Linux / WSL2 / Docker container runtimes: Native RDD executes with code 0.
"""

import os
import sys
import traceback

# Locate JDK 11 if JAVA_HOME is unset
if "JAVA_HOME" not in os.environ:
    candidates = [
        os.path.join(sys.prefix, "Library"),
        r"C:\Users\vaish\miniconda3\envs\datastack-env\Library",
    ]
    for cand in candidates:
        if os.path.exists(cand) and os.path.exists(os.path.join(cand, "bin", "java.exe")):
            os.environ["JAVA_HOME"] = cand
            os.environ["PATH"] = os.path.join(cand, "bin") + os.pathsep + os.environ.get("PATH", "")
            break

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import SparkConf, SparkContext

print("Python executable:", sys.executable)
print("Python version:", sys.version)

conf = SparkConf().setMaster("local[1]").setAppName("reproduce_failure")
conf.set("spark.driver.host", "127.0.0.1")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

try:
    print("Submitting RDD task: sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()")
    res = sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()
    print("RDD SUCCESS:", res)
except Exception:
    print("CAUGHT EXPECTED REPRODUCTION FAILURE:")
    traceback.print_exc()
finally:
    sc.stop()
