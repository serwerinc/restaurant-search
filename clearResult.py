import os
import glob

for p in glob.glob("results/*.csv"):
    if os.path.isfile(p):
        os.remove(p)
