# Databricks notebook source
import sys
print("\n".join(sys.path))

# COMMAND ----------

from sample import n_to_mth
n_to_mth(3, 4)

# COMMAND ----------

from utils.sample2 import cube_root
cube_root(8)
