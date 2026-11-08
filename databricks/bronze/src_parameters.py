# Databricks notebook source
params_dict = [
    {"src": "bookings"},
    {"src": "flights"},
    {"src": "airports"},
    {"src": "customers"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="src_key", value=params_dict)