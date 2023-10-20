#!/bin/bash

export SPARK_SUBMIT="/mnt/c/Spark"

${SPARK_SUBMIT}/spark-3.5.0-bin-hadoop3-scala2.13/bin/spark-submit \
--master local \
--class train.Main \
${SPARK_SUBMIT}/spark-train.jar