#  Troubleshooting Guide

This document outlines common problems and how to fix them.

## Docker won't start containers


Issue : Nothing happens or some services fail.

Fix:

Run docker ps -a to check failed containers

Try docker system prune -a to free up space

Restart Docker Desktop
## HDFS file not found

Fix:

Make sure copy_to_hdfs.bat was run successfully

Double-check the local folder path you entered

## Spark job fails: "File not found"

Fix:

Ensure the file exists on HDFS (hdfs dfs -ls /)

Check spelling and case in path names

Try viewing logs with:
docker logs spark-master

##  Beeline/DBeaver cannot connect to Hive

Fix:

Confirm Hive container is running:
docker ps | grep hive

Use this JDBC string:
jdbc:hive2://localhost:10000

Default user: hive, no password
