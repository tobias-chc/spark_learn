# Spark Notes

## Spark Execution Model and Architecture

**How to execute Spark Programs?**

1. Interactive Shell (Learning, Exploration)
   - spark-shell
   - Notebooks
2. Submit Job (Production)
   - spark-submit
   - Databricks Notebook
   - Rest API

**Spark Distributed Processing Model**

Every Spark Application applies a master-slave architecture and runs independently on the cluster.

**Spark Execution Model**
| Cluster Managers | Execution Modes | Execution Tools |
| ---------------- | --------------- | --------------- |
| local[n]         | Client          | IDE, Notebook   |
| YARN             | Client          | Notebook, Shell |
| YARN             | Cluster         | Spark Submit    |

## Create Spark Application

Application Logs Using Log4j:

1. Create a Log4j configuration file
2. Configure Spark JVM to pickup the Log4j configuration file
3. Create a Python Class to get Spark's Log4j instance and use it

Log4j basic setup (`log4j.properties`):

- Logger
- Configurations
- Appender: Output destination

