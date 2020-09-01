# Execution Instructions
1. Install requirements per the Setup Instructions below.
2. Run `run.sh`.

# Setup Instructions
1. Install dependencies using Pipfile.
2. Install Kafka binaries in a `kafka` folder in the root of this project.
3. Install Spark binaries in a `spark` folder in the root of this project.

## Notes
* To see output of Kafka Producer, run `kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic departments.sf.calls --from-beginning`.
