# Aim of this project

This project is reading from a Kafka stream of events that gets published to the `events` topic. Those messages are expected to be in a valid json format and to contain the fields `uid` (the customer id as String) and `ts` (a UNIX timestamp) in the first level of their json tree.

The script in this project will read the json information from that `events` topic and count the number unique customer ids (the `uid` field) per minute as defined in the event (the `ts` field). The resulting counts will then printed to the console and also published to a new topic called `user_count`. 


---
# How to run it

## Requirements

* Docker
* Scala SDK

## Start Kafka environment
1. open a shell on the project root and navigate to folder `src/main/docker`
2. run `docker-compose up -d` to initially download and start the **kafka environment**
3. when it is there log into the `broker` container with `docker exec -it broker bash`
4. inside the `broker` container create the **topics** required for this job by running the following commands
```
kafka-topics \
  --bootstrap-server localhost:9092 \
  --topic events \
  --create

kafka-topics \
  --bootstrap-server localhost:9092 \
  --topic user_count \
  --create
```

## Set up Kafka job for the customer count
1. open your IDE and import the project
2. configure the **scala sdk**, if needed
3. run the script in the `CountEventsBasic` object
4. the code will keep running and write the counted results to the `user_count` topic and to the console

## Check the output in the new Kafka Topic
1. use the open shell logged in to the `broker` or log in to the container again with 
```
docker exec -it broker bash
```
2. start the consumer on the topic with
```
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user_count \
  --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true
```
