#Aim of this project

This project is reading from a Kafka stream of events that gets published to the `events` topic. Those messages are expected to be in a valid json format and to contain the field `uid` (the customer id) in the first level of their json tree.

The script in this project will read the json information from that `events` topic and count the number of events by the customer id (the `uid` field). The resulting counts will then be written into the debug logs and also published to a new topic called `events-by-customer`. 


---
#How to run it

##Requirements

* Docker
* Scala SDK

##Start Kafka environment
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
  --topic events-by-customer \
  --create \
  --config "cleanup.policy=compact"
```

##Set up Kafka job for the customer count
1. open your IDE and import the project
2. configure the **scala sdk**, if needed
3. run the main method in the `CountEvents` object
4. the code will keep running and write the counted results to the `events-by-customer` topic and into the **debug log**

