package content

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Grouped, KStream, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.slf4j.LoggerFactory

import java.util.Properties

object CountEvents {
  // Types
  type CustomerId = String
  type EventValue = String

  // Topics
  final val eventTopic = "events"
  final val eventsByCustomer = "events-by-customer"

  val builder = new StreamsBuilder
  val mapper = new ObjectMapper
  val logger = LoggerFactory.getLogger(CountEvents.getClass.getName)

  def defineCountingTopology(): Unit = {
    // set up the source stream
    val eventStream: KStream[CustomerId, EventValue] = builder.stream[CustomerId, EventValue](eventTopic)(
      consumedFromSerde(Serdes.stringSerde, Serdes.stringSerde))

    // TODO: Fix the time windows
    //val timeWindow = TimeWindows.of(Duration.ofSeconds(10))
    //val windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(classOf[String],10000)

    // transform the event stream to count events by customer ids
    eventStream
      // read the uid field and move it to the message key
      .map((_, value: EventValue) => {
        val customerId = Option(mapper.readTree(value)) match {
          case Some(value) =>
            value.get("uid").asText
          case None =>
            logger.warn("Could not find uid")
            null
        }
        (customerId, value)
      })
      .groupByKey(Grouped.`with`(Serdes.stringSerde, Serdes.stringSerde))
      // TODO: Fix the time windows
      //.windowedBy(timeWindow)
      .count()
      .toStream
      // print out the results
      .map((k, v) => {
        logger.debug(s"events per customer -> key: $k, value: $v")
        (k, v)
      })
      // TODO when consuming from new topic the messages don't show up - fix it
      .to(eventsByCustomer)(Produced.`with`(Serdes.stringSerde, Serdes.longSerde))
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "counting-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    defineCountingTopology()

    val topology: Topology = builder.build()

    println(topology.describe())

    val application: KafkaStreams = new KafkaStreams(topology, props)
    application.start()
  }
}
