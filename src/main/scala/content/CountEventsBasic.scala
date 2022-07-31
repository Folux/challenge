package content

import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import java.time.Duration
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object CountEventsBasic extends App{
  // types (for better readability)
  type CustomerId = String
  type InputTimeStamp = Long
  type Minute = DateTime
  type MinuteKey = String
  type UniqueUserCount = String

  // Kafka topics
  final val eventTopic = "events"
  final val userCount = "user_count"

  // data structure for incoming event
  case class IncomingEvent( uid: CustomerId,
                            ts: InputTimeStamp
                           )

  // implicit json decode to automatically parse incoming strings
  implicit val incomingEventDecoder: Decoder[IncomingEvent] = deriveDecoder[IncomingEvent]

  // properties and consumer producer initialisation
  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  val consumer = new KafkaConsumer(props)
  val producer = new KafkaProducer[MinuteKey, UniqueUserCount](props)

  val topics = List(eventTopic)

  // TODO
  //   This map is just locally for this single consumer.
  //   For the future when thinking on computing this in parallel we need to use a clustered solution,
  //   a KTable from the Kafka Streams library for example.
  // map to cache the result of the unique user count per minute
  val minuteToCount = mutable.Map.empty[Minute, mutable.Set[CustomerId]]

  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(Duration.ofMillis(10))
      // iterating over all polled events
      for (record <- records.asScala) {
        // parse the input event - wrong events get ignored with a warning
        val inputEvent = decode(record.value()) match {
          case Left(error) => println(s"Could not parse incoming json message: ${error.getMessage}.\n" +
            "\tIncoming message was: ${record.value()}")
            None
          case Right(incomingEvent) =>
            Some(incomingEvent)
        }

        // extract customer id and timestamp from incoming event and compute count
        inputEvent.foreach(input => {
          val dateTime = new DateTime(input.ts, DateTimeZone.UTC)
          val customerId = input.uid
          // truncate input time by second
          val truncatedDateTime =
            dateTime
            .withMillisOfSecond(0)
            .withSecondOfMinute(0)

          // update the counts in the cache
          val newCount =
            minuteToCount.get(truncatedDateTime) match {
              case Some(uniqueCustomers) =>
                minuteToCount.put(truncatedDateTime, uniqueCustomers += customerId)
                uniqueCustomers.size
              case None =>
                minuteToCount.put(truncatedDateTime, mutable.Set(customerId))
                1L
            }

          // print the result on the console
          val formattedMinute = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm"))
          println(s"The minute $formattedMinute has $newCount unique customer ids")

          // push the result to a new topic
          val outputRecord = new ProducerRecord[MinuteKey, UniqueUserCount](userCount, truncatedDateTime.toString, newCount.toString)
          producer.send(outputRecord)
        })
      }
    }
  } catch {
    case e:Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
