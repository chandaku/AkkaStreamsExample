package org.example

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Producer
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

  object KafkaStreamExample extends App {

  implicit val system = ActorSystem("KafkaStreamExample")

  // Kafka consumer settings
  val consumerSettings: ConsumerSettings[Array[Byte], String] =
  ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
  .withBootstrapServers("localhost:9093")
  .withGroupId("group1")
  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  // Kafka producer settings
  val producerSettings: ProducerSettings[Array[Byte], String] =
  ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
  .withBootstrapServers("localhost:9093")

  // Kafka source: Read from "input-topic"
  val kafkaSource = Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("input-topic"))

  // Kafka sink: Write to "output-topic"
  val kafkaSink = Producer.plainSink(producerSettings)

  // Akka Streams flow: Transform messages with filtration
  val processFlow: Flow[ConsumerRecord[Array[Byte], String], ProducerRecord[Array[Byte], String], _] =
  Flow[ConsumerRecord[Array[Byte], String]]
  //.filter(record => shouldPublish(record.value)) // Add your filtration condition here
  .map { record =>
  // Perform any processing on the record here if needed
    print(record.key, record.value)
  new ProducerRecord("output-topic", record.key, record.value)
}

  // Filtration condition example (modify as needed)
  def shouldPublish(value: String): Boolean = {
  // Add your condition here, e.g., publish only if the value contains a specific substring
    print(value)
  value.contains("filterCondition")
}

  // Akka Streams graph: Connect source, flow, and sink
  val streamGraph = kafkaSource.via(processFlow).to(kafkaSink)

  // Run the stream
  streamGraph.run()

}
