import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object Main {

  def main(args: Array[String]): Unit = {

    // PRODUCER

    val producerProperties = new Properties()
    producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](producerProperties)

    val record = new ProducerRecord[String, String]("test", "key", "value")

    // fire and forgett
    producer.send(record)

    // send returns future
    val f = producer.send(record)

    // send with callback
    producer.send(record, (m:RecordMetadata, e:Exception) => {
      print(m)
      print(e)
    })

    // CONSUMER

    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.servers", "127.0.0.1:9092")
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("group.id", "dummy-group")

    val consumer: KafkaConsumer[String, String] = KafkaConsumer[String, String]

    consumer.subscribe(Collections.singletonList("test"))

    val records = consumer.poll(100)


  }
}
