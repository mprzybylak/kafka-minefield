import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

object Main {

  def main(args: Array[String]): Unit = {

    val producerProperties = new Properties()
    producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](producerProperties)
  }
}
