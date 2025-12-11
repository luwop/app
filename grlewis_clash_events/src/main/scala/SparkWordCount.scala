import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object SparkWordCount {

  implicit val formats = DefaultFormats

  case class ClashEvent(
    matches: Int,
    wins: Int,
    playerCards: List[Int],
    opponentCards: List[Int]
  )

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: spark-submit ... <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet

    val sparkConf = new SparkConf().setAppName("ClashSpeedLayer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> brokers.asInstanceOf[Object],
      "key.deserializer" -> classOf[StringDeserializer].asInstanceOf[Object],
      "value.deserializer" -> classOf[StringDeserializer].asInstanceOf[Object],
      "group.id" -> "clash_speed_layer_group".asInstanceOf[Object],
      "auto.offset.reset" -> "latest".asInstanceOf[Object],
      "enable.auto.commit" -> java.lang.Boolean.FALSE.asInstanceOf[Object],
      "security.protocol" -> "SASL_SSL".asInstanceOf[Object],
      "sasl.mechanism" -> "SCRAM-SHA-512".asInstanceOf[Object],
      "sasl.jaas.config" ->
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"mpcs53014-2025\" password=\"A3v4rd4@ujjw\";"
          .asInstanceOf[Object]
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val jsonStrings = stream.map(r => r.value())

    val events = jsonStrings.flatMap { json =>
      try Some(parse(json).extract[ClashEvent])
      catch { case _: Throwable => None }
    }

    events.foreachRDD { rdd =>
      rdd.foreach { e =>
        println(
          s"matches=${e.matches}, wins=${e.wins}, " +
            s"playerCards=${e.playerCards.mkString(",")}, " +
            s"opponentCards=${e.opponentCards.mkString(",")}"
        )
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
