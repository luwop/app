import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.util.Bytes

object DeckRecommender {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DeckRecommender")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df = spark.table("grlewis_card_events")
      .filter($"won" === 1)
      .select($"battle_id", $"card_id")

    val decks = df.groupBy("battle_id")
      .agg(collect_set("card_id").as("deck"))
      .filter(size($"deck") === 8)

    val exploded = decks
      .select($"deck", explode($"deck").as("card"))
      .withColumn("partner", explode($"deck"))
      .filter($"card" =!= $"partner")

    val pairCounts = exploded
      .groupBy("card", "partner")
      .agg(count("*").as("cnt"))

    val ranked = pairCounts
      .withColumn("rank", row_number().over(
        Window.partitionBy("card").orderBy($"cnt".desc)
      ))
      .filter($"rank" <= 8)

    val recommended = ranked
      .groupBy("card")
      .agg(collect_list("partner").as("recommended"))
      .orderBy("card")

    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "localhost")
    config.set("hbase.zookeeper.property.clientPort", "2181")

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("grlewis_recommended_decks_hb"))

    recommended.collect().foreach { row =>
      val card = row.getAs[Int]("card")
      val rec = row.getAs[Seq[Int]]("recommended")
      val json = rec.mkString("[", ",", "]")

      val put = new Put(Bytes.toBytes(card.toString))
      put.addColumn(Bytes.toBytes("rec"), Bytes.toBytes("deck1"), Bytes.toBytes(json))
      table.put(put)
    }

    table.close()
    connection.close()
    spark.stop()
  }
}
