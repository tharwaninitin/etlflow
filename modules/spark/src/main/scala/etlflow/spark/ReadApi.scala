package etlflow.spark

import etlflow.spark.IOType._
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, split}
import scala.reflect.runtime.universe.TypeTag

object ReadApi extends ApplicationLogger {
  private def dfReader(location: Seq[String], input_type: IOType)(spark: SparkSession): DataFrameReader = {
    val df_reader = spark.read
    input_type match {
      case CSV(delimiter, header_present, parse_mode, quotechar) =>
        df_reader
          .format("csv")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .option("delimiter", delimiter)
          .option("quote", quotechar)
          .option("header", header_present)
          .option("mode", parse_mode)
      case JSON(multi_line) => df_reader.format("json").option("multiline", multi_line)
      case PARQUET          => df_reader.format("parquet")
      case ORC              => df_reader.format("orc")
      case RDB(jdbc, partition) =>
        if (partition.isDefined) {
          df_reader
            .format("jdbc")
            .option("url", jdbc.url)
            .option("dbtable", location.mkString)
            .option("user", jdbc.user)
            .option("password", jdbc.password)
            .option("driver", jdbc.driver)
            .option("numPartitions", partition.get.num_partition.toLong)
            .option("partitionColumn", partition.get.partition_column)
            .option("lowerBound", partition.get.lower_bound)
            .option("upperBound", partition.get.upper_bound)
        } else {
          df_reader
            .format("jdbc")
            .option("url", jdbc.url)
            .option("dbtable", location.mkString)
            .option("user", jdbc.user)
            .option("password", jdbc.password)
            .option("driver", jdbc.driver)
        }
      case BQ(temp_dataset, operation_type) =>
        operation_type match {
          case "table" => df_reader.format("bigquery").option("table", location.head)
          case "query" =>
            spark.conf.set("viewsEnabled", "true")
            spark.conf.set("materializationDataset", temp_dataset)
            df_reader.format("bigquery").option("query", location.head)
        }
      case _ => df_reader.format("text")
    }
  }

  def DF(location: Seq[String], input_type: IOType, where_clause: String = "1 = 1", select_clause: Seq[String] = Seq("*"))(
      spark: SparkSession
  ): Dataset[Row] = {
    logger.info("Input location: " + location.toList)

    val df_reader = dfReader(location, input_type)(spark)

    val df = input_type match {
      case MCSV(delimiter, column_count) =>
        val columns: IndexedSeq[Column] = (0 to column_count).map(id => col(s"value")(id).as(s"value${id + 1}"))
        df_reader
          .load(location: _*)
          .withColumn("value", split(col("value"), delimiter))
          .select(columns: _*)
      case _ => df_reader.load(location: _*).where(where_clause).selectExpr(select_clause: _*)
    }
    df
  }

  def DSProps[T <: Product: TypeTag](location: Seq[String], input_type: IOType): Map[String, String] = Map(
    "input_location" -> location.mkString(","),
    "input_type"     -> input_type.toString,
    "input_class"    -> Encoders.product[T].schema.toDDL
  )

  def DS[T <: Product: TypeTag](location: Seq[String], input_type: IOType, where_clause: String = "1 = 1")(
      spark: SparkSession
  ): Dataset[T] = {

    logger.info("Input location: " + location.toList)

    val df_reader = dfReader(location, input_type)(spark)

    val mapping = Encoders.product[T]

    val df = input_type match {
      case RDB(_, _) | BQ(_, _)      => df_reader.load().where(where_clause)
      case CSV(_, _, _, _) | JSON(_) => df_reader.schema(mapping.schema).load(location: _*).where(where_clause)
      case PARQUET | ORC             => df_reader.load(location: _*).where(where_clause)
      case MCSV(delimiter, column_count) =>
        val columns: IndexedSeq[Column] = (0 to column_count).map(id => col(s"value")(id).as(s"value${id + 1}"))
        df_reader
          .load(location: _*)
          .withColumn("value", split(col("value"), delimiter))
          .select(columns: _*)
      case TEXT => df_reader.load(location: _*).where(where_clause)
    }

    logger.info("#" * 20 + " Actual Input Schema " + "#" * 20)
    df.schema.printTreeString() // df.schema.foreach(x => read_logger.info(x.toString))
    logger.info("#" * 20 + " Provided Input Case Class Schema " + "#" * 20)
    mapping.schema.printTreeString()

    // Selecting only those columns which are provided in type T
    df.select(mapping.schema.map(x => col(x.name)): _*).as[T](mapping)
  }
}
