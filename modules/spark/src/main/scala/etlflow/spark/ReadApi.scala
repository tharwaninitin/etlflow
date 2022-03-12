package etlflow.spark

import etlflow.model.EtlFlowException.EtlJobException
import etlflow.spark.IOType._
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

@SuppressWarnings(Array("org.wartremover.warts.Throw", "org.wartremover.warts.TraversableOps"))
object ReadApi extends ApplicationLogger {
  private def dfRead(location: List[String], inputType: IOType)(spark: SparkSession): DataFrameReader = {
    val dfReader = spark.read
    inputType match {
      case CSV(delimiter, header_present, parse_mode, quotechar) =>
        dfReader
          .format("csv")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .option("delimiter", delimiter)
          .option("quote", quotechar)
          .option("header", header_present)
          .option("mode", parse_mode)
      case JSON(multi_line) => dfReader.format("json").option("multiline", multi_line)
      case PARQUET          => dfReader.format("parquet")
      case ORC              => dfReader.format("orc")
      case RDB(jdbc, partition) =>
        partition.fold {
          dfReader
            .format("jdbc")
            .option("url", jdbc.url)
            .option("dbtable", location.mkString)
            .option("user", jdbc.user)
            .option("password", jdbc.password)
            .option("driver", jdbc.driver)
        } { p =>
          dfReader
            .format("jdbc")
            .option("url", jdbc.url)
            .option("dbtable", location.mkString)
            .option("user", jdbc.user)
            .option("password", jdbc.password)
            .option("driver", jdbc.driver)
            .option("numPartitions", p.num_partition.toLong)
            .option("partitionColumn", p.partition_column)
            .option("lowerBound", p.lower_bound)
            .option("upperBound", p.upper_bound)
        }
      case BQ(temp_dataset, operation_type) =>
        operation_type match {
          case "table" => dfReader.format("bigquery").option("table", location.head)
          case "query" =>
            spark.conf.set("viewsEnabled", "true")
            spark.conf.set("materializationDataset", temp_dataset)
            dfReader.format("bigquery").option("query", location.head)
        }
      case TEXT | MCSV(_, _) => dfReader.format("text")
    }
  }

  def df(location: List[String], inputType: IOType, whereClause: String = "1 = 1", selectClause: Seq[String] = Seq("*"))(
      spark: SparkSession
  ): Dataset[Row] = {
    logger.info(s"Input location: $location")

    val dfReader = dfRead(location, inputType)(spark)

    val df = inputType match {
      case MCSV(delimiter, column_count) =>
        val columns: IndexedSeq[Column] = (0 to column_count).map(id => col(s"value")(id).as(s"value${id + 1}"))
        dfReader
          .load(location: _*)
          .withColumn("value", split(col("value"), delimiter))
          .select(columns: _*)
      case _ => dfReader.load(location: _*).where(whereClause).selectExpr(selectClause: _*)
    }
    df
  }

  def dSProps[T <: Product: TypeTag](location: List[String], inputType: IOType): Map[String, String] = Map(
    "input_location" -> location.mkString(","),
    "input_type"     -> inputType.toString,
    "input_class"    -> Encoders.product[T].schema.toDDL
  )

  def ds[T <: Product: TypeTag](location: List[String], inputType: IOType, whereClause: String = "1 = 1")(
      spark: SparkSession
  ): Dataset[T] = {

    logger.info(s"Input location: $location")

    val dfReader = dfRead(location, inputType)(spark)

    val mapping = Encoders.product[T]

    val df = inputType match {
      case RDB(_, _) | BQ(_, _)      => dfReader.load().where(whereClause)
      case CSV(_, _, _, _) | JSON(_) => dfReader.schema(mapping.schema).load(location: _*).where(whereClause)
      case PARQUET | ORC             => dfReader.load(location: _*).where(whereClause)
      case MCSV(delimiter, column_count) =>
        val columns: IndexedSeq[Column] = (0 to column_count).map(id => col(s"value")(id).as(s"value${id + 1}"))
        dfReader
          .load(location: _*)
          .withColumn("value", split(col("value"), delimiter))
          .select(columns: _*)
      case TEXT => dfReader.load(location: _*).where(whereClause)
    }

    logger.info("#" * 20 + " Actual Input Schema " + "#" * 20)
    df.schema.printTreeString() // df.schema.foreach(x => read_logger.info(x.toString))
    logger.info("#" * 20 + " Provided Input Case Class Schema " + "#" * 20)
    mapping.schema.printTreeString()

    // Selecting only those columns which are provided in type T
    df.select(mapping.schema.map(x => col(x.name)): _*).as[T](mapping)
  }

  private def streamingDf(location: String, inputType: IOType, schema: StructType)(spark: SparkSession): DataFrame = {
    val dfReader = spark.readStream
    inputType match {
      case CSV(delimiter, header_present, parse_mode, quotechar) =>
        dfReader
          .format("csv")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .option("delimiter", delimiter)
          .option("quote", quotechar)
          .option("header", header_present)
          .option("mode", parse_mode)
          .schema(schema)
          .load(location)
      case JSON(multi_line) => dfReader.format("json").option("multiline", multi_line).schema(schema).load(location)
      case PARQUET          => dfReader.format("parquet").schema(schema).load(location)
      case ORC              => dfReader.format("orc").schema(schema).load(location)
      case TEXT             => dfReader.format("text").schema(schema).load(location)
      case MCSV(delimiter, column_count) =>
        val columns: IndexedSeq[Column] = (0 to column_count).map(id => col(s"value")(id).as(s"value${id + 1}"))
        dfReader
          .format("text")
          .schema(schema)
          .load(location)
          .withColumn("value", split(col("value"), delimiter))
          .select(columns: _*)
      case a => throw EtlJobException(s"Unsupported input format $a")
    }
  }

  def streamingDS[T <: Product: TypeTag](location: String, inputType: IOType, whereClause: String = "1 = 1")(
      spark: SparkSession
  ): Dataset[T] = {

    logger.info(s"Input location: $location")

    val mapping = Encoders.product[T]

    val df = streamingDf(location, inputType, mapping.schema)(spark).where(whereClause)

    logger.info("#" * 20 + " Provided Input Schema " + "#" * 20)
    mapping.schema.printTreeString()

    // Selecting only those columns which are provided in type T
    df.select(mapping.schema.map(x => col(x.name)): _*).as[T](mapping)
  }
}
