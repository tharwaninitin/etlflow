package etlflow.spark

import etlflow.spark.ReadApi.read_logger
import etlflow.utils.IOType
import etlflow.utils._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.slf4j.LoggerFactory
import zio.{Task, ZLayer}

import scala.reflect.runtime.universe.TypeTag

object SparkImpl {

  private val spark_logger = LoggerFactory.getLogger(getClass.getName)

  val live: ZLayer[SparkSession, Throwable, SparkApi] = ZLayer.fromFunction { spark: SparkSession =>
    new Service {
      override def LoadDSHelper[T <: Product : TypeTag](
                                                         level: String, location: Seq[String],
                                                         input_type: IOType, where_clause: String): Task[Map[String, String]] = Task {
        val mapping = Encoders.product[T]
        if (level.equalsIgnoreCase("info")) {
          Map("input_location" -> location.mkString(",")
            , "input_type" -> input_type.toString
          )
        } else {
          Map("input_location" -> location.mkString(",")
            , "input_type" -> input_type.toString
            , "input_class" -> mapping.schema.toDDL
          )
        }
      }
      override def LoadDS[T <: Product : TypeTag](
                                                   location: Seq[String], input_type: IOType,
                                                   where_clause: String): Task[Dataset[T]] = Task {
        val mapping = Encoders.product[T]

        val df_reader = spark.read

        spark_logger.info("Input location: " + location.toList)

        val df_reader_options = input_type match {
          case CSV(delimiter, header_present, parse_mode, quotechar) => df_reader.format("csv").schema(mapping.schema)
            .option("columnNameOfCorruptRecord", "_corrupt_record").option("delimiter", delimiter)
            .option("quote", quotechar).option("header", header_present).option("mode", parse_mode)
          case JSON(multi_line) => df_reader.format("json").option("multiline", multi_line).schema(mapping.schema)
          case PARQUET => df_reader.format("parquet")
          case ORC => df_reader.format("orc")
          case JDBC(url, user, password, driver) => df_reader.format("jdbc")
            .option("url", url).option("dbtable", location.mkString).option("user", user).option("password", password).option("driver", driver)
          case BQ(temp_dataset,operation_type) => {
            operation_type match {
              case "table" => df_reader.format("bigquery").option("table", location.mkString)
              case "query" =>  {
                spark.conf.set("viewsEnabled","true")
                spark.conf.set("materializationDataset",temp_dataset)
                df_reader.format("bigquery").option("query", location.mkString)
              }
            }
          }

          case _ => df_reader.format("text")
        }

        val df = input_type match {
          case JDBC(_, _, _, _) | BQ(_,_) => df_reader_options.load().where(where_clause)
          case _ => df_reader_options.load(location: _*).where(where_clause)
        }

        spark_logger.info("#" * 20 + " Actual Input Schema " + "#" * 20)
        df.schema.printTreeString // df.schema.foreach(x => read_logger.info(x.toString))
        spark_logger.info("#" * 20 + " Provided Input Case Class Schema " + "#" * 20)
        mapping.schema.printTreeString

        df.select(mapping.schema.map(x => col(x.name)): _*).as[T](mapping)
      }
      override def LoadDF(location: Seq[String], input_type: IOType, where_clause: String): Task[Dataset[Row]] = ???
    }
  }

}
