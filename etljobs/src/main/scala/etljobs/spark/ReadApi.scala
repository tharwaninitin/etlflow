package etljobs.spark

import etljobs.utils._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql._
import scala.reflect.runtime.universe.TypeTag

object ReadApi {

  private val read_logger = Logger.getLogger(getClass.getName)
  read_logger.info(s"Loaded ${getClass.getName}")

  def LoadDF(paths: Seq[String], fileType: IOType, where_clause : String = "1 = 1", select_clause: Seq[String] = Seq("*"))(implicit spark: SparkSession): Dataset[Row] = {
    val df_reader = spark.read

    read_logger.info("Input file paths: " + paths.toList)

    val df_reader_options = fileType match {
      case CSV(delimiter,header_present,_) => df_reader.format("csv").option("delimiter", delimiter).option("header", header_present)
      case PARQUET => df_reader.format("parquet")
      case ORC => df_reader.format("orc")
      case JSON => df_reader.format("json")
      case MCSV(_,_) | TEXT => df_reader.format("text")
    }

    val df = fileType match {
      case MCSV(delimiter,column_count) => {
        val columns: IndexedSeq[Column] = (0 to column_count).map(id => col(s"value")(id).as(s"value${id+1}"))
        df_reader_options.load(paths: _*)
          .withColumn("value" , split(col("value"), delimiter))
          .select(columns: _*)
      }
      case _ => df_reader_options.load(paths: _*).where(where_clause).selectExpr(select_clause: _*)
    }

    //(df, Map("Read paths"->paths.toList.mkString(","), "File Type"->fileType.toString))
    df
  }

  def LoadDSHelper[T <: Product : TypeTag](location: Seq[String], input_type: IOType, where_clause : String = "1 = 1", select_clause: Seq[String] = Seq("*")) : Map[String,String] = {
    val mapping = Encoders.product[T]
    Map("input_location"->location.mkString(",")
      , "input_type"->input_type.toString
      , "input_class" -> mapping.schema.toDDL
    )
  }

  def LoadDS[T <: Product : TypeTag](location: Seq[String], input_type: IOType, where_clause : String = "1 = 1", select_clause: Seq[String] = Seq("*"))(implicit spark: SparkSession) : Dataset[T] = {
    val mapping = Encoders.product[T]

    val df_reader = spark.read

    read_logger.info("Input location: " + location.toList)

    val df_reader_options = input_type match {
      case CSV(delimiter,header_present,parse_mode) => df_reader.format("csv").schema(mapping.schema).option("columnNameOfCorruptRecord","_corrupt_record").option("delimiter", delimiter).option("header", header_present).option("mode", parse_mode)
      case PARQUET => df_reader.format("parquet").schema(mapping.schema)
      case ORC => df_reader.format("orc").schema(mapping.schema)
      case JSON => df_reader.format("json").schema(mapping.schema)
      case JDBC(url, user, password) => df_reader.format("jdbc").schema(mapping.schema)
        .option("url", url).option("dbtable", location.mkString).option("user", user).option("password", password)
      case BQ => df_reader.format("bigquery").schema(mapping.schema).option("table", location.mkString)
    }

    val df = input_type match {
      case JDBC(_,_,_) | BQ => df_reader_options.load().where(where_clause).selectExpr(select_clause: _*)
      case _ => df_reader_options.load(location: _*).where(where_clause).selectExpr(select_clause: _*)
    }

    df.as[T](mapping)
  }

}
