package etlflow.spark

import etlflow.utils._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql._
import scala.reflect.runtime.universe.TypeTag

object ReadApi {

  private val read_logger = Logger.getLogger(getClass.getName)
  read_logger.info(s"Loaded ${getClass.getName}")

  def LoadDF(location: Seq[String], input_type: IOType, where_clause : String = "1 = 1", select_clause: Seq[String] = Seq("*"))(implicit spark: SparkSession): Dataset[Row] = {
    val df_reader = spark.read

    read_logger.info("Input file paths: " + location.toList)

    val df_reader_options = input_type match {
      case CSV(delimiter,header_present,_,quotechar) => df_reader.format("csv").option("delimiter", delimiter).option("quote",quotechar).option("header", header_present)
      case PARQUET => df_reader.format("parquet")
      case ORC => df_reader.format("orc")
      case JSON(multi_line) => df_reader.format("json").option("multiline",multi_line)
      case MCSV(_,_) | TEXT => df_reader.format("text")
      case _ => df_reader.format("text")
    }

    val df = input_type match {
      case MCSV(delimiter,column_count) => {
        val columns: IndexedSeq[Column] = (0 to column_count).map(id => col(s"value")(id).as(s"value${id+1}"))
        df_reader_options.load(location: _*)
          .withColumn("value" , split(col("value"), delimiter))
          .select(columns: _*)
      }
      case _ => df_reader_options.load(location: _*).where(where_clause).selectExpr(select_clause: _*)
    }

    //(df, Map("Read paths"->paths.toList.mkString(","), "File Type"->fileType.toString))
    df
  }

  def LoadDSHelper[T <: Product : TypeTag](level: String,location: Seq[String], input_type: IOType, where_clause : String = "1 = 1", select_clause: Seq[String] = Seq("*")) : Map[String,String] = {
    val mapping = Encoders.product[T]
    if (level.equalsIgnoreCase("info")){
     Map("input_location"->location.mkString(",")
      , "input_type"->input_type.toString
    )}else{
      Map("input_location"->location.mkString(",")
        , "input_type"->input_type.toString
        , "input_class" -> mapping.schema.toDDL
      )
    }
  }

  def LoadDS[T <: Product : TypeTag](location: Seq[String], input_type: IOType, where_clause: String = "1 = 1", select_clause: Seq[String] = Seq("*"))(spark: SparkSession) : Dataset[T] = {
    val mapping = Encoders.product[T]

    val df_reader = spark.read

    read_logger.info("Input location: " + location.toList)

    val df_reader_options = input_type match {
      case CSV(delimiter,header_present,parse_mode,quotechar) => df_reader.format("csv").schema(mapping.schema).option("columnNameOfCorruptRecord","_corrupt_record").option("delimiter", delimiter).option("quote",quotechar).option("header", header_present).option("mode", parse_mode)
      case JSON(multi_line) => df_reader.format("json").option("multiline",multi_line).schema(mapping.schema)
      case PARQUET => df_reader.format("parquet")
      case ORC => df_reader.format("orc")
      case JDBC(url, user, password, driver) => df_reader.format("jdbc")
        .option("url", url).option("dbtable", location.mkString).option("user", user).option("password", password).option("driver", driver)
      case BQ => df_reader.format("bigquery").option("table", location.mkString)
      case _ => df_reader.format("text")
    }

    val df = input_type match {
      case JDBC(_,_,_,_) | BQ => df_reader_options.load().where(where_clause).selectExpr(select_clause: _*)
      case _ => df_reader_options.load(location: _*).where(where_clause).selectExpr(select_clause: _*)
    }
    read_logger.info("#"*20 + " Input Schema " + "#"*20)
    df.schema.foreach(read_logger.info(_))
    read_logger.info("#"*20 + " Input Schema " + "#"*20)
    df.as[T](mapping)
  }

}