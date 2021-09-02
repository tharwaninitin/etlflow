package etlflow.spark

import etlflow.schema.LoggingLevel
import etlflow.spark.IOType._
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, split}

import scala.reflect.runtime.universe.TypeTag

object ReadApi extends  ApplicationLogger {

  logger.info(s"Loaded ${getClass.getName}")

  def LoadDF(location: Seq[String], input_type: IOType, where_clause : String = "1 = 1", select_clause: Seq[String] = Seq("*"))(implicit spark: SparkSession): Dataset[Row] = {
    val df_reader = spark.read

    logger.info("Input file paths: " + location.toList)

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

  def LoadDSHelper[T <: Product : TypeTag](level: LoggingLevel,location: Seq[String], input_type: IOType, where_clause : String = "1 = 1") : Map[String,String] = {
    val mapping = Encoders.product[T]
    if (level == LoggingLevel.INFO){
      Map("input_location"->location.mkString(",")
        , "input_type"->input_type.toString
      )}else{
      Map("input_location"->location.mkString(",")
        , "input_type"->input_type.toString
        , "input_class" -> mapping.schema.toDDL
      )
    }
  }

  def LoadDS[T <: Product : TypeTag](location: Seq[String], input_type: IOType, where_clause: String = "1 = 1")(spark: SparkSession) : Dataset[T] = {
    val mapping = Encoders.product[T]

    val df_reader = spark.read

    logger.info("Input location: " + location.toList)

    val df_reader_options = input_type match {
      case CSV(delimiter,header_present,parse_mode,quotechar) => df_reader.format("csv").schema(mapping.schema)
        .option("columnNameOfCorruptRecord","_corrupt_record").option("delimiter", delimiter)
        .option("quote",quotechar).option("header", header_present).option("mode", parse_mode)
      case JSON(multi_line) => df_reader.format("json").option("multiline",multi_line).schema(mapping.schema)
      case PARQUET => df_reader.format("parquet")
      case ORC => df_reader.format("orc")
      case RDB(jdbc,partition) =>
        if(partition.isDefined) {
          df_reader.format("jdbc")
            .option("url", jdbc.url).option("dbtable", location.mkString).option("user", jdbc.user)
            .option("password", jdbc.password).option("driver", jdbc.driver)
            .option("numPartitions", partition.get.num_partition)
            .option("partitionColumn", partition.get.partition_column).option("lowerBound", partition.get.lower_bound)
            .option("upperBound", partition.get.upper_bound)
        } else{
          df_reader.format("jdbc").option("url", jdbc.url).option("dbtable", location.mkString).option("user", jdbc.user)
            .option("password", jdbc.password).option("driver", jdbc.driver)
        }
      case BQ(temp_dataset,operation_type) =>
        operation_type match {
          case "table" => df_reader.format("bigquery").option("table", location.mkString)
          case "query" =>
            spark.conf.set("viewsEnabled","true")
            spark.conf.set("materializationDataset",temp_dataset)
            df_reader.format("bigquery").option("query", location.mkString)
        }
      case _ => df_reader.format("text")
    }

    val df = input_type match {
      case RDB(_,_) | BQ(_,_) => df_reader_options.load().where(where_clause)
      case _ => df_reader_options.load(location: _*).where(where_clause)
    }

    logger.info("#"*20 + " Actual Input Schema " + "#"*20)
    df.schema.printTreeString // df.schema.foreach(x => read_logger.info(x.toString))
    logger.info("#"*20 + " Provided Input Case Class Schema " + "#"*20)
    mapping.schema.printTreeString

    df.select(mapping.schema.map(x => col(x.name)):_*).as[T](mapping)
  }

}