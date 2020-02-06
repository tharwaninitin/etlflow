package etljobs.spark

import etljobs.utils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import scala.reflect.runtime.universe.TypeTag

object WriteApi {

  private val write_logger = Logger.getLogger(getClass.getName)
  write_logger.info(s"Loaded ${getClass.getName}")

  def WriteDSHelper[T <: Product : TypeTag](level: String,output_type: IOType, output_location: String, partition_by: Seq[String] = Seq.empty[String]
                                            , save_mode : SaveMode = SaveMode.Append, output_filename: Option[String] = None
                                            , recordsWrittenCount:Long,n : Int = 1, compression : String = "none", repartition : Boolean = false
                                           ) : Map[String,String] = {
    val mapping = Encoders.product[T]

    if (level.equalsIgnoreCase("info")){
    Map("output_location"->output_location
      , "output_filename"->output_filename.getOrElse("NA")
      , "output_type"->output_type.toString
      , "output_rows" -> recordsWrittenCount.toString
    )}else{
      Map("output_location"->output_location
        , "output_filename"->output_filename.getOrElse("NA")
        , "output_type"->output_type.toString
        , "output_class" -> mapping.schema.toDDL
        , "output_rows" -> recordsWrittenCount.toString

      )
    }
  }

  def WriteDS[T <: Product : TypeTag](output_type: IOType, output_location: String, partition_by: Seq[String] = Seq.empty[String]
                                      , save_mode : SaveMode = SaveMode.Append, output_filename: Option[String] = None
                                      , n : Int = 1, compression : String = "none", repartition : Boolean = false)
                                     (source: Dataset[T], spark : SparkSession) : Unit = {

    val df_writer = partition_by match {
      case partition if(!partition.isEmpty) => if (repartition)
        source.repartition(n,partition.map(c=>col(c)):_*).write.option("compression",compression) //("compression", "gzip","snappy")
      else source.write.option("compression",compression)
      case _ => source.repartition(n).write.option("compression",compression)
    }

    val df_writer_options = output_type match {
      case CSV(delimiter,header_present,_,quotechar) => df_writer.format("csv").option("delimiter", delimiter).option("quote",quotechar).option("header", header_present)
      case EXCEL => df_writer.format("com.crealytics.spark.excel").option("useHeader","true")
      case PARQUET => df_writer.format("parquet")
      case ORC => df_writer.format("orc")
      case JSON(multi_line) => df_writer.format("json").option("multiline",multi_line)
      case TEXT => df_writer.format("text")
      case _ => df_writer.format("text")
    }

    partition_by match {
      case partition if(!partition.isEmpty) => df_writer_options.partitionBy(partition:_*).mode(save_mode).save(output_location)
      case _ => df_writer_options.mode(save_mode).save(output_location)
    }

    write_logger.info(s"Successfully written data in $output_type in location $output_location with SAVEMODE $save_mode ${partition_by.map(pbc => "Partitioned by " + pbc)}")

    output_filename.foreach { output_file =>
      val path = s"$output_location/"
      val fs = FileSystem.get(new java.net.URI(path), spark.sparkContext.hadoopConfiguration)
      val fileName = fs.globStatus(new Path(path + "part*"))(0).getPath.getName
      fs.rename(new Path(path + fileName), new Path(path + output_file))
      write_logger.info(s"Renamed file path $path$fileName to $path$output_file")
      fs.close()
    }
  }
}