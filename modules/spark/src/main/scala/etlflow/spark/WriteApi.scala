package etlflow.spark

import etlflow.spark.IOType._
import etlflow.utils.ApplicationLogger
import etlflow.model.EtlFlowException.EtlJobException
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import scala.reflect.runtime.universe.TypeTag

object WriteApi extends ApplicationLogger {

  logger.info(s"Loaded ${getClass.getName}")

  def WriteDSHelper[T <: Product: TypeTag](
      output_type: IOType,
      output_location: String,
      partition_by: Seq[String] = Seq.empty[String],
      save_mode: SaveMode = SaveMode.Append,
      output_filename: Option[String] = None,
      recordsWrittenCount: Long,
      n: Int = 1,
      compression: String = "none",
      repartition: Boolean = false
  ): Map[String, String] = {
    val mapping = Encoders.product[T]
    Map(
      "output_location" -> output_location,
      "output_filename" -> output_filename.getOrElse("NA"),
      "output_type"     -> output_type.toString,
      "output_class"    -> mapping.schema.toDDL,
      "output_rows"     -> recordsWrittenCount.toString
    )

  }

  def WriteDS[T <: Product: TypeTag](
      output_type: IOType,
      output_location: String,
      partition_by: Seq[String] = Seq.empty[String],
      save_mode: SaveMode = SaveMode.Append,
      output_filename: Option[String] = None,
      n: Int = 1,
      compression: String = "none", // ("compression", "gzip","snappy")
      repartition: Boolean = false
  )(source: Dataset[T], spark: SparkSession): Unit = {
    val mapping = Encoders.product[T]

    logger.info("#" * 20 + " Actual Output Schema " + "#" * 20)
    source.schema.printTreeString()
    logger.info("#" * 20 + " Provided Output Case Class Schema " + "#" * 20)
    mapping.schema.printTreeString()

    val df_writer = partition_by match {
      case partition if partition.nonEmpty && repartition =>
        logger.info(s"Will generate $n repartitioned output files inside partitions $partition_by")
        source
          .select(mapping.schema.map(x => col(x.name)): _*)
          .as[T](mapping)
          .repartition(n, partition.map(c => col(c)): _*)
          .write
          .option("compression", compression)
      case partition if partition.isEmpty && repartition =>
        logger.info(s"Will generate $n repartitioned output files")
        source
          .select(mapping.schema.map(x => col(x.name)): _*)
          .as[T](mapping)
          .repartition(n)
          .write
          .option("compression", compression)
      case _ => source.select(mapping.schema.map(x => col(x.name)): _*).as[T](mapping).write.option("compression", compression)
    }

    val df_writer_options = output_type match {
      case CSV(delimiter, header_present, _, quotechar) =>
        df_writer
          .format("csv")
          .option("delimiter", delimiter)
          .option("quote", quotechar)
          .option("header", header_present)
      case EXCEL            => df_writer.format("com.crealytics.spark.excel").option("useHeader", "true")
      case PARQUET          => df_writer.format("parquet")
      case ORC              => df_writer.format("orc")
      case JSON(multi_line) => df_writer.format("json").option("multiline", multi_line)
      case TEXT             => df_writer.format("text")
      case RDB(_, _)        => df_writer
      case a                => throw EtlJobException(s"Unsupported output format $a")
    }

    partition_by match {
      case partition if partition.nonEmpty =>
        output_type match {
          case RDB(_, _) => throw EtlJobException("Output partitioning with JDBC is not yet implemented")
          case _         => df_writer_options.partitionBy(partition: _*).mode(save_mode).save(output_location)
        }
      case _ =>
        output_type match {
          case RDB(jdbc, _) =>
            val prop = new java.util.Properties
            prop.setProperty("driver", jdbc.driver)
            prop.setProperty("user", jdbc.user)
            prop.setProperty("password", jdbc.password)
            df_writer_options.mode(save_mode).jdbc(jdbc.url, output_location, prop)
          case _ => df_writer_options.mode(save_mode).save(output_location)
        }
    }

    logger.info(
      s"Successfully wrote data in $output_type in location $output_location with SAVEMODE $save_mode Partitioned by $partition_by"
    )

    output_filename.foreach { output_file =>
      val path       = s"$output_location/"
      val fs         = FileSystem.get(new java.net.URI(path), spark.sparkContext.hadoopConfiguration)
      val fileStatus = fs.globStatus(new Path(path + "part*"))
      if (fileStatus.size > 1) {
        logger.error("multiple output files found, expected single file")
        throw new RuntimeException("multiple output files found, expected single file")
      }
      val fileName = fileStatus(0).getPath.getName
      fs.rename(new Path(path + fileName), new Path(path + output_file))
      logger.info(s"Renamed file path $path$fileName to $path$output_file")
      fs.close()
    }
  }
}
