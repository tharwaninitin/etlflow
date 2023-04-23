package etlflow.spark

import etlflow.log.ApplicationLogger
import etlflow.model.EtlFlowException.EtlJobException
import etlflow.spark.IOType._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import scala.reflect.runtime.universe.TypeTag

@SuppressWarnings(
  Array("org.wartremover.warts.Throw", "org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.ToString")
)
object WriteApi extends ApplicationLogger {

  def dSProps[T <: Product: TypeTag](
      outputType: IOType,
      outputLocation: String,
      saveMode: SaveMode = SaveMode.Append,
      partitionBy: Seq[String] = Seq.empty[String],
      outputFilename: Option[String] = None,
      compression: String = "none", // ("compression", "gzip","snappy")
      repartition: Boolean = false,
      repartitionNo: Int = 1
  ): Map[String, String] =
    Map(
      "output_location" -> outputLocation,
      "save_mode"       -> saveMode.toString,
      "partition_by"    -> partitionBy.mkString(","),
      "compression"     -> compression,
      "repartition"     -> repartition.toString,
      "repartition_no"  -> repartitionNo.toString,
      "output_filename" -> outputFilename.getOrElse("NA"),
      "output_type"     -> outputType.toString,
      "output_class"    -> Encoders.product[T].schema.toDDL
    )

  def ds[T <: Product: TypeTag](
      input: Dataset[T],
      outputType: IOType,
      outputLocation: String,
      saveMode: SaveMode = SaveMode.Append,
      partitionBy: Seq[String] = Seq.empty[String],
      outputFilename: Option[String] = None,
      compression: String = "none", // ("compression", "gzip","snappy")
      repartition: Boolean = false,
      repartitionNo: Int = 1
  )(spark: SparkSession): Unit = {
    val mapping = Encoders.product[T]

    logger.info("#" * 20 + " Actual Output Schema " + "#" * 20)
    input.schema.printTreeString()
    logger.info("#" * 20 + " Provided Output Case Class Schema " + "#" * 20)
    mapping.schema.printTreeString()

    val dfWriter: DataFrameWriter[T] = partitionBy match {
      case partition if partition.nonEmpty && repartition =>
        logger.info(s"Will generate $repartitionNo repartitioned output files inside partitions $partitionBy")
        input
          .select(mapping.schema.map(x => col(x.name)): _*)
          .as[T](mapping)
          .repartition(repartitionNo, partition.map(c => col(c)): _*)
          .write
          .option("compression", compression)
      case partition if partition.nonEmpty && !repartition =>
        logger.info(s"Will generate output files inside partitions $partitionBy")
        input
          .select(mapping.schema.map(x => col(x.name)): _*)
          .as[T](mapping)
          .repartition(partition.map(c => col(c)): _*)
          .write
          .option("compression", compression)
      case partition if partition.isEmpty && repartition =>
        logger.info(s"Will generate $repartitionNo repartitioned output files")
        input
          .select(mapping.schema.map(x => col(x.name)): _*)
          .as[T](mapping)
          .repartition(repartitionNo)
          .write
          .option("compression", compression)
      case _ =>
        input
          .select(mapping.schema.map(x => col(x.name)): _*)
          .as[T](mapping)
          .write
          .option("compression", compression)
    }

    val dfWriterOptions: DataFrameWriter[T] = outputType match {
      case CSV(delimiter, header_present, _, quotechar) =>
        dfWriter
          .format("csv")
          .option("delimiter", delimiter)
          .option("quote", quotechar)
          .option("header", header_present)
      case PARQUET          => dfWriter.format("parquet")
      case ORC              => dfWriter.format("orc")
      case JSON(multi_line) => dfWriter.format("json").option("multiline", multi_line)
      case TEXT             => dfWriter.format("text")
      case RDB(_, _)        => dfWriter
      case a                => throw EtlJobException(s"Unsupported output format $a")
    }

    partitionBy match {
      case partition if partition.nonEmpty =>
        outputType match {
          case RDB(_, _) => throw EtlJobException("Output partitioning with JDBC is not yet implemented")
          case _         => dfWriterOptions.partitionBy(partition: _*).mode(saveMode).save(outputLocation)
        }
      case _ =>
        outputType match {
          case RDB(jdbc, _) =>
            val prop = new java.util.Properties
            prop.setProperty("driver", jdbc.driver)
            prop.setProperty("user", jdbc.user)
            prop.setProperty("password", jdbc.password)
            dfWriterOptions.mode(saveMode).jdbc(jdbc.url, outputLocation, prop)
          case _ => dfWriterOptions.mode(saveMode).save(outputLocation)
        }
    }

    logger.info(
      s"Successfully wrote data in $outputType in location $outputLocation with SAVEMODE $saveMode Partitioned by $partitionBy"
    )

    outputFilename.foreach { output_file =>
      val path       = s"$outputLocation/"
      val fs         = FileSystem.get(new java.net.URI(path), spark.sparkContext.hadoopConfiguration)
      val fileStatus = fs.globStatus(new Path(path + "part*"))
      if (fileStatus.size > 1) {
        logger.error("multiple output files found, expected single file")
        throw new RuntimeException("multiple output files found, expected single file")
      }
      val fileName = fileStatus(0).getPath.getName
      val success  = fs.rename(new Path(path + fileName), new Path(path + output_file))
      if (success) logger.info(s"Renamed file path $path$fileName to $path$output_file")
      else logger.info(s"Failed to renamed file path $path$fileName to $path$output_file")
      fs.close()
    }
  }
}
