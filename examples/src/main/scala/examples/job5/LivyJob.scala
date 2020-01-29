package examples.job5

import java.io.{File, FileNotFoundException}
import java.net.URI
import etljobs.spark.ReadApi
import etljobs.utils.{CSV, GlobalProperties}
import org.apache.livy.LivyClientBuilder
import org.apache.livy.scalaapi._
import org.apache.livy.scalaapi.{LivyScalaClient, ScalaJobHandle}
import org.apache.spark.sql.SparkSession
import scala.concurrent.Await
import scala.concurrent.duration._

object LivyJob extends App {
    val url = sys.env.getOrElse("LIVY_URL","<livy_url_not_provided>")
    var scalaClient: LivyScalaClient = null
    val etlJobType = etljobs.etlsteps.SparkReadWriteStep // just for getting path
    case class Rating(user_id:Int, movie_id: Int, rating : Double, timestamp: Long)

    def uploadJar(path: String): Unit = {
        val file = new File(path)
        val uploadJarFuture = scalaClient.uploadJar(file)
        Await.result(uploadJarFuture, 100 second) match {
            case null => println("Successfully uploaded " + file.getName)
        }
    }

    def addJar(path: String): Unit = {
        val addJarFuture = scalaClient.addJar(new URI(path))
        Await.result(addJarFuture, 100 second) match {
            case null => println("Successfully added jar: " + path)
        }
    }

    def uploadFile(path: String): Unit = {
        val file = new File(path)
        val addJarFuture = scalaClient.uploadFile(file)
        Await.result(addJarFuture, 100 second) match {
            case null => println("Successfully uploaded file: " + path)
        }
    }

    @throws(classOf[FileNotFoundException])
    def getSourcePath(obj: Object): String = {
        val source = obj.getClass.getProtectionDomain.getCodeSource
        if (source != null && source.getLocation.getPath != "") {
        source.getLocation.getPath
        } else {
        throw new FileNotFoundException(s"Jar containing ${obj.getClass.getName} not found.")
        }
    }

    @throws(classOf[FileNotFoundException])
    def uploadRelevantJarsForJobExecution(): Unit = {
        val exampleAppJarPath = getSourcePath(this)
        println("exampleAppJarPath: " + exampleAppJarPath )
        val scalaApiJarPath = getSourcePath(scalaClient)
        println("scalaApiJarPath: " + scalaApiJarPath )
        val etlJobJarPath = getSourcePath(etlJobType)
        println("etlJobJarPath: " + etlJobJarPath )
        uploadJar(exampleAppJarPath)
        uploadJar(scalaApiJarPath)
        uploadJar(etlJobJarPath)
    }

    def stopClient(): Unit = {
        if (scalaClient != null) {
            scalaClient.stop(true)
            scalaClient = null
        }
    }

    def processJob(job_properties : Map[String,String]): ScalaJobHandle[Array[Rating]] = {
        scalaClient.submit { context =>
            val spark: SparkSession = context.sparkSession
            val etl_job = new EtlJobDefinition(job_properties)
            etl_job.execute(send_slack_notification = true, slack_notification_level = "info")
            val x = ReadApi.LoadDS[Rating](Seq(job_properties("ratings_input_path")), input_type = CSV())(spark)
//            x.write.mode("Overwrite").json(job_properties("ratings_output_path"))
            x.head(10)
        }
    }

    try {
        val job_properties : Map[String,String] = Map(
            "ratings_input_path" -> s"gs://${sys.env.getOrElse("GCS_OUTPUT_BUCKET","<no_bucket_provided>")}/input/ratings/*",
            "ratings_output_path" -> s"gs://${sys.env.getOrElse("GCS_OUTPUT_BUCKET","<no_bucket_provided>")}/output/ratings",
            "job_name" -> "TestSparkBQ",
            "ratings_output_dataset" -> "test",
            "ratings_output_file_name" -> "ratings.parquet",
            "ratings_output_table_name" -> "ratings"
        )
        val canonical_path = new java.io.File(".").getCanonicalPath + "/examples/src/main/resources/loaddata.properties"
        scalaClient = new LivyClientBuilder(false).setConf("kind", "spark").setURI(new URI(url)).build().asScalaClient

        // Upload and add files and jar
        uploadRelevantJarsForJobExecution()
        addJar("gs://<bucket_name>/jars/lib/bigquerylib.jar")
        uploadFile(canonical_path)

        println("Submitting Job now....")
        val handle = processJob(job_properties)
        println("Output of job => " + Await.result(handle, 100 second).toList)
    }
    finally {
        stopClient()
    }
}
