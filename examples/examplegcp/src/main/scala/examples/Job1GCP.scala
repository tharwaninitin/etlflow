package examples

import etlflow.log.ApplicationLogger
import etlflow.task._
import gcp4zio.dp._
import zio._

// export GOOGLE_APPLICATION_CREDENTIALS=
// export GCP_PROJECT=
// export GCP_REGION=
// export DP_CLUSTER=
// export DP_ENDPOINT=
// export DP_BUCKET=

object Job1GCP extends ZIOAppDefault with ApplicationLogger {
  override val bootstrap: ULayer[Unit] = zioSlf4jLogger

  private val gcpProject: String = sys.env("GCP_PROJECT")
  private val gcpRegion: String  = sys.env("GCP_REGION")
  private val dpCluster: String  = sys.env("DP_CLUSTER")
  private val dpEndpoint: String = sys.env("DP_ENDPOINT")
  private val dpBucket: String   = sys.env("DP_BUCKET")

  private val createCluster = DPCreateTask("DPCreateTask", dpCluster, ClusterProps(dpBucket)).execute

  private val deleteCluster = DPDeleteTask("DPDeleteTask", dpCluster).execute

  private val args      = List("1000")
  private val mainClass = "org.apache.spark.examples.SparkPi"
  private val libs      = List("file:///usr/lib/spark/examples/jars/spark-examples.jar")
  private val conf      = Map("spark.executor.memory" -> "1g", "spark.driver.memory" -> "1g")

  private val sparkJob = DPSparkJobTask("DPSparkJobTask", args, mainClass, libs, conf).execute

  private val program = for {
    _ <- createCluster
    _ <- sparkJob
    _ <- deleteCluster
  } yield ()

  private val dpJobLayer = DPJob.live(dpCluster, gcpProject, gcpRegion, dpEndpoint)

  private val dpClusterLayer = DPCluster.live(gcpProject, gcpRegion, dpEndpoint)

  override def run: Task[Unit] = program.provide(dpJobLayer ++ dpClusterLayer ++ etlflow.audit.test)
}
