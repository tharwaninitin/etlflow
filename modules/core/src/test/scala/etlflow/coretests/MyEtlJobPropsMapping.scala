package etlflow.coretests

import etlflow.coretests.Schema._
import etlflow.coretests.jobs._
import etlflow.etljobs.EtlJob
import etlflow.schema.Executor
import etlflow.schema.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}
import etlflow.{EtlJobProps, EtlJobPropsMapping}

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {

  val kubernetes = KUBERNETES(
    "etlflow:0.10.0",
    "default",
    Map(
      "GOOGLE_APPLICATION_CREDENTIALS"-> Option("<cred_file>"),
      "LOG_DB_URL"-> Option("jdbc:postgresql://host.docker.internal:5432/postgres"),
      "LOG_DB_USER"-> Option("<username>"),
      "LOG_DB_PWD"-> Option("<pwd>"),
      "LOG_DB_DRIVER"-> Option("org.postgresql.Driver")
    )
  )

  val dataproc   = DATAPROC("project-name","region","endpoint","cluster-name")

  // https://www.scala-sbt.org/sbt-native-packager/archetypes/java_app/index.html#
  val local_subprocess: LOCAL_SUBPROCESS = LOCAL_SUBPROCESS("examples/target/universal/stage/bin/etlflow-examples")

  case object Job1 extends MyEtlJobPropsMapping[EtlJob1Props,Job1HelloWorld] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
    override val job_schedule: String = "0 */2 * * * ?"
  }

  case object Job2 extends MyEtlJobPropsMapping[EtlJob2Props,Job2Retry] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob2Props = EtlJob2Props(ratings_output_table_name = job_properties("ratings_output_table_name"))
    override val job_schedule: String = "0 */15 * * * ?"
    override val job_max_active_runs: Int = 1
    override val job_deploy_mode: Executor = kubernetes
    override val job_enable_db_logging: Boolean = false
  }

  case object Job3 extends MyEtlJobPropsMapping[EtlJob4Props,Job3DBSteps] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
    override val job_schedule: String = "0 30 7 ? * *"
    override val job_deploy_mode: Executor = Executor.LOCAL
  }

  case object Job4 extends MyEtlJobPropsMapping[EtlJob5Props,Job4GenericSteps] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob5Props = EtlJob5Props()
    override val job_schedule: String = "0 0 11 ? * 4"
    override val job_deploy_mode: Executor = Executor.LOCAL
    override val job_retries: Int = 3
    override val job_retry_delay_in_minutes: Int = 1

  }

  case object Job5 extends MyEtlJobPropsMapping[EtlJob1Props,Job5EtlflowJobSteps] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
  }
}

