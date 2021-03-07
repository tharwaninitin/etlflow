package etlflow.utils

import scopt.OptionParser
import etlflow.{BuildInfo => BI}

object EtlJobArgsParser {
  case class EtlJobConfig(
       list_jobs: Boolean = false,
       show_job_props: Boolean = false,
       show_step_props: Boolean = false,
       run_job: Boolean = false,
       job_name: String = "",
       job_properties: Map[String,String] = Map.empty,
       run_db_migration:Boolean = false,
       add_user: Boolean = false,
       user: String = "",
       password: String = ""
     )
  val parser: OptionParser[EtlJobConfig] = new OptionParser[EtlJobConfig]("etlflow-cli") {
    head("EtlFlow", BI.version, s"Build with scala version ${BI.scalaVersion}")
    help("help")
    opt[Unit]('l', "list_jobs")
      .action((_, c) => c.copy(list_jobs = true))
      .text("List all jobs \n")
    cmd("run_db_migration")
      .action((_, c) => c.copy(run_db_migration = true))
      .text("Run Database Migration \n")
    cmd("show_job_props")
      .action((_, c) => c.copy(show_job_props = true))
      .text("Show job props")
      .children(
        opt[String]("job_name")
          .action((x, c) => c.copy(job_name = x))
          .text("job_name is a EtlJobName"),
        opt[Map[String, String]]("props")
          .valueName("k1=v1,k2=v2...")
          .action((x, c) => c.copy(job_properties = x))
          .text("other arguments \n")
      )
    cmd("show_step_props")
      .action((_, c) => c.copy(show_step_props = true))
      .text("Show job step props")
      .children(
        opt[String]("job_name")
          .action((x, c) => c.copy(job_name = x))
          .text("job_name is a EtlJobName"),
        opt[Map[String, String]]("props")
          .valueName("k1=v1,k2=v2...")
          .action((x, c) => c.copy(job_properties = x))
          .text("other arguments \n")
      )
    cmd("run_job")
      .action((_, c) => c.copy(run_job = true))
      .text("Run job")
      .children(
        opt[String]("job_name")
          .action((x, c) => c.copy(job_name = x))
          .text("job_name is a EtlJobName"),
        opt[Map[String, String]]("props")
          .valueName("k1=v1,k2=v2...")
          .action((x, c) => c.copy(job_properties = x))
          .text("other arguments \n")
      )
    cmd("add_user")
      .action((_, c) => c.copy(add_user = true))
      .text("Insert user into database")
      .children(
        opt[String]("user")
          .action((x, c) => c.copy(user = x))
          .text("user"),
        opt[String]("password")
          .action((x, c) => c.copy(password = x))
          .text("password \n"),
      )
  }
}
