package etlflow.utils

import scopt.OptionParser
import etlflow.{BuildInfo => BI}

private[etlflow] object CliArgsServerParserAPI {
  case class EtlJobServerConfig(
       init_db: Boolean = false,
       reset_db: Boolean = false,
       list_jobs: Boolean = false,
       show_job_props: Boolean = false,
       show_step_props: Boolean = false,
       run_job: Boolean = false,
       run_server: Boolean = false,
       job_name: String = "",
       job_properties: Map[String,String] = Map.empty,
       add_user: Boolean = false,
       user: String = "",
       password: String = "",
     )
  val parser: OptionParser[EtlJobServerConfig] = new OptionParser[EtlJobServerConfig]("etlflow") {
    head("EtlFlow", BI.version, s"Build with scala version ${BI.scalaVersion}")
    help("help")
    cmd("initdb")
      .action((_, c) => c.copy(init_db = true))
      .text("Initialize Database")
    cmd("resetdb")
      .action((_, c) => c.copy(reset_db = true))
      .text("Initialize Database")
    cmd("list_jobs")
      .action((_, c) => c.copy(list_jobs = true))
      .text("List all jobs")
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
          .text("other arguments")
      )
    cmd("run_server")
      .action((_, c) => c.copy(run_server = true))
      .text("Start Server")
    cmd("add_user")
      .action((_, c) => c.copy(add_user = true))
      .text("Add user into database")
      .children(
        opt[String]("user")
          .action((x, c) => c.copy(user = x))
          .text("user"),
        opt[String]("password")
          .action((x, c) => c.copy(password = x))
          .text("password"),
      )
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
            .text("other arguments")
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
            .text("other arguments")
        )
  }
}
