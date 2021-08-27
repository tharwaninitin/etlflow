package etlflow.utils

import etlflow.{BuildInfo => BI}
import scopt.OptionParser

private[etlflow] object CliArgsCoreParserAPI {
  case class EtlJobCoreConfig(
       list_jobs: Boolean = false,
       show_job_props: Boolean = false,
       show_step_props: Boolean = false,
       run_job: Boolean = false,
       job_name: String = "",
       job_properties: Map[String,String] = Map.empty,
     )
  val parser: OptionParser[EtlJobCoreConfig] = new OptionParser[EtlJobCoreConfig]("etlflow") {
    head("EtlFlow", BI.version, s"Build with scala version ${BI.scalaVersion}")
    help("help")
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
