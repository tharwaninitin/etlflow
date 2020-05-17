package etlflow.utils

import scopt.OptionParser
import etlflow.{BuildInfo => BI}

object EtlJobArgsParser {
  case class EtlJobConfig(
                           list_jobs: Boolean = false,
                           default_values: Boolean = false,
                           actual_values: Boolean = false,
                           show_job_props: Boolean = false,
                           show_step_props: Boolean = false,
                           run_job: Boolean = false,
                           run_job_remote: Boolean = false,
                           job_name: String = "",
                           job_properties: Map[String,String] = Map.empty
                         )
  val parser: OptionParser[EtlJobConfig] = new OptionParser[EtlJobConfig]("etljobs-cli") {
    head(BI.name, BI.version, s"Build with scala version ${BI.scalaVersion}")
    help("help")
    opt[Unit]('l', "list_jobs")
      .action((_, c) => c.copy(list_jobs = true))
      .text("List jobs in etljobs")
    cmd("show_job_props")
      .action((_, c) => c.copy(show_job_props = true))
      .text("Show jobs props in etljobs")
      .children(
        opt[String]("job_name")
          .action((x, c) => c.copy(job_name = x))
          .text("job_name is a EtlJobName"),
        opt[Unit]('d',"default_values")
          .action((_, c) => c.copy(default_values = true))
          .text("Default Properties"),
        opt[Unit]('a',"actual_values")
          .action((_, c) => c.copy(actual_values = true))
          .text("Actual Properties"),
        opt[Map[String, String]]("props")
          .valueName("k1=v1,k2=v2...")
          .action((x, c) => c.copy(job_properties = x))
          .text("other arguments")
      )
    cmd("show_step_props")
      .action((_, c) => c.copy(show_step_props = true))
      .text("Show job step props in etljobs")
      .children(
        opt[String]("job_name")
          .action((x, c) => c.copy(job_name = x))
          .text("job_name is a EtlJobName"),
        opt[Map[String, String]]("props")
          .valueName("k1=v1,k2=v2...")
          .action((x, c) => c.copy(job_properties = x))
          .text("other arguments")
      )
    cmd("run_job")
      .action((_, c) => c.copy(run_job = true))
      .text("run job in etljobs")
      .children(
        opt[String]("job_name")
          .action((x, c) => c.copy(job_name = x))
          .text("job_name is a EtlJobName"),
        opt[Map[String, String]]("props")
          .valueName("k1=v1,k2=v2...")
          .action((x, c) => c.copy(job_properties = x))
          .text("other arguments")
      )
    cmd("run_job_remote")
      .action((_, c) => c.copy(run_job_remote = true))
      .text("Submit job to cluster")
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
