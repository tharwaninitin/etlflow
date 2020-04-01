package etljobs.utils

import scopt.OptionParser

object EtlJobArgsParser {
  case class EtlJobConfig(
                           list_jobs: Boolean = false,
                           show_job_props: Boolean = false,
                           show_job_default_props: Boolean = false,
                           show_step_props: Boolean = false,
                           run_job: Boolean = false,
                           job_name: String = "",
                           job_properties: Map[String,String] = Map.empty
                         )
  val parser: OptionParser[EtlJobConfig] = new OptionParser[EtlJobConfig]("etljobs-cli") {
    head("etljobs", "0.7.6")
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
          .text("job_name is a EtlJobName")
      )
    cmd("show_job_default_props")
      .action((_, c) => c.copy(show_job_default_props = true))
      .text("Show job default props in etljobs")
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
  }
}
