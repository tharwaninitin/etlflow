package etlflow.jobtests

import etlflow.utils.Configuration

trait ConfigHelper {
  val config = zio.Runtime.default.unsafeRun(Configuration.config)
}
