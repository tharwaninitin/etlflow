package etlflow

import etlflow.utils.ApplicationLogger
import zio.{Has, Task}

package object aws extends ApplicationLogger {
  type S3Env = Has[S3Api.Service[Task]]
}
