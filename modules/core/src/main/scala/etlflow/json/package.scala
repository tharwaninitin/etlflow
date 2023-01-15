package etlflow

import etlflow.json.JSON.jsonImpl
import zio.{ULayer, ZLayer}

package object json {
  val live: ULayer[JSON] = ZLayer.succeed(jsonImpl)
}
