package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.db.DBApi
import etlflow.json.{Implementation, JsonService}
import etlflow.utils.{Encryption, LoggingLevel}
import io.circe.Decoder
import zio.RIO
import scala.reflect.runtime.universe.TypeTag

case class GetCredentialStep[T : TypeTag : Decoder](name: String, credential_name: String) extends EtlStep[Unit,T] {

  override def process(input_state: => Unit): RIO[JobEnv,T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    for {
      result <- DBApi.executeQueryWithSingleResponse[String](query)
      dValue <- Encryption.getDecreptValues[T](result)
      op     <- JsonService.convertToObject[T](dValue.toString()).provideLayer(Implementation.live)
    } yield op
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
