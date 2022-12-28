package etlflow.k8s

import etlflow.log.ApplicationLogger
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.util.Config

object K8SClient extends ApplicationLogger {
  def batchClient(httpConnectionTimeout: Long, logRequestResponse: Boolean): BatchV1Api = {
    logger.warn(s"$logRequestResponse, $httpConnectionTimeout")
    Configuration.setDefaultApiClient(Config.defaultClient)
    new BatchV1Api()
  }
}
