package etlflow.k8s

import etlflow.log.ApplicationLogger
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}
import io.kubernetes.client.util.Config

object K8SClient extends ApplicationLogger {

  /** Method: setConfig - Set configuration for K8S client
    * @param httpConnectionTimeout
    *   Http request connection timeout in MILLISECONDS, A value of 0 means no timeout
    */
  def setConfig(httpConnectionTimeout: Int): Unit = {
    logger.info(s"HTTP Connection timeout is set to $httpConnectionTimeout")
    Configuration.setDefaultApiClient(Config.defaultClient.setConnectTimeout(httpConnectionTimeout))
  }

  /** Method: batchClient - Provides BatchV1Api
    * @return
    *   BatchV1Api
    */
  def batchClient: BatchV1Api = new BatchV1Api()

  /** Method: coreClient - Provides CoreV1Api
    * @return
    *   CoreV1Api
    */
  def coreClient: CoreV1Api = new CoreV1Api()
}
