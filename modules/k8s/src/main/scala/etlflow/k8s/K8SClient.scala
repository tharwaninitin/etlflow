package etlflow.k8s

import etlflow.log.ApplicationLogger
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}
import io.kubernetes.client.util.Config

object K8SClient extends ApplicationLogger {

  /** Method: batchClient - Provides BatchV1Api
    * @param httpConnectionTimeout
    *   Http request connection timeout in MILLISECONDS, A value of 0 means no timeout
    * @return
    *   BatchV1Api
    */
  def batchClient(httpConnectionTimeout: Int): BatchV1Api = {
    logger.info(s"HTTP Connection timeout is set to $httpConnectionTimeout")
    Configuration.setDefaultApiClient(Config.defaultClient.setConnectTimeout(httpConnectionTimeout))
    new BatchV1Api()
  }

  /** Method: coreClient - Provides CoreV1Api
    * @param httpConnectionTimeout
    *   Http request connection timeout in MILLISECONDS, A value of 0 means no timeout
    * @return
    *   CoreV1Api
    */
  def coreClient(httpConnectionTimeout: Int): CoreV1Api = {
    logger.info(s"HTTP Connection timeout is set to $httpConnectionTimeout")
    Configuration.setDefaultApiClient(Config.defaultClient.setConnectTimeout(httpConnectionTimeout))
    new CoreV1Api()
  }
}
