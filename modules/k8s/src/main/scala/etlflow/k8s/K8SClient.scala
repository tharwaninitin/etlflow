package etlflow.k8s

import etlflow.log.ApplicationLogger
import io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}
import io.kubernetes.client.openapi.{ApiClient, Configuration}
import io.kubernetes.client.util.{ClientBuilder, KubeConfig}
import zio.{Scope, ZIO}
import java.io.FileReader

object K8SClient extends ApplicationLogger {

  /** Method: createApiClient - Creates K8S ApiClient using either KUBECONFIG or HOME environment variable as the location for
    * configuration in this order <ul> <li>Config file defined by KUBECONFIG environment variable</li> <li>Config file defined by
    * HOME/.kube/config file</li> </ul>
    * @param httpConnectionTimeout
    *   Http request connection timeout in MILLISECONDS, A value of 0 means no timeout
    */
  def createApiClient(httpConnectionTimeout: Int): ZIO[Scope, Throwable, ApiClient] = ZIO.acquireRelease(ZIO.attempt {
    logger.info("Connecting to K8S using Kube Config")

    val configPath = sys.env.get("KUBECONFIG") match {
      case Some(value) => value
      case None =>
        sys.env.get("HOME") match {
          case Some(value) => s"$value/.kube/config"
          case None        => "NA"
        }
    }

    val config = KubeConfig.loadKubeConfig(new FileReader(configPath))
    logger.info(s"Config.CurrentContext => ${config.getCurrentContext}")
    logger.info(s"Config.Server => ${config.getServer}")

    val apiClient: ApiClient = ClientBuilder.kubeconfig(config).build().setConnectTimeout(httpConnectionTimeout)
    logger.info(s"ApiClient.HTTPConnectionTimeout => ${apiClient.getConnectTimeout} millis")

    apiClient
  })(c =>
    ZIO.attempt {
      val httpClient = c.getHttpClient
      logger.info(s"Closing K8S client connections")
      logger.info(s"HttpClient.ConnectionCount => ${httpClient.connectionPool.connectionCount}")
      logger.info(s"HttpClient.IdleConnectionCount => ${httpClient.connectionPool.idleConnectionCount}")
      httpClient.connectionPool.evictAll()
    }.orDie
  )

  def setDefaultApiClient(client: ApiClient): ApiClient = {
    Configuration.setDefaultApiClient(client)
    client
  }

  /** Method: batchClient - Provides BatchV1Api
    * @return
    *   BatchV1Api
    */
  def createBatchClient(client: ApiClient): BatchV1Api = new BatchV1Api(client)

  /** Method: coreClient - Provides CoreV1Api
    * @return
    *   CoreV1Api
    */
  def createCoreClient(client: ApiClient): CoreV1Api = new CoreV1Api(client)
}
