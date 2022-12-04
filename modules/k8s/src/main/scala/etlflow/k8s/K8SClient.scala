package etlflow.k8s

import com.coralogix.zio.k8s.client.config.httpclient.getHostnameVerificationDisabled
import com.coralogix.zio.k8s.client.config.{defaultConfigChain, k8sCluster, K8sClusterConfig, SSL}
import com.coralogix.zio.k8s.client.model.K8sCluster
import etlflow.http.HttpApi
import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.client3.SttpBackend
import zio.{Task, TaskLayer, ZIO, ZLayer}

object K8SClient {
  def apply(
      connectionTimeout: Long = 10000,
      logDetails: Boolean = false
  ): TaskLayer[K8sCluster with SttpBackend[Task, ZioStreams with WebSockets]] =
    defaultConfigChain >>> (k8sCluster ++ sttpClient(connectionTimeout, logDetails))

  private def sttpClient(
      connectionTimeout: Long,
      logDetails: Boolean
  ): ZLayer[K8sClusterConfig, Throwable, SttpBackend[Task, ZioStreams with WebSockets]] =
    ZLayer.scoped {
      for {
        config                      <- ZIO.service[K8sClusterConfig]
        disableHostnameVerification <- ZIO.succeed(getHostnameVerificationDisabled(config))
        _ <- ZIO
          .attempt(java.lang.System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true"))
          .when(disableHostnameVerification)
        sslContext <- SSL(config.client.serverCertificate, config.authentication)
        client <- HttpApi
          .getBackendWithSSLContext(connectionTimeout, sslContext)
          .map(backend => HttpApi.logBackend(backend, logDetails))
      } yield client
    }
}
