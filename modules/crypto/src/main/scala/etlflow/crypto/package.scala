package etlflow

import zio.Has

package object crypto {

  type CryptoEnv = Has[CryptoApi.Service]

}
