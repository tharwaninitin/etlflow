package etlflow

import zio.Has

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

package object crypto {

  type CryptoEnv = Has[CryptoApi.Service]

}
