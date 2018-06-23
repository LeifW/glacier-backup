{-# LANGUAGE  TemplateHaskell #-}
module LiftedGlacierRequests (initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart) where

import Control.Lens (Lens', view, makeLenses)

import Control.Monad.Trans.AWS (AWSConstraint, HasEnv(..), Env)
import Network.AWS.Glacier (ArchiveCreationOutput, UploadMultipartPartResponse, InitiateMultipartUploadResponse)
import Network.AWS.Data.Body (ToHashedBody)
import Network.AWS.Data.Crypto (Digest, SHA256)

import qualified GlacierRequests

import Data.Text (Text)

data GlacierSettings = GlacierSettings {
  _accountId :: Text,
  _vaultName :: Text
} deriving Show

class HasGlacierSettings r where
  glacierSettingsL :: Lens' r GlacierSettings

instance HasGlacierSettings GlacierSettings where
  glacierSettingsL = id

data GlacierEnv = GlacierEnv {
  _awsEnv :: Env,
  _glacierSettings :: GlacierSettings
}

makeLenses ''GlacierEnv

instance HasEnv GlacierEnv  where
  environment = awsEnv

initiateMultipartUpload :: (AWSConstraint r m, HasGlacierSettings r)
       => Maybe Text 
       -> Int
       -> m InitiateMultipartUploadResponse
initiateMultipartUpload archiveDescription chunkSizeBytes = do
  GlacierSettings accountId vaultName <- view glacierSettingsL
  GlacierRequests.initiateMultipartUpload accountId vaultName archiveDescription chunkSizeBytes 

completeMultipartUpload :: (AWSConstraint r m, HasGlacierSettings r)
       => Text 
       -> Int
       -> Digest SHA256
       -> m ArchiveCreationOutput
completeMultipartUpload  uploadId totalArchiveSize treeHashChecksum = do
  GlacierSettings accountId vaultName <- view glacierSettingsL
  GlacierRequests.completeMultipartUpload accountId vaultName uploadId totalArchiveSize treeHashChecksum

uploadMultipartPart :: (AWSConstraint r m, HasGlacierSettings r, ToHashedBody a) => Text -> (Int, Int) -> Digest SHA256 -> a -> m UploadMultipartPartResponse
uploadMultipartPart uploadId byteRange checksum body  = do 
  GlacierSettings accountId vaultName <- view glacierSettingsL
  GlacierRequests.uploadMultipartPart accountId vaultName uploadId byteRange checksum body
