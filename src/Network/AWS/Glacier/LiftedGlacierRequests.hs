{-# LANGUAGE TemplateHaskell #-}
module LiftedGlacierRequests (GlacierSettings(..), HasGlacierSettings(..), PartSize, getNumBytes, NumBytes, UploadId, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256) where

import Control.Lens (Lens', view, makeLenses)

import Control.Monad.Trans.AWS (AWSConstraint, HasEnv(..), Env)
import Network.AWS.Glacier (ArchiveCreationOutput, UploadMultipartPartResponse)

import GlacierRequests (PartSize, getNumBytes, NumBytes, UploadId, Digest, SHA256, ToHashedBody)
import qualified GlacierRequests

import Data.Text (Text)

data GlacierSettings = GlacierSettings {
  _accountId :: Text,
  _vaultName :: Text,
  _partSize  :: PartSize
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

instance HasGlacierSettings GlacierEnv where
  glacierSettingsL = glacierSettings

initiateMultipartUpload :: (AWSConstraint r m, HasGlacierSettings r)
       => Maybe Text 
       -> m UploadId
initiateMultipartUpload archiveDescription = do
  GlacierSettings accountId vaultName partSize <- view glacierSettingsL
  GlacierRequests.initiateMultipartUpload accountId vaultName archiveDescription partSize 

completeMultipartUpload :: (AWSConstraint r m, HasGlacierSettings r)
       => UploadId 
       -> NumBytes
       -> Digest SHA256
       -> m Text
completeMultipartUpload  uploadId totalArchiveSize treeHashChecksum = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.completeMultipartUpload accountId vaultName uploadId totalArchiveSize treeHashChecksum

uploadMultipartPart :: (AWSConstraint r m, HasGlacierSettings r, ToHashedBody a) => UploadId -> (NumBytes, NumBytes) -> Digest SHA256 -> a -> m UploadMultipartPartResponse
uploadMultipartPart uploadId byteRange checksum body  = do 
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.uploadMultipartPart accountId vaultName uploadId byteRange checksum body
