{-# LANGUAGE TemplateHaskell, BangPatterns, ConstraintKinds #-}
module LiftedGlacierRequests (GlacierSettings(..), GlacierEnv(..), HasGlacierSettings(..), GlacierConstraint, PartSize, partSizeInBytes, NumBytes, UploadId(uploadIdAsText), ArchiveId, JobDescription(..), JobId, GlacierUpload(..), createVault, deleteArchive, initiateMultipartUpload, archiveRetrievalJob, inventoryRetrievalJob, selectJob, getJobOutput, bulk, expedited, standard, listJobs, saveJobOutput, jobOutputToStdout, completeMultipartUpload, uploadMultipartPart, Digest, SHA256) where

import Data.ByteString (ByteString)
import Control.Lens --(Lens', view, makeLenses, set)

import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource (ResourceT, liftResourceT)
import Control.Monad.Trans.AWS (AWSConstraint, HasEnv(..), Env, runResourceT)
import Network.AWS.Data.Body (_streamBody)
import Data.Conduit (ConduitT, runConduit, Void, (.|), transPipe)
import Data.Conduit.Combinators (sinkFile, stdout)

import Control.Monad.IO.Unlift
import Control.Monad.Reader.Class
import Control.Monad.Catch

import GlacierRequests (GetJobOutputResponse, PartSize, partSizeInBytes, NumBytes, UploadId(uploadIdAsText), ArchiveId, GlacierUpload(..), JobDescription(..), JobId, Digest, SHA256, ToHashedBody, bulk, expedited, standard)
import qualified GlacierRequests

import Network.AWS.Glacier.Types
import Network.AWS.Glacier (gjorsBody)

import Data.Text (Text)

type GlacierConstraint r m = (MonadUnliftIO m, MonadCatch m, MonadReader r m, HasEnv r, HasGlacierSettings r)

data GlacierSettings = GlacierSettings {
  _accountId :: !Text,
  _vaultName :: !Text,
  _partSize  :: !PartSize
} deriving Show

class HasGlacierSettings r where
  glacierSettingsL :: Lens' r GlacierSettings

instance HasGlacierSettings GlacierSettings where
  glacierSettingsL = id

data GlacierEnv = GlacierEnv {
  _awsEnv :: !Env,
  _glacierSettings :: !GlacierSettings
}

makeLenses ''GlacierEnv

instance HasEnv GlacierEnv  where
  environment = awsEnv

instance HasGlacierSettings GlacierEnv where
  glacierSettingsL = glacierSettings

listJobs :: (GlacierConstraint r m) => m [JobDescription]
listJobs = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.listJobs accountId vaultName

getJobOutput :: (AWSConstraint r m, HasGlacierSettings r) => JobId -> m GetJobOutputResponse
getJobOutput jobId = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.getJobOutput accountId vaultName jobId

jobOutputSource :: (AWSConstraint r m, HasGlacierSettings r) => JobId -> ConduitT () ByteString m ()
jobOutputSource jobId = do
  jobOutput <- lift $ getJobOutput jobId
  let body = jobOutput ^. gjorsBody
  transPipe liftResourceT $ _streamBody body

saveJobOutput :: (GlacierConstraint r m) => FilePath -> JobId -> m ()
saveJobOutput fileName = jobOutputToSink $ sinkFile fileName

jobOutputToStdout :: (GlacierConstraint r m) => JobId -> m ()
jobOutputToStdout = jobOutputToSink stdout
  --let body = _streamBody $ jobOutput ^. gjorsBody
  --liftIO $ runResourceT $ runConduit $ body .| sinkFile fileName
  
jobOutputToSink :: (GlacierConstraint r m) => ConduitT ByteString Void (ResourceT m) a -> JobId -> m a
jobOutputToSink sink jobId = runResourceT $ runConduit $ jobOutputSource jobId .| sink
{-
jobOutputToSink :: (GlacierConstraint r m) => ConduitT ByteString Void (ResourceT IO) a -> JobId -> m a
jobOutputToSink sink jobId = runResourceT $ do
  jobOutput <- getJobOutput jobId
  let body = jobOutput ^. gjorsBody
  sinkBody body sink
-}

archiveRetrievalJob :: (GlacierConstraint r m) => Tier -> ArchiveId -> Maybe Text -> m JobId
archiveRetrievalJob tier archiveId jobDescription = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.archiveRetrievalJob accountId vaultName tier archiveId jobDescription

inventoryRetrievalJob :: (GlacierConstraint r m) => Tier -> Maybe Text -> (JobParameters -> JobParameters) -> m JobId
inventoryRetrievalJob tier jobDescription settings = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.inventoryRetrievalJob accountId vaultName tier jobDescription settings

selectJob :: (GlacierConstraint r m) => Tier -> Maybe Text -> SelectParameters -> (JobParameters -> JobParameters) -> m JobId
selectJob selectParams tier jobDescription settings = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.selectJob accountId vaultName selectParams tier jobDescription settings

deleteArchive :: (GlacierConstraint r m) => ArchiveId -> m ()
deleteArchive archiveId = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.deleteArchive accountId vaultName archiveId

createVault :: (GlacierConstraint r m) => m ()
createVault = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.createVault accountId vaultName

initiateMultipartUpload :: (GlacierConstraint r m)
       => Maybe Text 
       -> m UploadId
initiateMultipartUpload archiveDescription = do
  GlacierSettings accountId vaultName partSize <- view glacierSettingsL
  GlacierRequests.initiateMultipartUpload accountId vaultName archiveDescription partSize 

completeMultipartUpload :: (GlacierConstraint r m)
       => UploadId 
       -> NumBytes
       -> Digest SHA256
       -> m ArchiveId
completeMultipartUpload  uploadId totalArchiveSize treeHashChecksum = do
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.completeMultipartUpload accountId vaultName uploadId totalArchiveSize treeHashChecksum

uploadMultipartPart :: (GlacierConstraint r m, ToHashedBody a) => UploadId -> (NumBytes, NumBytes) -> Digest SHA256 -> a -> m ()
uploadMultipartPart !uploadId !byteRange !checksum !body  = do 
  GlacierSettings accountId vaultName _ <- view glacierSettingsL
  GlacierRequests.uploadMultipartPart accountId vaultName uploadId byteRange checksum body
