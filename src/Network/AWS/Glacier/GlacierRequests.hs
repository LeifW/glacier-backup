{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, BangPatterns, ConstraintKinds #-}
module GlacierRequests (NumBytes, PartSize, partSizeInBytes, UploadId(..), JobId(..), JobDescription(..), ArchiveId, GlacierUpload(..), InitiateJobResponse, GetJobOutputResponse, JobParameters, createVault, deleteArchive, archiveRetrievalJob, inventoryRetrievalJob, selectJob, getJobOutput, listJobs, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody, MonadUnliftIO, MonadCatch, MonadReader, HasEnv, bulk, expedited, standard) where

import Data.Word (Word64)

import Control.Monad.Trans.AWS (AWSConstraint, HasEnv, send, runResourceT)
import Data.Maybe (fromJust)
import Control.Monad (void)
import Control.Monad.IO.Unlift
import Control.Monad.Reader.Class
import Control.Monad.Catch
import qualified Network.AWS.Glacier as Amazonka
import Network.AWS.Glacier (JobParameters, InitiateJobResponse, SelectParameters, GetJobOutputResponse, imuArchiveDescription, imursUploadId, imursResponseStatus, acoArchiveId, ljrsJobList, ijrsJobId, umprsResponseStatus, umprsChecksum, cvrsResponseStatus, ijrsResponseStatus)
import Network.AWS.Glacier.Types
import Network.AWS.Data.Text (ToText(toText), FromText(parser), takeText)
import Network.AWS.Data.Body (ToHashedBody, toHashed)
import Network.AWS.Data.Crypto (Digest, SHA256)

import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.ToField

import Data.Text (Text)

import Control.Lens

import Formatting

import Util (ensureIs)
import AmazonkaSupport (digestFromHexThrow, ensure201Response, ensure202Response, ensure204Response)

import AllowedPartSizes (PartSize, partSizeInBytes)

type NumBytes = Word64

newtype UploadId = UploadId {uploadIdAsText :: Text} deriving (ToField, FromField)

newtype JobId = JobId Text deriving (Show, ToField, FromField)

newtype ArchiveId = ArchiveId Text deriving (Show, Eq, Ord, ToField, FromField)

instance ToText ArchiveId where toText (ArchiveId t) = t
instance FromText ArchiveId where parser = ArchiveId <$> takeText

data GlacierUpload = GlacierUpload {
  _archiveId :: !ArchiveId,
  _treeHashChecksum :: !(Digest SHA256),
  _size :: !NumBytes
} deriving Show

data JobDescription = ArchiveRetrievalJob {
  _jobId :: JobId,
  _jobStatus :: StatusCode,
  _jobDescription :: Maybe Text,
  _glacierUpload :: GlacierUpload
} deriving Show
  
{-
instance ToField UploadId where
  toField = toField . getAsText

instance FromField UploadId where
  fromField = fmap UploadId . fromField
-}

type AWSMonads r m = (MonadUnliftIO m, MonadCatch m, MonadReader r m, HasEnv r)

createVault :: (MonadUnliftIO m, MonadCatch m,  MonadReader r m, HasEnv r) => Text -> Text -> m ()
createVault accountId vaultName = do
  resp <- runResourceT $ send $ Amazonka.createVault accountId vaultName
  ensure201Response cvrsResponseStatus resp

deleteArchive :: (MonadUnliftIO m, MonadCatch m,  MonadReader r m, HasEnv r) => Text -> Text -> ArchiveId -> m ()
deleteArchive accountId vaultName (ArchiveId archiveId) =
  void $ runResourceT $ send $ Amazonka.deleteArchive accountId vaultName archiveId
 -- "Delete archive doesn't currently return anything useful in amazonka types"

initiateJob :: (AWSMonads r m) => Text -> Text -> Type -> Tier -> Maybe Text -> (JobParameters -> JobParameters) -> m JobId
initiateJob accountId vaultName jobType tier jobDescription settings = do
  jobResponse <- runResourceT $ send $ Amazonka.initiateJob accountId vaultName $ set jpDescription jobDescription $ jpTier ?~ tier $ settings $ jobParameters jobType
  ensure202Response ijrsResponseStatus jobResponse
  pure $ JobId $ fromJust $ view ijrsJobId jobResponse

archiveRetrievalJob :: (AWSMonads r m) => Text -> Text -> Tier -> ArchiveId -> Maybe Text -> m JobId
archiveRetrievalJob accountId vaultName tier (ArchiveId archiveId) jobDescription = initiateJob accountId vaultName ArchiveRetrieval tier jobDescription $ jpArchiveId ?~ archiveId 

inventoryRetrievalJob :: (AWSMonads r m) => Text -> Text -> Tier -> Maybe Text -> (JobParameters -> JobParameters) -> m JobId
inventoryRetrievalJob accountId vaultName tier jobDescription settings = initiateJob accountId vaultName InventoryRetrieval tier jobDescription settings

selectJob :: (AWSMonads r m) => Text -> Text -> Tier -> Maybe Text -> SelectParameters -> (JobParameters -> JobParameters) -> m JobId
selectJob accountId vaultName tier jobDescription selectParams settings = initiateJob accountId vaultName Select tier jobDescription $ (jpSelectParameters ?~ selectParams) . settings 

bulk, expedited, standard :: JobParameters -> JobParameters
bulk = jpTier ?~ TBulk
expedited = jpTier ?~ TExpedited
standard = jpTier ?~ TStandard

getJobOutput :: (AWSConstraint r m) => Text -> Text -> JobId -> m GetJobOutputResponse
getJobOutput accountId vaultName (JobId jid) =
  send $ Amazonka.getJobOutput accountId vaultName jid

listJobs :: (AWSMonads  r m) => Text -> Text -> m [JobDescription]
listJobs accountId vaultName = do
  listJobsResponse <- runResourceT (send (Amazonka.listJobs accountId vaultName))
  traverse readJobDesc $ view ljrsJobList listJobsResponse

readJobDesc :: MonadThrow m => GlacierJobDescription -> m JobDescription
readJobDesc jobDesc = do
  checksum <- digestFromHexThrow $ fromJust $ view gjdArchiveSHA256TreeHash jobDesc
  pure $ ArchiveRetrievalJob 
    (JobId $ fromJust $ view gjdJobId jobDesc)
    (fromJust $ view gjdStatusCode jobDesc)
    (view gjdJobDescription jobDesc)
    (GlacierUpload (ArchiveId $ fromJust $ view gjdArchiveId jobDesc) checksum (fromIntegral $ fromJust $ view gjdArchiveSizeInBytes jobDesc))
  
initiateMultipartUpload :: (AWSMonads r m)
       => Text 
       -> Text 
       -> Maybe Text 
       -> PartSize
       -> m UploadId
initiateMultipartUpload accountId vaultName archiveDescription partSize = do
  resp <- runResourceT $ send $ set imuArchiveDescription archiveDescription $ Amazonka.initiateMultipartUpload accountId vaultName (toText $ partSizeInBytes partSize)
  ensure201Response imursResponseStatus resp
  pure $ UploadId $ resp ^. imursUploadId

completeMultipartUpload :: (AWSMonads r m)
       => Text 
       -> Text 
       -> UploadId 
       -> NumBytes
       -> Digest SHA256
       -> m ArchiveId
completeMultipartUpload  accountId vaultName (UploadId uploadId) totalArchiveSize treeHashChecksum = do
  resp <- runResourceT $ send $ Amazonka.completeMultipartUpload accountId vaultName uploadId (toText totalArchiveSize) (toText treeHashChecksum)
  digestFromHexThrow (fromJust $ view acoChecksum resp) >>= ensureIs "acoChecksum" treeHashChecksum
  pure $ ArchiveId $ resp ^. acoArchiveId

uploadMultipartPart :: (AWSMonads r m, ToHashedBody a) => Text -> Text -> UploadId -> (NumBytes, NumBytes) -> Digest SHA256 -> a -> m ()
uploadMultipartPart !accountId !vaultName (UploadId !uploadId) !byteRange !checksum !body  = do
  !resp <- runResourceT $ send $ Amazonka.uploadMultipartPart
    accountId
    vaultName
    uploadId
    (rangeHeader byteRange)
    (toText checksum)
    (toHashed body)
  ensure204Response umprsResponseStatus resp
  digestFromHexThrow (fromJust $ view umprsChecksum resp) >>= ensureIs "umprsChecksum" checksum

rangeHeader :: (NumBytes, NumBytes) -> Text
rangeHeader (start, end) = sformat ("bytes " % int % "-" % int % "/*") start end
