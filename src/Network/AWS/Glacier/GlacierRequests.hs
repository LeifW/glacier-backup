{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, BangPatterns, ConstraintKinds #-}
--module GlacierRequests (NumBytes, PartSize(getNumBytes), UploadId(getAsText), createVault, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody) where
module GlacierRequests (NumBytes, PartSize, getNumBytes, UploadId(..), JobId(..), ArchiveId, InitiateJobResponse, GetJobOutputResponse, JobParameters, createVault, deleteArchive, archiveRetrievalJob, inventoryRetrievalJob, selectJob, getJobOutput, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody, MonadUnliftIO, MonadCatch, MonadReader, HasEnv, bulk, expedited, standard) where

import Control.Monad.IO.Class
import Data.Word (Word64)

import Control.Monad.Trans.AWS (AWSConstraint, HasEnv, send, runResourceT)
import qualified Network.AWS.Glacier as Amazonka
import Control.Monad.IO.Unlift
import Control.Monad.Reader.Class
import Control.Monad.Catch
import Network.AWS.Glacier (JobParameters, InitiateJobResponse, SelectParameters, GetJobOutputResponse, imuArchiveDescription, imursUploadId, acoArchiveId)
import Network.AWS.Glacier.Types
import Network.AWS.Data.Text (ToText(toText), FromText(parser), takeText)
import Network.AWS.Data.Body (ToHashedBody, toHashed)
import Network.AWS.Data.Crypto (Digest, SHA256)

import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.ToField

import Data.Text (Text)

import Control.Lens

import Formatting

import AmazonkaSupport ()

import AllowedPartSizes (PartSize, getNumBytes)

type NumBytes = Word64

newtype UploadId = UploadId {uploadIdAsText :: Text} deriving (ToField, FromField)

newtype JobId = JobId Text deriving (ToField, FromField)

newtype ArchiveId = ArchiveId Text deriving (Show, Eq, ToField, FromField)

instance ToText ArchiveId where toText (ArchiveId t) = t
instance FromText ArchiveId where parser = ArchiveId <$> takeText


{-
instance ToField UploadId where
  toField = toField . getAsText

instance FromField UploadId where
  fromField = fmap UploadId . fromField
-}

type AWSMonads r m = (MonadUnliftIO m, MonadCatch m,  MonadReader r m, HasEnv r)

createVault :: (MonadUnliftIO m, MonadCatch m,  MonadReader r m, HasEnv r) => Text -> Text -> m ()
createVault accountId vaultName = do
  resp <- runResourceT $ send $ Amazonka.createVault accountId vaultName
  pure ()

deleteArchive :: (MonadUnliftIO m, MonadCatch m,  MonadReader r m, HasEnv r) => Text -> Text -> ArchiveId -> m ()
deleteArchive accountId vaultName (ArchiveId archiveId) = do
  resp <- runResourceT $ send $ Amazonka.deleteArchive accountId vaultName archiveId
  pure ()

initiateJob :: (AWSMonads r m) => Text -> Text -> JobParameters -> m InitiateJobResponse
initiateJob accountId vaultName jobParams = do
  runResourceT $ send $ Amazonka.initiateJob accountId vaultName jobParams

archiveRetrievalJob :: (AWSMonads r m) => Text -> Text -> ArchiveId -> (JobParameters -> JobParameters) -> m InitiateJobResponse
archiveRetrievalJob accountId vaultName (ArchiveId archiveId) settings = initiateJob accountId vaultName $ jpArchiveId ?~ archiveId $ settings $ jobParameters ArchiveRetrieval

inventoryRetrievalJob :: (AWSMonads r m) => Text -> Text -> (JobParameters -> JobParameters) -> m InitiateJobResponse
inventoryRetrievalJob accountId vaultName settings = initiateJob accountId vaultName $ settings $ jobParameters InventoryRetrieval

selectJob :: (AWSMonads r m) => Text -> Text -> SelectParameters -> (JobParameters -> JobParameters) -> m InitiateJobResponse
selectJob accountId vaultName selectParams settings = initiateJob accountId vaultName $ jpSelectParameters ?~ selectParams $ settings $ jobParameters Select

bulk, expedited, standard :: JobParameters -> JobParameters
bulk = jpTier ?~ TBulk
expedited = jpTier ?~ TExpedited
standard = jpTier ?~ TStandard

getJobOutput :: (AWSConstraint r m) => Text -> Text -> JobId -> m GetJobOutputResponse
getJobOutput accountId vaultName (JobId jid) = do
  send $ Amazonka.getJobOutput accountId vaultName jid

initiateMultipartUpload :: (AWSMonads r m)
       => Text 
       -> Text 
       -> Maybe Text 
       -> PartSize
       -> m UploadId
initiateMultipartUpload accountId vaultName archiveDescription partSize = do
  resp <- runResourceT $ send $ set imuArchiveDescription archiveDescription $ Amazonka.initiateMultipartUpload accountId vaultName (toText $ getNumBytes partSize)
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
  -- ensure umprsResponseStatus == 204, umprsChecksum == treeHashChecksum
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
  pure ()

rangeHeader :: (NumBytes, NumBytes) -> Text
rangeHeader (start, end) = sformat ("bytes " % int % "-" % int % "/*") start end
