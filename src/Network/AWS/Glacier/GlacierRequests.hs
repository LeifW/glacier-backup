{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, BangPatterns, ConstraintKinds #-}
--module GlacierRequests (NumBytes, PartSize(getNumBytes), UploadId(getAsText), createVault, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody) where
module GlacierRequests (NumBytes, PartSize(getNumBytes), UploadId(..), JobId(..), InitiateJobResponse, GetJobOutputResponse, JobParameters, createVault, initiateJob, getJobOutput, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody, MonadUnliftIO, MonadCatch, MonadReader, HasEnv) where

import Control.Monad.IO.Class
import Data.Int (Int64)
import Data.Maybe (fromMaybe)

--import Control.Monad.Trans.AWS (AWSConstraint, send, runResourceT)
import Control.Monad.Trans.AWS
import qualified Network.AWS.Glacier as Amazonka
import Control.Monad.IO.Unlift
import Control.Monad.Reader.Class
import Control.Monad.Catch
import Network.AWS.Glacier (JobParameters(..), InitiateJobResponse, GetJobOutputResponse, imuArchiveDescription, imursUploadId, acoArchiveId)
import Network.AWS.Data.Text (toText)
import Network.AWS.Data.Body (ToHashedBody, toHashed)
import Network.AWS.Data.Crypto (Digest, SHA256)

import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.ToField

import Data.Text (Text)

import Control.Lens -- (set)

import Formatting

import AllowedPartSizes (PartSize, getNumBytes)

import Control.DeepSeq

type NumBytes = Int64

newtype UploadId = UploadId {getAsText :: Text} deriving (ToField, FromField)

newtype JobId = JobId Text deriving (ToField, FromField)

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

initiateJob :: (AWSMonads r m) => Text -> Text -> JobParameters -> m InitiateJobResponse
initiateJob accountId vaultName jobParams = do
  runResourceT $ send $ Amazonka.initiateJob accountId vaultName jobParams

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
  resp <- runResourceT $ send $ set imuArchiveDescription archiveDescription $ Amazonka.initiateMultipartUpload accountId vaultName (toText partSize)
  pure $ UploadId $ resp ^. imursUploadId

completeMultipartUpload :: (AWSMonads r m)
       => Text 
       -> Text 
       -> UploadId 
       -> NumBytes
       -> Digest SHA256
       -> m Text
completeMultipartUpload  accountId vaultName (UploadId uploadId) totalArchiveSize treeHashChecksum = do
  resp <- runResourceT $ send $ Amazonka.completeMultipartUpload accountId vaultName uploadId (toText totalArchiveSize) (toText treeHashChecksum)
  -- ensure umprsResponseStatus == 204, umprsChecksum == treeHashChecksum
  pure $ resp ^. acoArchiveId

uploadMultipartPart :: (AWSMonads r m, ToHashedBody a) => Text -> Text -> UploadId -> (NumBytes, NumBytes) -> Digest SHA256 -> a -> m ()
uploadMultipartPart !accountId !vaultName (UploadId !uploadId) !byteRange !checksum !body  = do
  !resp <- runResourceT $ send $ Amazonka.uploadMultipartPart
    accountId
    vaultName
    uploadId
    (rangeHeader byteRange)
    (toText checksum)
    (toHashed body)
  liftIO $ print resp
  pure ()

rangeHeader :: (NumBytes, NumBytes) -> Text
rangeHeader (start, end) = sformat ("bytes " % int % "-" % int % "/*") start end
