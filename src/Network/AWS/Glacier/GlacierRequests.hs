{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, BangPatterns #-}
--module GlacierRequests (NumBytes, PartSize(getNumBytes), UploadId(getAsText), createVault, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody) where
module GlacierRequests (NumBytes, PartSize(getNumBytes), UploadId(..), createVault, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody) where

import Control.Monad.IO.Class
import Data.Int (Int64)
import Data.Maybe (fromMaybe)

import Control.Monad.Trans.AWS (AWSConstraint, send)
import qualified Network.AWS.Glacier as Amazonka
import Network.AWS.Glacier (imuArchiveDescription, imursUploadId, acoArchiveId)
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

{-
instance ToField UploadId where
  toField = toField . getAsText

instance FromField UploadId where
  fromField = fmap UploadId . fromField
-}


createVault :: (AWSConstraint r m) => Text -> Text -> m ()
createVault accountId vaultName = do
  resp <- send $ Amazonka.createVault accountId vaultName
  pure ()

initiateMultipartUpload :: (AWSConstraint r m)
       => Text 
       -> Text 
       -> Maybe Text 
       -> PartSize
       -> m UploadId
initiateMultipartUpload accountId vaultName archiveDescription partSize = do
  resp <- send $ set imuArchiveDescription archiveDescription $ Amazonka.initiateMultipartUpload accountId vaultName (toText partSize)
  pure $ UploadId $ fromMaybe (error "upload: No UploadId on initiate response") $ resp ^. imursUploadId

completeMultipartUpload :: (AWSConstraint r m)
       => Text 
       -> Text 
       -> UploadId 
       -> NumBytes
       -> Digest SHA256
       -> m Text
completeMultipartUpload  accountId vaultName (UploadId uploadId) totalArchiveSize treeHashChecksum = do
  resp <- send $ Amazonka.completeMultipartUpload accountId vaultName uploadId (toText totalArchiveSize) (toText treeHashChecksum)
  pure $ fromMaybe (error "No archive id on upload completion response") $ resp ^. acoArchiveId

uploadMultipartPart :: (AWSConstraint r m, ToHashedBody a) => Text -> Text -> UploadId -> (NumBytes, NumBytes) -> Digest SHA256 -> a -> m ()
uploadMultipartPart !accountId !vaultName (UploadId !uploadId) !byteRange !checksum !body  = do
  !resp <- send $ Amazonka.uploadMultipartPart
    accountId
    vaultName
    uploadId
    (rangeHeader byteRange)
    (toText checksum)
    (toHashed body)
  liftIO $ print $ force resp
  liftIO $ print $ resp ^. Amazonka.umprsChecksum 
  liftIO $ print $ resp ^. Amazonka.umprsResponseStatus 
  pure ()

rangeHeader :: (NumBytes, NumBytes) -> Text
rangeHeader (start, end) = sformat ("bytes " % int % "-" % int % "/*") start end
