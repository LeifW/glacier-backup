{-# LANGUAGE OverloadedStrings #-}
module GlacierRequests (NumBytes, PartSize, getNumBytes, UploadId, initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart, Digest, SHA256, ToHashedBody) where

import Data.Int (Int64)
import Data.Maybe (fromMaybe)

import Control.Monad.Trans.AWS (AWSConstraint, send)
import qualified Network.AWS.Glacier as Amazonka
import Network.AWS.Glacier (ArchiveCreationOutput, UploadMultipartPartResponse, InitiateMultipartUploadResponse, imuArchiveDescription, imursUploadId, acoArchiveId)
import Network.AWS.Data.Text (toText)
import Network.AWS.Data.Body (ToHashedBody, toHashed)
import Network.AWS.Data.Crypto (Digest, SHA256)

import Data.Text (Text)

import Control.Lens -- (set)

import Formatting

import AllowedPartSizes (PartSize, getNumBytes)

type NumBytes = Int64

newtype UploadId = UploadId Text

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

uploadMultipartPart :: (AWSConstraint r m, ToHashedBody a) => Text -> Text -> UploadId -> (NumBytes, NumBytes) -> Digest SHA256 -> a -> m UploadMultipartPartResponse
uploadMultipartPart accountId vaultName (UploadId uploadId) byteRange checksum body  = send $ Amazonka.uploadMultipartPart
  accountId
  vaultName
  uploadId
  (rangeHeader byteRange)
  (toText checksum)
  (toHashed body)

rangeHeader :: (NumBytes, NumBytes) -> Text
rangeHeader (start, end) = sformat ("bytes " % int % "-" % int % "/*") start end
