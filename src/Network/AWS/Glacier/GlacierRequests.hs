{-# LANGUAGE OverloadedStrings #-}
module GlacierRequests (initiateMultipartUpload, completeMultipartUpload, uploadMultipartPart) where

import Data.Int (Int64)

import Control.Monad.Trans.AWS (AWSConstraint, send)
import qualified Network.AWS.Glacier as Amazonka
import Network.AWS.Glacier (ArchiveCreationOutput, UploadMultipartPartResponse, InitiateMultipartUploadResponse, imuArchiveDescription)
import Network.AWS.Data.Text (toText)
import Network.AWS.Data.Body (ToHashedBody, toHashed)
import Network.AWS.Data.Crypto (Digest, SHA256)

import Data.Text (Text)

import Control.Lens (set)

import Formatting

type NumBytes = Int64

initiateMultipartUpload :: (AWSConstraint r m)
       => Text 
       -> Text 
       -> Maybe Text 
       -> Int
       -> m InitiateMultipartUploadResponse
initiateMultipartUpload accountId vaultName archiveDescription chunkSizeBytes = send $
  set imuArchiveDescription archiveDescription $ Amazonka.initiateMultipartUpload accountId vaultName (toText chunkSizeBytes)

completeMultipartUpload :: (AWSConstraint r m)
       => Text 
       -> Text 
       -> Text 
       -> NumBytes
       -> Digest SHA256
       -> m ArchiveCreationOutput
completeMultipartUpload  accountId vaultName uploadId totalArchiveSize treeHashChecksum  = send $ Amazonka.completeMultipartUpload 
  accountId vaultName uploadId (toText totalArchiveSize) (toText treeHashChecksum)

uploadMultipartPart :: (AWSConstraint r m, ToHashedBody a) => Text -> Text -> Text -> (NumBytes, NumBytes) -> Digest SHA256 -> a -> m UploadMultipartPartResponse
uploadMultipartPart accountId vaultName uploadId byteRange checksum body  = send $ Amazonka.uploadMultipartPart
  accountId
  vaultName
  uploadId
  (rangeHeader byteRange)
  (toText checksum)
  (toHashed body)

rangeHeader :: (NumBytes, NumBytes) -> Text
rangeHeader (start, end) = sformat ("bytes " % int % "-" % int % "/*") start end
