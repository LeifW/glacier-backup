{-# LANGUAGE BangPatterns #-}
module MultipartGlacierUpload (GlacierUpload(..), HasGlacierSettings(..), GlacierConstraint, _vaultName, PartSize, partSizeInBytes, UploadId(uploadIdAsText), NumBytes, upload, uploadByChunks, initiateMultipartUpload, completeMultipartUpload, zipChunkAndIndex) where

import Control.Monad.Reader

import Data.Conduit (ConduitT, (.|), Void)
import qualified Data.Conduit.Combinators as C
import Data.Conduit.Zlib (gzip)

import Data.Text (Text)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS

import Control.Lens (view)

import Control.Monad.Primitive (PrimMonad)
import Control.Monad.Trans.Class (lift)

import Network.AWS.Data.Crypto

import TreeHash (treeHashByChunksOf, treeHashList)
import ConduitSupport (chunksOf, zipWithIndexFrom)
import AmazonkaSupport (getLogger, textFormat, bytesFormat)
import Control.Monad.Trans.AWS (LogLevel(..))
import Formatting (format, int, (%))
import LiftedGlacierRequests

treeHash :: ByteString -> Digest SHA256
treeHash = treeHashByChunksOf (1024 * 1024) -- 1 MB chunks == 1024k where 1k = 1024 bytes

upload :: (GlacierConstraint r m, PrimMonad m) -- PrimMonad constraint is for vectorBuilder 
       => Maybe Text 
       -> ConduitT ByteString Void m GlacierUpload
upload archiveDescription = do
  uploadId <- lift $ initiateMultipartUpload archiveDescription
  (totalArchiveSize, treeHashChecksum) <- zipChunkAndIndex .| uploadByChunks uploadId 0
  archiveId <- lift $ completeMultipartUpload uploadId totalArchiveSize treeHashChecksum 
  pure $ GlacierUpload archiveId treeHashChecksum totalArchiveSize

uploadByChunks :: (GlacierConstraint r m, PrimMonad m) => UploadId -> Int -> ConduitT (Int, ByteString) Void m (NumBytes, Digest SHA256)
uploadByChunks uploadId resumeFrom = do
  (sizes, checksums) <- unzip <$> glacierUploadParts uploadId resumeFrom
  pure (sum sizes, treeHashList checksums)
  
range :: Int -> PartSize -> Int -> (NumBytes, NumBytes)
range !index !partSize !size =
  let startOffset = fromIntegral index * fromIntegral (partSizeInBytes partSize)
      endOffset = startOffset + fromIntegral size - 1 -- math is hard? I guess the end range is non-inclusive.
  in
     (startOffset, endOffset)

zipChunkAndIndex :: (GlacierConstraint r m, PrimMonad m) => ConduitT ByteString (Int, ByteString) m ()
zipChunkAndIndex = do
  partSize <- partSizeInBytes . _partSize <$> view glacierSettingsL
  gzip
    .| chunksOf partSize
    .| zipWithIndexFrom 0
  
glacierUploadParts :: (GlacierConstraint r m, PrimMonad m) => UploadId -> Int -> ConduitT (Int, ByteString) Void m [(NumBytes, Digest SHA256)]
glacierUploadParts uploadId resumeFrom =
    (C.drop resumeFrom
       *> C.mapM (uploadPart uploadId))
  .| C.sinkList

uploadPart :: (GlacierConstraint r m) => UploadId -> (Int,  ByteString) -> m (NumBytes, Digest SHA256)
uploadPart uploadId (!sequenceNum, !chunk) = do
  partSize <- _partSize <$> view glacierSettingsL
  logger <- getLogger
  let !size = BS.length chunk
  logger Info $ format ("Chunk size: " % bytesFormat) size
  let !byteRange = range sequenceNum partSize size
  let !checksum = treeHash chunk
  logger Info $ format ("Chunk checksum: " % textFormat) checksum
  let (start, end) = byteRange in
    logger Info $ format ("About to upload byte range " % int % " - " % int % " as part number " % int) start end sequenceNum
  uploadMultipartPart uploadId byteRange checksum chunk
  pure (fromIntegral size, checksum) 
