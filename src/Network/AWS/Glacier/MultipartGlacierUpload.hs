{-# LANGUAGE DeriveGeneric, BangPatterns #-}
module MultipartGlacierUpload (GlacierUpload(..), HasGlacierSettings(..), GlacierConstraint, _vaultName, PartSize, partSizeInBytes, UploadId(uploadIdAsText), NumBytes, upload, uploadByChunks, initiateMultipartUpload, completeMultipartUpload, zipChunkAndIndex) where

import GHC.Generics (Generic)

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
import LiftedGlacierRequests

treeHash :: ByteString -> Digest SHA256
treeHash = treeHashByChunksOf (1024 * 1024) -- 1 MB chunks == 1024k where 1k = 1024 bytes

data GlacierUpload = GlacierUpload {
  _archiveId :: !ArchiveId,
  _treeHashChecksum :: !(Digest SHA256),
  _size :: !NumBytes
} deriving (Show, Generic)
  
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
  let !size = BS.length chunk
  liftIO $ putStr "size: "
  liftIO $ print size
  let !byteRange = range sequenceNum partSize size
  liftIO $ print byteRange
  let !checksum = treeHash chunk
  liftIO $ putStrLn $ "About to upload part: " <> show sequenceNum
  liftIO $ print checksum
  uploadMultipartPart uploadId byteRange checksum chunk
  
  --unless reponse checksum = sent checksum error "why?
  pure (fromIntegral size, checksum) 
