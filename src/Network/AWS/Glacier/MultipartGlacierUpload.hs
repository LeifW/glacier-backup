{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}
module MultipartGlacierUpload (GlacierUpload(..), UploadId, NumBytes, upload, uploadByChunks) where

import Data.Data (Data(..), mkNoRepType, Typeable)
import GHC.Generics (Generic)

import Control.Monad.Trans.AWS (AWSConstraint)

import Data.Conduit (ConduitT, (.|), Void)
import qualified Data.Conduit.Combinators as C
import Data.Conduit.Zlib (gzip)

import Data.Text (Text)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS

import Control.Lens (view)

import Control.Monad.Primitive (PrimMonad)
import Control.Monad.Trans.Class (lift)

import TreeHash (treeHashByChunksOf,treeHashList)
import ConduitSupport (chunksOf, zipWithIndex)
import LiftedGlacierRequests

treeHash :: ByteString -> Digest SHA256
treeHash = treeHashByChunksOf (1024 * 1024) -- 1 MB chunks == 1024k where 1k = 1024 bytes

-- It's a newtype for a block of memory.
instance Typeable a => Data (Digest a) where
    gunfold _ _ = error "gunfold"
    toConstr _ = error "toConstr"
    dataTypeOf _ = mkNoRepType "Crypto.Hash.Types.Digest"
    --gfoldl k z digest = z (unsafeCoerce @(Block Word8) @(Digest SHA256)) `k` (unsafeCoerce @(Digest SHA256) @(Block Word8) digest)

data GlacierUpload = GlacierUpload {
  _archiveId :: Text,
  _treeHashChecksum :: Digest SHA256,
  _size :: NumBytes
} deriving (Show, Data, Generic)
  
upload :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m) -- PrimMonad constraint is for vectorBuilder 
       => Maybe Text 
       -> ConduitT ByteString Void m GlacierUpload
upload archiveDescription = do
  uploadId <- lift $ initiateMultipartUpload archiveDescription
  (totalArchiveSize, treeHashChecksum) <- uploadByChunks uploadId
  archiveId <- lift $ completeMultipartUpload uploadId totalArchiveSize treeHashChecksum 
  pure $ GlacierUpload archiveId treeHashChecksum totalArchiveSize

uploadByChunks :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m) => UploadId -> ConduitT ByteString Void m (NumBytes, Digest SHA256)
uploadByChunks uploadId = do
  partSize <- _partSize <$> view glacierSettingsL
  (sizes, checksums) <- unzip <$> pipeline partSize uploadId
  pure (sum sizes, treeHashList checksums)
  
range :: Int -> PartSize -> Int -> (NumBytes, NumBytes)
range index partSize size =
  let startOffset = fromIntegral index * fromIntegral (getNumBytes partSize)
      endOffset = startOffset + fromIntegral size - 1 -- math is hard? I guess the end range is non-inclusive.
  in
     (startOffset, endOffset)

pipeline :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m) => PartSize -> UploadId -> ConduitT ByteString Void m [(NumBytes, Digest SHA256)]
pipeline partSize uploadId = 
     gzip
  .| chunksOf (getNumBytes partSize)
  .| zipWithIndex
  .| C.mapM uploadChunk
  .| C.sinkList
    where uploadChunk :: (AWSConstraint r m, HasGlacierSettings r) => (Int,  ByteString) -> m (NumBytes, Digest SHA256)
          uploadChunk (sequenceNum, chunk) = do
            let size = BS.length chunk
            let byteRange = range sequenceNum partSize size
            let checksum = treeHash chunk
            response <- uploadMultipartPart uploadId byteRange checksum chunk
            --unless reponse checksum = sent checksum error "why?
            pure (fromIntegral $ BS.length chunk, checksum) 
