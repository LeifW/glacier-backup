{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverloadedStrings, BangPatterns #-}
module MultipartGlacierUpload (GlacierUpload(..), HasGlacierSettings(..), _vaultName, PartSize(getNumBytes), UploadId(getAsText), NumBytes, upload, uploadByChunks, initiateMultipartUpload, completeMultipartUpload) where

import Data.Data (Data(..), mkNoRepType, Typeable)
import GHC.Generics (Generic)

--import Control.Monad.Trans.AWS (AWSConstraint, runResourceT, environment)
import Control.Monad.Trans.AWS --(AWSConstraint, runResourceT, environment)
import Control.Monad.Reader

import Data.Conduit (ConduitT, (.|), Void)
import qualified Data.Conduit.Combinators as C
import Data.Conduit.Zlib -- (gzip)

import Data.Text (Text)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS

import Control.Lens (view)

import Control.Monad (when)
import Control.Monad.IO.Class

import Control.Monad.Primitive (PrimMonad)
import Control.Monad.Trans.Class (lift)

import Network.AWS.Data.Crypto

import TreeHash (treeHashByChunksOf,treeHashList)
import ConduitSupport (chunksOf, zipWithIndex)
import LiftedGlacierRequests

import Control.Monad.IO.Unlift

import Control.DeepSeq
--import Data.Tuple.Strict (Pair(..))
--import qualified Data.Tuple.Strict as StrictPair

treeHash :: ByteString -> Digest SHA256
treeHash = treeHashByChunksOf (1024 * 1024) -- 1 MB chunks == 1024k where 1k = 1024 bytes

-- It's a newtype for a block of memory.
instance Typeable a => Data (Digest a) where
    gunfold _ _ = error "gunfold"
    toConstr _ = error "toConstr"
    dataTypeOf _ = mkNoRepType "Crypto.Hash.Types.Digest"
    --gfoldl k z digest = z (unsafeCoerce @(Block Word8) @(Digest SHA256)) `k` (unsafeCoerce @(Digest SHA256) @(Block Word8) digest)

data GlacierUpload = GlacierUpload {
  _archiveId :: !Text,
  _treeHashChecksum :: !(Digest SHA256),
  _size :: !NumBytes
} deriving (Show, Data, Generic)
  
upload :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m, MonadUnliftIO m) -- PrimMonad constraint is for vectorBuilder 
       => Maybe Text 
       -> ConduitT ByteString Void m GlacierUpload
upload archiveDescription = do
  uploadId <- lift $ initiateMultipartUpload archiveDescription
  (totalArchiveSize, treeHashChecksum) <- uploadByChunks uploadId 0
  archiveId <- lift $ completeMultipartUpload uploadId totalArchiveSize treeHashChecksum 
  pure $ GlacierUpload archiveId treeHashChecksum totalArchiveSize

uploadByChunks :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m, MonadUnliftIO m) => UploadId -> Int -> ConduitT ByteString Void m (NumBytes, Digest SHA256)
uploadByChunks uploadId resumeFrom = do
  partSize <- _partSize <$> view glacierSettingsL
  liftIO $ print partSize
  --l <- C.sinkList
  --liftIO $ print l
  (!sizes,  !checksums) <- unzip <$> pipeline partSize uploadId resumeFrom
  pure (sum sizes, treeHashList checksums)
  --pure (50, hash ("foo" :: ByteString))
  --pure undefined
  
range :: Int -> PartSize -> Int -> (NumBytes, NumBytes)
range !index !partSize !size =
  let startOffset = fromIntegral index * fromIntegral (getNumBytes partSize)
      endOffset = startOffset + fromIntegral size - 1 -- math is hard? I guess the end range is non-inclusive.
  in
     (startOffset, endOffset)

pipeline :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m, MonadUnliftIO m) => PartSize -> UploadId -> Int -> ConduitT ByteString Void m [(NumBytes, Digest SHA256)]
pipeline partSize uploadId resumeFrom = 
     gzip
  .| chunksOf (getNumBytes partSize)
  .| zipWithIndex
  .| (C.drop resumeFrom *> C.mapM uploadChunk)
  .| C.sinkList
    where uploadChunk :: (AWSConstraint r m, HasGlacierSettings r, MonadUnliftIO m) => (Int,  ByteString) -> m (NumBytes, Digest SHA256)
          uploadChunk (!sequenceNum, !chunk) = do
            let !size = BS.length chunk
            liftIO $ putStr "size: "
            liftIO $ print size
            let !byteRange = range sequenceNum partSize size
            liftIO $ print byteRange
            let !checksum = treeHash chunk
            liftIO $ putStrLn $ "About to upload part: " <> show sequenceNum
            liftIO $ print checksum
            --liftIO $ BS.appendFile "/tmp/out" chunk
            glacierSettings <- view glacierSettingsL
            env <- view environment
            --liftIO $ runResourceT $ runReaderT  (uploadMultipartPart uploadId byteRange checksum chunk) (GlacierEnv env glacierSettings)
            runResourceT $ uploadMultipartPart uploadId byteRange checksum chunk
            --force <$> uploadMultipartPart uploadId byteRange checksum chunk
            
            --unless reponse checksum = sent checksum error "why?
            pure (fromIntegral size, checksum) 
