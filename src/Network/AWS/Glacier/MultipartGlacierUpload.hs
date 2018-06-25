{-# LANGUAGE PackageImports, OverloadedStrings, FlexibleContexts #-}
module MultipartGlacierUpload (NumBytes, chunkSizeToBytes, upload, initiate, uploadByChunks, complete) where

--import Network.AWS (MonadAWS, runAWS, liftAWS, newEnv, Credentials(..), within, Region(..))
import Network.AWS (MonadAWS, runAWS, AWS, liftAWS, Credentials(..), Region(..), HasEnv, environment)
import Control.Monad.Catch          (MonadThrow, MonadCatch, Exception, throwM)
import Control.Monad.Trans.Resource (MonadResource, ResourceT)
import Type.Reflection (Typeable)
import Control.Monad.Trans.AWS (runAWST, runResourceT, AWST, AWST', AWSConstraint, send, Env, LogLevel(..), newLogger, envLogger, envRegion, newEnv)
import Network.AWS.Glacier (ArchiveCreationOutput)
import Network.AWS.Data.Text (toText)
import Network.AWS.Data.Body (toHashed)
--import Control.Monad.Trans.Reader   ( ReaderT  )
import System.IO (stdout)

import "cryptonite" Crypto.Hash
import Data.ByteArray (ByteArrayAccess)

import Data.Conduit (ConduitT, runConduit,  (.|), Void, transPipe)
import Data.Conduit.Zlib (gzip)
import Data.Conduit.Lift
import qualified Data.Conduit.Combinators as C

import Data.Int (Int64)
import Data.Text (Text, pack)
import Data.Text.Encoding (decodeUtf8)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS

import Control.Lens -- (set)
import Control.Monad (unless)
import Control.Monad.Trans.Class (lift)
import Data.Maybe (fromMaybe)

import Control.Monad.Primitive (PrimMonad)

import Control.Monad.Error.Class
import Control.Monad.Reader

import TreeHash --(toHex)
import LiftedGlacierRequests
import AmazonkaSupport ()
import ConduitSupport

--default (Int)

treeHash :: ByteString -> Digest SHA256
treeHash = treeHashByChunksOf (1024 * 1024) -- 1 MB chunks == 1024k where 1k = 1024 bytes

data AmazonError =
    UnexpectedHTTPResponseCode Int
  | InvalidChunkSizeRequested
  deriving (Show, Typeable)
instance Exception AmazonError

upload :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m) -- PrimMonad constraint is for vectorBuilder 
       => Maybe Text 
       -> PartSize
       -> ConduitT ByteString Void m ArchiveCreationOutput
upload archiveDescription partSize = do
  --unless (chunkSizeMB `elem` allowedChunkSizes) $ error ("upload: Chunk size must be a power of 2, e.g. one of: " ++ show allowedChunkSizes)
  --let chunkSizeBytes = megabytesToBytes chunkSizeMB
  --chunkSizeBytes <- chunkSizeToBytes chunkSizeMB
  uploadId <- lift $ initiateMultipartUpload archiveDescription partSize
  --let i = 10
  --  in unless (1 == 1) $ error ("foo" ++ show i)
  --unless False undefined where i = 10
  (totalArchiveSize, treeHashChecksum) <- uploadByChunks chunkSizeBytes accountId vaultName uploadId
  completeResponse <- lift $ complete accountId vaultName uploadId treeHashChecksum totalArchiveSize
  --throwM $ UnexpectedHTTPResponseCode 200
  --ask
  --throwError undefined
  pure completeResponse

--bar :: MonadThrow m => m Int
--bar = either throwM pure eitherBar-- throwM $ UnexpectedHTTPResponseCode 200
--bar = throwM $ UnexpectedHTTPResponseCode 200

--baz :: MonadError AmazonError m => m Int
--baz = pure 10

uploadByChunks :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m) => PartSize -> UploadId -> ConduitT ByteString Void m (NumBytes, Digest SHA256)
uploadByChunks partSize uploadId = do
  (sizes, checksums) <- unzip <$> pipeline partSize uploadId
  pure (sum sizes, treeHashList checksums)
  
complete :: (AWSConstraint r m)
       => Text 
       -> Text 
       -> Text 
       -> Digest SHA256
       -> NumBytes
       -> m ArchiveCreationOutput
complete accountId vaultName uploadId treeHashChecksum totalArchiveSize = do
  let completeRequest = completeMultipartUpload accountId vaultName uploadId (toText totalArchiveSize) (toText treeHashChecksum)
  send completeRequest

range :: Int -> PartSize -> Int -> (NumBytes, NumBytes)
range index partSize size =
  let startOffset = fromIntegral index * fromIntegral (getNumBytes partSize)
      endOffset = startOffset + fromIntegral size - 1 -- math is hard? I guess the end range is non-inclusive?
  in
     (startOffset, endOffset)

pipeline :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m) => PartSize -> UploadId -> ConduitT ByteString Void m [(NumBytes, Digest SHA256)]
pipeline partSize uploadId = 
     gzip
  .| chunksOf (getNumBytes partSize)
  .| zipWithIndex
  .| C.mapM uploadChunk
    -- .| C.map (\(i, bs) -> (BS.length bs, hash bs))
  .| C.sinkList
    where uploadChunk :: (AWSConstraint r m, HasGlacierSettings r) => (Int,  ByteString) -> m (NumBytes, Digest SHA256)
          uploadChunk (sequenceNum, chunk) = do
            let size = BS.length chunk
            let byteRange = range sequenceNum partSize size
            let checksum = treeHash chunk
            response <- uploadMultipartPart uploadId byteRange checksum chunk
            --unless reponse checksum = sent checksum error "why?
            pure (fromIntegral $ BS.length chunk, checksum) 
