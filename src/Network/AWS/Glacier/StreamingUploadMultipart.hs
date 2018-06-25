{-# LANGUAGE PackageImports, OverloadedStrings, FlexibleContexts #-}
module StreamingUploadMultipart (NumBytes, chunkSizeToBytes, upload, initiate, uploadByChunks, complete) where

--import Network.AWS (MonadAWS, runAWS, liftAWS, newEnv, Credentials(..), within, Region(..))
import Network.AWS (MonadAWS, runAWS, AWS, liftAWS, Credentials(..), Region(..), HasEnv, environment)
import Control.Monad.Catch          (MonadThrow, MonadCatch, Exception, throwM)
import Control.Monad.Trans.Resource (MonadResource, ResourceT)
import Type.Reflection (Typeable)
import Control.Monad.Trans.AWS (runAWST, runResourceT, AWST, AWST', AWSConstraint, send, Env, LogLevel(..), newLogger, envLogger, envRegion, newEnv)
import Network.AWS.Glacier
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
import AmazonkaSupport ()
import ConduitSupport

--default (Int)
import Formatting

type NumBytes = Int64

-- Powers of two from 1 MB to 4 GB.
allowedChunkSizes :: [Int]
allowedChunkSizes = map (2 ^) [0..12]

-- Can't use Int64 for chunk size, because we're limited by the signature of
-- Conduit's vectorBuilder for the chunking, which takes an Int.
-- Theoretically that limits us to a chunk size of 2GB on 32-bit machines
-- which seems like a far-fetched scenario for a number of reasons (2GB upload chunk in a 4GB address space?)
megabytesToBytes :: Int -> Int
megabytesToBytes i = i * 1024 * 1024

treeHash :: ByteString -> Digest SHA256
treeHash = treeHashByChunksOf (1024 * 1024) -- 1 MB chunks == 1024k where 1k = 1024 bytes

testRunner :: IO ArchiveCreationOutput
testRunner = do
  lgr <- newLogger Debug stdout
  env <- newEnv Discover
 -- runResourceT $ runAWS env $ within Oregon $  runConduit $ C.yieldMany (["foo", "bar"] :: [ByteString]) .| chunksOf 2 .| consume
  
  --runResourceT $ runAWS (set envLogger lgr env) $ within Oregon $ runConduit $ C.yieldMany (["foo", "bar"] :: [ByteString]) .| upload "033819134864" "test" Nothing 1 
  runResourceT $ runReaderT (runConduit $ C.yieldMany (["foo", "bar"] :: [ByteString]) .| upload "033819134864" "test" Nothing 1) (set envRegion Oregon $ set envLogger lgr env)
  --runResourceT $ runConduit $ runReaderC (set envLogger lgr env) (C.yieldMany (["foo", "bar"] :: [ByteString]) .| upload "033819134864" "test" Nothing 5) 

data AmazonError =
    UnexpectedHTTPResponseCode Int
  | InvalidChunkSizeRequested
  deriving (Show, Typeable)
instance Exception AmazonError
--upload :: MonadAWS m => Text -> Text -> Maybe Text -> Int -> Sink ByteString m ArchiveCreationOutput
--upload :: (MonadAWS m, PrimMonad m) -- PrimMonad constraint is for vectorBuilder 
upload' :: (PrimMonad m, MonadResource m, MonadCatch m)
       => Text 
       -> Text 
       -> Maybe Text 
       -> Int -- ^ Chunk Size in MB
       -> ConduitT ByteString Void m ArchiveCreationOutput
upload' accountId vaultName archiveDescription chunkSizeMB = do
   env <- lift $ newEnv Discover
   runAWSConduit env $ upload accountId vaultName archiveDescription chunkSizeMB
   --transPipe (runAWS env) $ upload accountId vaultName archiveDescription chunkSizeMB
 
--instance (Has Env r) => HasEnv r where
--  environment = hasLens

{-
runA :: (AWSConstraint Env m, MonadThrow n, MonadResource n)
              => Env
              -> ConduitT i o m a
              -> ConduitT i o n a
runA env c = runResourcerunReaderC env c
-}

runAWSConduit :: Monad m
              => Env
              -> ConduitT i o (AWST m) a
              -> ConduitT i o m a
runAWSConduit env = transPipe $ runAWST env

runAWSConduit' ::
              Env
              -> ConduitT i o AWS a
              -> ConduitT i o (ResourceT IO) a
runAWSConduit' = runAWSConduit

upload'' :: (PrimMonad m, MonadResource m, MonadCatch m)
       => Env 
       -> Text 
       -> Text 
       -> Maybe Text 
       -> Int -- ^ Chunk Size in MB
       -> ConduitT ByteString Void m ArchiveCreationOutput
upload'' env accountId vaultName archiveDescription chunkSizeMB = runAWSConduit env $ upload accountId vaultName archiveDescription chunkSizeMB
--upload'' env accountId vaultName archiveDescription chunkSizeMB =
--   transPipe (runAWS env) $ upload accountId vaultName archiveDescription chunkSizeMB
--upload'' env accountId vaultName archiveDescription chunkSizeMB =
--   transPipe (runAWS env) $ upload accountId vaultName archiveDescription chunkSizeMB
 
chunkSizeToBytes :: MonadThrow m => Int -> m Int
chunkSizeToBytes chunkSizeMB = do
  unless (chunkSizeMB `elem` allowedChunkSizes) $ throwM InvalidChunkSizeRequested
  pure $ megabytesToBytes chunkSizeMB

upload :: (AWSConstraint r m, PrimMonad m) -- PrimMonad constraint is for vectorBuilder 
       => Text 
       -> Text 
       -> Maybe Text 
       -> Int -- ^ Chunk Size in MB
       -> ConduitT ByteString Void m ArchiveCreationOutput
upload accountId vaultName archiveDescription chunkSizeMB = do
  --unless (chunkSizeMB `elem` allowedChunkSizes) $ error ("upload: Chunk size must be a power of 2, e.g. one of: " ++ show allowedChunkSizes)
  --let chunkSizeBytes = megabytesToBytes chunkSizeMB
  chunkSizeBytes <- chunkSizeToBytes chunkSizeMB
  uploadId <- lift $ initiate accountId vaultName archiveDescription chunkSizeBytes
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

uploadByChunks :: (AWSConstraint r m, PrimMonad m) => Int -> Text -> Text -> Text -> ConduitT ByteString Void m (NumBytes, Digest SHA256)
uploadByChunks chunkSizeBytes accountId vaultName uploadId = do
  (sizes, checksums) <- unzip <$> pipeline chunkSizeBytes accountId vaultName uploadId
  pure (sum sizes, treeHashList checksums)
  
initiate :: (AWSConstraint r m)
       => Text 
       -> Text 
       -> Maybe Text 
       -> Int -- ^ Chunk Size in bytes
       -> m Text
initiate accountId vaultName archiveDescription chunkSizeBytes = do
  let initiateRequest = set imuArchiveDescription archiveDescription $ initiateMultipartUpload accountId vaultName (toText chunkSizeBytes)
  initiateResponse <- send initiateRequest
  let responseCode = initiateResponse  ^. imursResponseStatus in
    unless (responseCode == 201) $ error ("upload: call to InitiateMultipartUpload returned status code: " ++ show responseCode)
  pure $ fromMaybe (error "upload: No UploadId on initiate response") $ initiateResponse ^. imursUploadId

--  let completeRequest = completeMultipartUpload accountId vaultName uploadId (textShow $ sum sizes) (toHexText (treeHashList checksums :: Digest SHA256))

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

range :: Int -> Int -> Int -> (NumBytes, NumBytes)
range index chunkSize size =
  let startOffset = fromIntegral index * fromIntegral chunkSize
      endOffset = startOffset + fromIntegral size - 1 -- math is hard? I guess the end range is non-inclusive?
  in
     (startOffset, endOffset)

--rangeHeader :: Int -> Int -> Int -> Text
rangeHeader :: (NumBytes, NumBytes) -> Text
rangeHeader (start, end) = sformat ("bytes " % int % "-" % int % "/*") start end
  

--uploadChunk :: Text -> Text
pipeline :: (AWSConstraint r m, PrimMonad m) => Int -> Text -> Text -> Text -> ConduitT ByteString Void m [(NumBytes, Digest SHA256)]
pipeline chunkSizeBytes accountId vaultName uploadId = 
     gzip
  .| chunksOf chunkSizeBytes
  .| zipWithIndex
  .| C.mapM uploadChunk
    -- .| C.map (\(i, bs) -> (BS.length bs, hash bs))
  .| C.sinkList
    where uploadChunk :: (AWSConstraint r m) => (Int,  ByteString) -> m (NumBytes, Digest SHA256)
          uploadChunk (sequenceNum, chunk) = do
            let size = BS.length chunk
            let byteRange = range sequenceNum chunkSizeBytes size
            let checksum = treeHash chunk
            let request = uploadMultipartPart accountId vaultName uploadId (rangeHeader byteRange) (toText checksum) (toHashed chunk)
            response <- send request
            --unless reponse checksum = sent checksum error "why?
            pure (fromIntegral $ BS.length chunk, checksum) 
