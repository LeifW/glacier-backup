{-# LANGUAGE OverloadedStrings, DeriveGeneric, StandaloneDeriving, BangPatterns, TupleSections #-}
module GlacierUploadFromProc(GlacierConstraint, HasGlacierSettings, NumBytes, GlacierUpload(..), _vaultName, glacierUploadFromProcess)  where

--import GlacierReaderT (NumBytes, chunkSizeToBytes, initiate, uploadByChunks, complete, GlacierSettings(..), GlacierEnv(..), HasGlacierSettings, upload, runReaderResource)
--import GlacierReaderT (NumBytes, chunkSizeToBytes, initiate, uploadByChunks, complete, GlacierSettings(..), GlacierEnv(..), HasGlacierSettings, upload, runReaderResource)

--import Data.Maybe (fromMaybe)
import MultipartGlacierUpload(HasGlacierSettings, GlacierConstraint, NumBytes, UploadId(getAsText), GlacierUpload(..), _vaultName, zipChunkAndIndex, uploadByChunks, initiateMultipartUpload, completeMultipartUpload) 
--import MemTest(HasGlacierSettings, NumBytes, UploadId(getAsText), GlacierUpload(..), _vaultName, uploadByChunks, initiateMultipartUpload, completeMultipartUpload) 
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text.Lazy as Lazy
import Data.Text.Lazy.Encoding (encodeUtf8Builder)
import Formatting
import Data.ByteString.Builder (Builder, stringUtf8, intDec, byteString)
--import System.Process (CreateProcess(..))
import System.Process (CmdSpec(..), cmdspec) -- for logging
import Control.Lens (view)
--import Control.Monad.Reader.Class (asks, runReaderT)
--import Data.Has


import Control.Monad.IO.Unlift


import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Primitive (PrimMonad)
--import Data.Conduit (ConduitT, Void)
import Data.Conduit.Process (sourceProcessWithConsumer, CreateProcess(..), shell, proc)
import qualified Data.Conduit.Combinators as C
import Data.Conduit.Zlib (gzip)
--import Data.Conduit ((.|))
--import Data.Conduit.Zlib (gzip)
--import Network.AWS (LogLevel(..))
import System.IO (stdout)

import Database.SQLite.Simple.ToField
import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.Ok

import Data.Aeson
import Data.Aeson.Text (encodeToLazyText)
import GHC.Generics (Generic)

import System.Posix.Types
import Control.Exception (Exception, toException)

import GlacierRequests(UploadId(..))

import ConduitSupport

import Control.Monad.Trans.AWS (AWSConstraint, Logger, LogLevel(..), envLogger)

deriving instance Generic CmdSpec
instance ToJSON CmdSpec
instance FromJSON CmdSpec

deriving instance Generic CreateProcess
deriving instance Generic CUid
deriving instance Generic CGid
instance ToJSON CUid
instance ToJSON CGid
--instance ToJSON CreateProcess
--instance FromJSON CreateProcess

instance ToField CmdSpec where
  toField = toField . encodeToLazyText

newtype JSONDecodingError = JSONDecodingError String deriving Show
instance Exception JSONDecodingError

eitherToFieldParseOk :: Either String a -> Ok a
eitherToFieldParseOk = either (Errors . pure . toException .JSONDecodingError) Ok

instance FromField CmdSpec where
  fromField f = fromField f >>= eitherToFieldParseOk . eitherDecodeStrict' . encodeUtf8
  --fromField f = fromField f >>= either (Errors . pure . toException . JSONDecodingError ) Ok . eitherDecodeStrict'

liftedTextLogger :: MonadIO m => Logger -> LogLevel -> Lazy.Text -> m ()
liftedTextLogger lg level = liftIO . lg level . encodeUtf8Builder
  
cmdSpecToCreateProcess :: CmdSpec -> CreateProcess
cmdSpecToCreateProcess (ShellCommand cmd) = shell cmd
cmdSpecToCreateProcess (RawCommand execPath args) = proc execPath args

glacierUploadFromProcess :: (GlacierConstraint r m, PrimMonad m)
                         => CmdSpec
                         -> Maybe Text 
                         -> Maybe (Int, UploadId)
                         -> m GlacierUpload
glacierUploadFromProcess cmd archiveDescription resumptionPoint = do
  logger <- liftedTextLogger <$> view envLogger
  --uploadId <- initiateMultipartUpload archiveDescription
  --(resumeFrom, uploadId) <- sequence $ maybe (0, initiateMultipartUpload archiveDescription) (fmap pure) resumptionPoint
  --(resumeFrom, uploadId) <- maybe ((0,) <$> initiateMultipartUpload archiveDescription) pure resumptionPoint
  (resumeFrom, uploadId) <- case resumptionPoint of
    Just (i, upId) -> pure (i, upId)
    Nothing -> (0,) <$> initiateMultipartUpload archiveDescription
  --let resumeFrom = 0
  --let uploadId = UploadId "foo"
  --let uploadId = UploadId "WY9WGbf2NPm7j7Xs-lh-AFwNdhyqq8HzSx87C7W8YLhjfDDtdS4G46ZzLi5Mg6sWYloWMHFtajOPOiK-k6XZ44pLGBBK"
  --logger Info $ "Uploading " <> builderShow (cmdspec createProcess) <> " w/ uploadId " <> builderText uploadId <> " using a chunk size of " <> intDec chunkSizeMB <> "MB"
  --logger Info $ format ("Uploading " % shown % " w/ uploadId " % stext % " using a chunk size of " % int % "MB") (cmdspec createProcess) uploadId chunkSizeMB
  logger Info $ format ("Uploading " % shown % " w/ uploadId " % stext) cmd (getAsText uploadId)
  --(exitCode, (totalArchiveSize, treeHashChecksum))  <- sourceProcessWithConsumer (proc "cat" ["/home/leif/Downloads/The-Data-Engineers-Guide-to-Apache-Spark.pdf"])$ uploadByChunks uploadId
  --exitCode, bytes) <- sourceProcessWithConsumer createProcess C.fold
  (exitCode, (!totalArchiveSize, !treeHashChecksum)) <- bufferedSourceCommandWithConsumer (cmdSpecToCreateProcess cmd) zipChunkAndIndex $ uploadByChunks uploadId resumeFrom
  --(exitCode, (!totalArchiveSize, !treeHashChecksum)) <- sourceProcessWithConsumer (cmdSpecToCreateProcess cmd) $ uploadByChunks uploadId resumeFrom
  liftIO $ print totalArchiveSize
  liftIO $ print treeHashChecksum
  liftIO $ putStr "Exit code: "
  liftIO $ print exitCode
  --(_, conn) <- allocate (open "test.db") close 
  
  !archiveId <- completeMultipartUpload uploadId totalArchiveSize treeHashChecksum 
  --let archiveId = "bar"
  pure $ GlacierUpload archiveId treeHashChecksum totalArchiveSize 
