{-# LANGUAGE  FlexibleContexts, FlexibleInstances, OverloadedStrings, LambdaCase #-}
module GlacierUploadFromProc  where

import GlacierReaderT (NumBytes, chunkSizeToBytes, initiate, uploadByChunks, complete, GlacierSettings(..), GlacierEnv(..), HasGlacierSettings, upload, runReaderResource)

import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text.Lazy as Lazy
import Data.Text.Lazy.Encoding (encodeUtf8Builder)
import Formatting
import Data.ByteString.Builder (Builder, stringUtf8, intDec, byteString)
--import System.Process (CreateProcess(..))
import System.Process (cmdspec) -- for logging
import Control.Lens
--import Control.Monad.Reader.Class (asks, runReaderT)
import Control.Monad.Reader
--import Data.Has
import Control.Monad.Trans.Resource

import Control.Monad.Primitive (PrimMonad)
--import Data.Conduit (ConduitT, Void)
import Data.Conduit.Process (sourceProcessWithConsumer, CreateProcess, proc)
import qualified Data.Conduit.Combinators as C
--import Data.Conduit ((.|))
--import Data.Conduit.Zlib (gzip)
import Network.AWS.Glacier (ArchiveCreationOutput)
import Network.AWS (LogLevel(..))
import System.IO (stdout)

import Control.Monad.Trans.AWS


import           Database.SQLite.Simple

glacierSettings :: GlacierSettings
glacierSettings = GlacierSettings "-" "test"
--glacierSettings = GlacierSettings "033819134864" "test"

--builderShow :: Show s => s -> Builder
--builderShow = stringUtf8 . show

--builderText :: Text -> Builder
--builderText = byteString . encodeUtf8

--liftedTextLogger :: MonadIO m => (t -> Builder -> IO a) -> t -> Lazy.Text -> m a
--liftedTextLogger :: MonadIO m => (LogLevel -> Builder -> IO a) -> LogLevel -> Lazy.Text -> m a
liftedTextLogger :: MonadIO m => Logger -> LogLevel -> Lazy.Text -> m ()
liftedTextLogger lg level = liftIO . lg level . encodeUtf8Builder


{-
data GlacierArchive = GlacierArchive {
  archiveId :: Text,
  size
-}
  
glacierUploadFromProcess :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m)
                         => CreateProcess
                         -> Maybe Text 
                         -> Int -- ^ Chunk Size in MB
                         -> m (NumBytes, ArchiveCreationOutput)
glacierUploadFromProcess createProcess archiveDescription chunkSizeMB = do
  logger <- liftedTextLogger <$> view envLogger
  --logger <- (\lg level -> liftIO . lg level . encodeUtf8Builder) <$> view envLogger
  chunkSizeBytes <- chunkSizeToBytes chunkSizeMB
  uploadId <- initiate archiveDescription chunkSizeBytes
  --logger Info $ "Uploading " <> builderShow (cmdspec createProcess) <> " w/ uploadId " <> builderText uploadId <> " using a chunk size of " <> intDec chunkSizeMB <> "MB"
  logger Info $ format ("Uploading " % shown % " w/ uploadId " % stext % " using a chunk size of " % int % "MB") (cmdspec createProcess) uploadId chunkSizeMB
  (exitCode, (totalArchiveSize, treeHashChecksum))  <- sourceProcessWithConsumer (proc "cat" ["/home/leif/Downloads/The-Data-Engineers-Guide-to-Apache-Spark.pdf"])$ uploadByChunks chunkSizeBytes uploadId
  --exitCode, bytes) <- sourceProcessWithConsumer createProcess C.fold
  --(exitCode, (totalArchiveSize, treeHashChecksum)) <- sourceProcessWithConsumer createProcess $ uploadByChunks chunkSizeBytes uploadId
  --(_, conn) <- allocate (open "test.db") close 
  
  completionResp <- complete uploadId treeHashChecksum totalArchiveSize
  pure (totalArchiveSize, completionResp)
  --pure undefined

--runGlacier :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m)


--btrfsSendToGlacier =
--  glacierUploadFromProcess . btrfsSendCmd
{-
btrfsSendCmd :: FilePath -> Maybe FilePath -> CreateProcess
btrfsSendCmd snapshot = \case
  Just parent ->  cmd $ "-p" : parent : args
  Nothing -> cmd args
--btrfsSendCmd (Just parent) snapshot = cmd args
--btrfsSendCmd Nothing snapshot = cmd args
  where cmd = proc "cat"
        args = ["-q", snapshot]
--btrfsSendCmd parent snapshot = maybe [] ("-p":) parent
-}

{-
main :: IO ()
main = do
  lgr <- newLogger Debug stdout
  env <- set envRegion Oregon . set envLogger lgr <$> newEnv Discover
  --aco <- runResourceT $ runReaderT (glacierUpload (Just "a pdf") 1) (GlacierEnv env glacierSettings)
  --aco <- runResourceT $ runReaderT (sourceProcessWithConsumer (proc "cat" ["/home/leif/Downloads/The-Data-Engineers-Guide-to-Apache-Spark.pdf"])$ upload (Just "a pdf") 1) (GlacierEnv env glacierSettings)
  --aco <- runResourceT $ runReaderT (sourceProcessWithConsumer (proc "cat" ["/tmp/foo.hz"])$ upload (Just "a pdf") 1) (GlacierEnv env glacierSettings)
  --aco <- runResourceT $ runReaderT (glacierUpload (Just "a pdf") 1) (GlacierEnv env glacierSettings)
  --aco <- runReaderResource (GlacierEnv env glacierSettings) $ glacierUploadFromProcess (proc "cat" ["/tmp/foo.hz"]) (Just "an xz file") 1
  aco <- runReaderResource (GlacierEnv env glacierSettings) $ glacierUploadFromProcess (proc "cat" ["/home/leif/Downloads/The-Data-Engineers-Guide-to-Apache-Spark.pdf"]) (Just "a real pdf this time") 1
  print aco
-}
