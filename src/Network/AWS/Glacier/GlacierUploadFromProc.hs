{-# LANGUAGE OverloadedStrings, DeriveGeneric, StandaloneDeriving, BangPatterns, TupleSections #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module GlacierUploadFromProc(GlacierConstraint, HasGlacierSettings, NumBytes, GlacierUpload(..), _vaultName, glacierUploadFromProcess)  where

import MultipartGlacierUpload(HasGlacierSettings, GlacierConstraint, NumBytes, UploadId(uploadIdAsText), GlacierUpload(..), _vaultName, zipChunkAndIndex, uploadByChunks, initiateMultipartUpload, completeMultipartUpload) 
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text.Lazy as Lazy
import Data.Text.Lazy.Encoding (encodeUtf8Builder)
import Formatting
import System.Process (CreateProcess, CmdSpec(..), shell, proc) -- for logging
import Control.Lens (view)

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Primitive (PrimMonad)

import Database.SQLite.Simple.ToField
import Database.SQLite.Simple.FromField
import Database.SQLite.Simple.Ok

import Data.Aeson
import Data.Aeson.Text (encodeToLazyText)
import GHC.Generics (Generic)

import System.Posix.Types
import Control.Exception (Exception, toException)

import ConduitSupport
import Util (throwExitFailure)

import Control.Monad.Trans.AWS (Logger, LogLevel(..), envLogger)

deriving instance Generic CmdSpec
instance ToJSON CmdSpec
instance FromJSON CmdSpec

deriving instance Generic CUid
deriving instance Generic CGid
instance ToJSON CUid
instance ToJSON CGid

instance ToField CmdSpec where
  toField = toField . encodeToLazyText

newtype JSONDecodingError = JSONDecodingError String deriving Show
instance Exception JSONDecodingError

eitherToFieldParseOk :: Either String a -> Ok a
eitherToFieldParseOk = either (Errors . pure . toException .JSONDecodingError) Ok

instance FromField CmdSpec where
  fromField f = fromField f >>= eitherToFieldParseOk . eitherDecodeStrict' . encodeUtf8

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
  logger Info $ format ("Uploading " % shown % " w/ uploadId " % stext) cmd (uploadIdAsText uploadId)
  --(exitCode, (totalArchiveSize, treeHashChecksum))  <- sourceProcessWithConsumer (proc "cat" ["/home/leif/Downloads/The-Data-Engineers-Guide-to-Apache-Spark.pdf"])$ uploadByChunks uploadId
  --exitCode, bytes) <- sourceProcessWithConsumer createProcess C.fold
  (exitCode, (!totalArchiveSize, !treeHashChecksum)) <- bufferedSourceProcessWithConsumer (cmdSpecToCreateProcess cmd) zipChunkAndIndex $ uploadByChunks uploadId resumeFrom
  --(exitCode, (!totalArchiveSize, !treeHashChecksum)) <- sourceProcessWithConsumer (cmdSpecToCreateProcess cmd) $ uploadByChunks uploadId resumeFrom
  throwExitFailure exitCode
  liftIO $ print totalArchiveSize
  liftIO $ print treeHashChecksum
  !archiveId <- completeMultipartUpload uploadId totalArchiveSize treeHashChecksum 
  pure $ GlacierUpload archiveId treeHashChecksum totalArchiveSize 
