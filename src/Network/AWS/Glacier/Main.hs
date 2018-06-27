{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass, TypeApplications, RecordWildCards #-}
module Main where

import Control.Exception
import Type.Reflection (Typeable)
import System.Environment (getArgs)
import System.IO (stdout)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Int (Int64)
import Data.Time.Clock (UTCTime)


import GHC.Generics (Generic)
import Data.Yaml --(FromJSON, toJSON)
import Data.Yaml.Config (loadYamlSettings, useEnv)

import Control.Monad.Trans.AWS --(AWSConstraint, Credentials(..))
import Control.Monad (join, guard)
import Control.Monad.Catch (throwM)
import Control.Monad.IO.Class (liftIO)
import Control.Lens.Setter (set)
import Network.AWS.Data.Text

import Data.Conduit.Process (CreateProcess, proc)
import Control.Monad.Primitive (PrimMonad)
import Network.AWS.Glacier (ArchiveCreationOutput)
import Network.AWS (LogLevel(..))
--import GlacierReaderT
import GlacierUploadFromProc
import LiftedGlacierRequests (GlacierEnv(..), GlacierSettings(..))
import AmazonkaSupport (runReaderResource)
import Snapper
import AllowedPartSizes (PartSize)
import UploadSnapshot

import Control.Concurrent


--app_name :: String
--app_name = "glacier-backup"

data Config = Config {
  snapper_config_name :: String, -- Defaults to "root"
  region :: Region,
  glacier_vault_name :: Text,
  upload_part_size_MB :: PartSize,
  aws_account_id :: Text -- A 12-digit number, defaults to account of credentials. (-)
  --aws_account_id :: Maybe Int64 -- A 12-digit number, defaults to account of credentials. (-)
} deriving (Show, Generic, FromJSON)

confDef :: Value
confDef = object [
    ("snapper_config_name", "root"),
    ("aws_account_id",      "-"),
    ("upload_part_size_MB", toJSON @Int 64)
  ]

{-
--runDBusWithSettings :: (HasGlacierSettings r, MonadReader r m => 

-- Talk to Snapper (over DBus) and SimpleDB to figure out where we're at.
-- Err, give the 
-- SELECT (snapshotNum, date) FROM uploads SORT BY date LIMIT 1
-- Check if there's a full upload
-- How many uploads have happened since then?
-- Over n, make a new one.
-- Is there more than 1 full upload, and if so, is the oldest one over 90 days old?

--shouldCreateNewFullBackup :: (AWSConstraint r m, HasGlacierSettings r) => m Bool
shouldCreateNewFullBackup :: (AWSConstraint r m, HasGlacierSettings r) => m (Maybe UTCTime)
shouldCreateNewFullBackup = do
  -- SELECT date FROM vaultName_uploads WHERE previous = NULL
  -- If none return None
  -- Else:
    --   How many incremental uploads have been applied on top of the most recent one?
    --   Over n? Return None, to create a new one.
    --  Also while we're here: If more than one, check if the older one(s) are over 90 days old, and delete them.
  pure undefined
  
--This is only called if shouldcreate found a full upload, so there's at least one upload
--hence this doesn't return a maybe
getLastUpload :: (AWSConstraint r m, HasGlacierSettings r) =>  m (Maybe Snapshot)
getLastUpload = pure undefined -- ask simpledb

-- You've gotta have at least one snapshot already or we're quitting.
data E = E deriving (Typeable, Show)
instance Exception E

getDeltaRange :: (AWSConstraint r m, HasGlacierSettings r) => String -> m (Maybe Snapshot, Snapshot)
getDeltaRange snapperConfig = do
  -- get snapper config: subvolume path
  -- is there an existing snapshot according to snapper?
  -- If not, throw an error, there's nothing we can do for now.
  -- Get the latest snapshot from snapper
  --latestSnapshot from snapper
  subvolume <-  runSystemDBus (getSubvolumeFromConfig snapperConfig)
  --let latestSnapshot = undefined :: (Int, UTCTime)
  lastSnapshot <- maybe (throwM E) pure =<< runSystemDBus (getLastSnapshot snapperConfig)
  --lastSnapshot <- maybe (throwM E) pure =<< liftIO (getLastSnapshot snapperConfig)
  doFullBackup <- shouldCreateNewFullBackup
  -- Technically the join isn't needed - we know getLastUpload will have at least one item if there's already a full backup uploaded.
  -- Could also use >>= of MaybeT
  previousSnapshot <- join <$> traverse (const getLastUpload) doFullBackup
  --previousSnapshot <- sequence $ doFullBackup >>= (const getLastUpload)
  --previousSnapshot <- if doFullBackup then pure Nothing else Just getLatestUpload
  --fullBackup <- createNewFullBackup
  --let previousSnapshot = const . getLatestUpload <$> fullBackup
  --pure (previousSnapshot, lastSnapshot)
  -- get the paths to those snapshots
  --pure undefined 
  pure (previousSnapshot, lastSnapshot)

provisionAWS :: (AWSConstraint r m, HasGlacierSettings r) => m ()
provisionAWS = pure undefined

data ArchiveDescription = ArchiveDescription {
  current :: SnapshotRef,
  timestamp :: UTCTime,
  previous :: Maybe SnapshotRef
} deriving (Show, Generic)
-}

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["provision", "aws"] -> setupConfig "glacier-backup.yml" >>= flip runReaderResource provisionAWS . snd
    --["provision", "aws"] -> setupConfig "/etc/glacier-backup" >>= flip runReaderResource provisionAWS . snd
    [fileName] -> main' fileName
    [] -> main' "glacier-backup.yml"
    --[] -> main' "/etc/glacier-backup.yml"
    _ -> error "glacier-backup: Pass in a filename or don't"

setupConfig :: FilePath -> IO (String, GlacierEnv)
setupConfig configFilePath = do
  --config <- decodeFileEither configFilePath >>=  either throwIO pure
  Config{..} <- loadYamlSettings [configFilePath] [confDef] useEnv
  --let snapper_config = fromMaybe "root" $ snapper_config_name config
  print snapper_config_name
  --let account_id = maybe "-" toText $ aws_account_id config
  --    vault_name = glacier_vault_name config
  let glacierConfig = GlacierSettings aws_account_id glacier_vault_name upload_part_size_MB 
  lgr <- newLogger Debug stdout
  awsEnv <- set envRegion region . set envLogger lgr <$> newEnv Discover
  -- also return the snapper config name
  pure (snapper_config_name, GlacierEnv awsEnv glacierConfig)

--main :: IO ()
main' configFilePath = do
  --config <- either (error . show) id <$> decodeFileEither "/etc/glacier-backup.yml"
  --config <- decodeFileEither "glacier-backup.yml"
  (snapper_config_name, glacierEnv) <- setupConfig configFilePath
  --getDeltaRange snapper_config_name
  runReaderResource glacierEnv $ uploadBackup snapper_config_name
  --threadDelay 10000000
