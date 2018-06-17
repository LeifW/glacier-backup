{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass, TypeApplications #-}
module Main where

import Control.Exception
import System.Environment (getArgs)
import System.IO (stdout)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Int (Int64)
import Data.Time.Clock (UTCTime)


import GHC.Generics (Generic)
import Data.Aeson (FromJSON)
import Data.Yaml (decodeFileEither)

import Control.Monad.Trans.AWS --(AWSConstraint, Credentials(..))
import Control.Monad (guard)
import Control.Lens.Setter (set)
import Network.AWS.Data.Text

import Data.Conduit.Process (CreateProcess, proc)
import Control.Monad.Primitive (PrimMonad)
import Network.AWS.Glacier (ArchiveCreationOutput)
import Network.AWS (LogLevel(..))
import GlacierReaderT
import GlacierUploadFromProc

--app_name :: String
--app_name = "glacier-backup"

data Config = Config {
  snapper_config_name :: Maybe String, -- Defaults to "root"
  region :: Region,
  glacier_vault_name :: Text,
  upload_chunk_size_MB :: Int,
  aws_account_id :: Maybe Int64 -- A 12-digit number, defaults to account of credentials.
} deriving (Show, Generic, FromJSON)

btrfsSendCmd :: Maybe FilePath -> FilePath -> CreateProcess
btrfsSendCmd parent snapshot = proc "btrfs" $ ["send"] ++ maybe [] (\p -> ["-p", p]) parent ++ [snapshot]

btrfsSendToGlacier :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m)
                      => Maybe FilePath
                      -> FilePath
                      -> Maybe Text 
                      -> Int -- ^ Chunk Size in MB
                      -> m ArchiveCreationOutput
btrfsSendToGlacier parent snapshot = glacierUploadFromProcess (btrfsSendCmd parent snapshot)

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
getLastUpload :: (AWSConstraint r m, HasGlacierSettings r) =>  m (Int, UTCTime)
getLastUpload = pure undefined -- ask simpledb

getDeltaRange :: (AWSConstraint r m, HasGlacierSettings r) => String -> m (Maybe (Int, UTCTime), (Int, UTCTime))
getDeltaRange snapperConfig = do
  -- get snapper config: subvolume path
  -- is there an existing snapshot according to snapper?
  -- If not, throw an error, there's nothing we can do for now.
  -- Get the latest snapshot from snapper
  --latestSnapshot from snapper
  let latestSnapshot = undefined :: (Int, UTCTime)
  doFullBackup <- shouldCreateNewFullBackup
  previousSnapshot <- traverse (const getLastUpload) doFullBackup
  --previousSnapshot <- if doFullBackup then pure Nothing else Just getLatestUpload
  --fullBackup <- createNewFullBackup
  --let previousSnapshot = const . getLatestUpload <$> fullBackup
  pure (previousSnapshot, latestSnapshot)
  -- get the paths to those snapshots
  --pure undefined 

provisionAWS :: (AWSConstraint r m, HasGlacierSettings r) => m ()
provisionAWS = pure undefined

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["provision", "aws"] -> setupConfig "/etc/glacier-backup" >>= flip runReaderResource provisionAWS
    [fileName] -> main' fileName
    [] -> main' "/etc/glacier-backup.yml"
    _ -> error "glacier-backup: Pass in a filename or don't"

setupConfig :: FilePath -> IO GlacierEnv
setupConfig configFilePath = do
  config <- decodeFileEither configFilePath >>=  either throwIO pure
  let snapper_config = fromMaybe "root" $ snapper_config_name config
  let account_id = maybe "-" toText $ aws_account_id config
      vault_name = glacier_vault_name config
      glacierConfig = GlacierSettings account_id vault_name
  lgr <- newLogger Debug stdout
  awsEnv <- set envRegion (region config) . set envLogger lgr <$> newEnv Discover
  pure $ GlacierEnv awsEnv glacierConfig

--main :: IO ()
main' configFilePath = do
  --config <- either (error . show) id <$> decodeFileEither "/etc/glacier-backup.yml"
  --config <- decodeFileEither "glacier-backup.yml"
  glacierEnv <- setupConfig configFilePath
  pure undefined
