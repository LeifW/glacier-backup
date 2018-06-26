{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass, TypeApplications, RecordWildCards, DataKinds, FlexibleInstances, DisambiguateRecordFields #-}
module UploadSnapshot where

import Control.Exception
import Type.Reflection (Typeable)
import System.Environment (getArgs)
import System.IO (stdout)
import Data.Maybe (fromMaybe)
import Data.Text (Text, stripEnd)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
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
import Network.AWS.Data.ByteString

import Data.Conduit.Process (CreateProcess, proc)
import Control.Monad.Primitive (PrimMonad)
import Network.AWS.Glacier (ArchiveCreationOutput)
import Network.AWS (LogLevel(..))
import Network.AWS.Data.Time -- (Time, ISO8601)
import GlacierReaderT
import GlacierUploadFromProc
import Snapper
import SimpleDB (getLatestUpload)

import qualified Data.Csv as Csv
import Data.Map (Map, fromList) 

btrfsSendCmd :: Maybe FilePath -> FilePath -> CreateProcess
btrfsSendCmd parent snapshot = proc "btrfs" $ ["send"] ++ maybe [] (\p -> ["-p", p]) parent ++ [snapshot]

btrfsSendToGlacier :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m)
                      => Maybe FilePath
                      -> FilePath
                      -> Maybe Text 
                      -> Int -- ^ Chunk Size in MB
                      -> m (NumBytes, ArchiveCreationOutput)
btrfsSendToGlacier parent snapshot = glacierUploadFromProcess (btrfsSendCmd parent snapshot)

--runDBusWithSettings :: (HasGlacierSettings r, MonadReader r m => 

-- Talk to Snapper (over DBus) and SimpleDB to figure out where we're at.
-- Err, give the 
-- SELECT (snapshotNum, date) FROM uploads SORT BY date LIMIT 1
-- Check if there's a full upload
-- How many uploads have happened since then?
-- Over n, make a new one.
-- Is there more than 1 full upload, and if so, is the oldest one over 90 days old?

--shouldCreateNewFullBackup :: (AWSConstraint r m, HasGlacierSettings r) => m Bool
shouldCreateNewFullBackup :: (AWSConstraint r m, HasGlacierSettings r) => m (Maybe ISO8601)
shouldCreateNewFullBackup = do
  -- SELECT date FROM vaultName_uploads WHERE previous = NULL
  -- If none return None
  -- Else:
    --   How many incremental uploads have been applied on top of the most recent one?
    --   Over n? Return None, to create a new one.
    --  Also while we're here: If more than one, check if the older one(s) are over 90 days old, and delete them.
  pure Nothing
  
--This is only called if shouldcreate found a full upload, so there's at least one upload
--getLastUpload :: (AWSConstraint r m, HasGlacierSettings r) =>  m (Maybe Snapshot)
--getLastUpload = pure undefined -- ask simpledb

-- You've gotta have at least one snapshot already or we're quitting.
data E = E deriving (Typeable, Show)
instance Exception E

getDeltaRange :: (AWSConstraint r m, HasGlacierSettings r) => String -> m (Maybe SnapshotRef, Snapshot)
getDeltaRange snapperConfig = do
  -- get snapper config: subvolume path
  -- is there an existing snapshot according to snapper?
  -- If not, throw an error, there's nothing we can do for now.
  -- Get the latest snapshot from snapper
  --latestSnapshot from snapper
  subvolume <- runSystemDBus (getSubvolumeFromConfig snapperConfig)
  --let latestSnapshot = undefined :: (Int, UTCTime)
  lastSnapshot <- maybe (throwM E) pure =<< runSystemDBus (getLastSnapshot snapperConfig)
  --lastSnapshot <- maybe (throwM E) pure =<< liftIO (getLastSnapshot snapperConfig)
  doFullBackup <- shouldCreateNewFullBackup
  -- Technically the join isn't needed - we know getLastUpload will have at least one item if there's already a full backup uploaded.
  -- Could also use >>= of MaybeT
  previousSnapshot <- join <$> traverse (const getLatestUpload) doFullBackup
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
  _current :: SnapshotRef,
  _timestamp :: ISO8601,
  _previous :: Maybe SnapshotRef
} deriving (Show, Generic)

--instance FromText ArchiveDescription where
--  toText b = stripEnd $ toText $ toBS $ Csv.encode [b]

uploadBackup :: (AWSConstraint r m, HasGlacierSettings r) => String -> m ()
uploadBackup snapperConfig = do
  (previous, current) <- getDeltaRange snapperConfig
  let archiveDescription = ArchiveDescription (snapshotNum current) (timestamp current) (previous)
  pure ()
