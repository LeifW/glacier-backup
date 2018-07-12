{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass, TypeApplications, RecordWildCards, DataKinds, FlexibleInstances, DisambiguateRecordFields #-}
module UploadSnapshot where

import Control.Exception
import Type.Reflection (Typeable)
import System.Environment (getArgs)
import System.IO (stdout)
import Data.Maybe (fromMaybe)
import Data.Text (Text, stripEnd)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
--import Data.Int (Int64)
import Data.Time.Clock (UTCTime)

import Control.Monad.IO.Unlift

import GHC.Generics (Generic)
import Data.Yaml --(FromJSON, toJSON)
import Data.Yaml.Config (loadYamlSettings, useEnv)

import Control.Monad.Trans.AWS --(AWSConstraint, Credentials(..))
import Control.Monad.Catch (throwM)
import Control.Monad.IO.Class (liftIO)
import Control.Lens.Setter (set)
import Network.AWS.Data.Text
import Network.AWS.Data.ByteString

import Database.SQLite.Simple

import Data.Conduit.Process (CreateProcess, CmdSpec(..), proc, shell)
import Control.Monad.Primitive (PrimMonad)
import Network.AWS.Glacier (ArchiveCreationOutput)
import Network.AWS (LogLevel(..))
import LiftedGlacierRequests (UploadId, createVault)
import GlacierRequests (UploadId(UploadId))
import GlacierUploadFromProc
import Snapper (SnapshotRef, UploadStatus(UploadStatus), Snapshot(_timestamp),  runSystemDBus, createSnapshot, setUploadStatus, getSubvolumeFromConfig, nthSnapshotOnSubvolume)
import SimpleDB (SnapshotUpload(..), getLatestUpload, insertSnapshotUpload, createDomain)
import ArchiveSnapshotDescription(ArchiveSnapshotDescription(ArchiveSnapshotDescription))

import qualified Data.Csv as Csv
import Data.Map (Map, fromList) 

btrfsSendCmd :: Maybe FilePath -> FilePath -> CmdSpec
btrfsSendCmd parent snapshot = RawCommand "sudo" $ ["btrfs", "send", "-q"] ++ maybe [] (\p -> ["-p", p]) parent ++ [snapshot]

btrfsSendToGlacier :: (GlacierConstraint r m, PrimMonad m)
                      => Maybe FilePath
                      -> FilePath
                      -> Maybe Text 
                      -> Maybe (Int, UploadId)
                      -> m GlacierUpload
btrfsSendToGlacier parent snapshot = glacierUploadFromProcess (btrfsSendCmd parent snapshot)

maybeWhen :: Applicative m => Bool -> m (Maybe a) -> m (Maybe a)
maybeWhen True f = f
maybeWhen False _ = pure Nothing
    
-- Talk to Snapper (over DBus) and SimpleDB to figure out where we're at.
-- Err, give the 
-- SELECT (snapshotNum, date) FROM uploads SORT BY date LIMIT 1
-- Check if there's a full upload
-- How many uploads have happened since then?
-- Over n, make a new one.
-- Is there more than 1 full upload, and if so, is the oldest one over 90 days old?

shouldCreateNewFullBackup :: (GlacierConstraint r m) => m Bool
shouldCreateNewFullBackup = do
  -- SELECT date FROM vaultName_uploads WHERE previous = NULL
  -- If none return None
  -- Else:
    --   How many incremental uploads have been applied on top of the most recent one?
    --   Over n? Return None, to create a new one.
    --  Also while we're here: If more than one, check if the older one(s) are over 90 days old, and delete them.
  pure False
  
--This is only called if shouldcreate found a full upload, so there's at least one upload
--getLastUpload :: (AWSConstraint r m, HasGlacierSettings r) =>  m (Maybe Snapshot)
--getLastUpload = pure undefined -- ask simpledb

-- You've gotta have at least one snapshot already or we're quitting.
data E = E deriving (Typeable, Show)
instance Exception E

getDeltaRange :: (GlacierConstraint r m) => String -> m (Maybe SnapshotRef, SnapshotRef)
getDeltaRange snapperConfigName = do
  -- get snapper config: subvolume path
  -- is there an existing snapshot according to snapper?
  -- If not, throw an error, there's nothing we can do for now.
  -- Get the latest snapshot from snapper
  --lastSnapshot <- maybe (throwM E) pure =<< runSystemDBus (getLastSnapshot snapperConfigName)
  currentSnapshot <-runSystemDBus $ createSnapshot snapperConfigName Nothing
  --lastSnapshot <- maybe (throwM E) pure =<< liftIO (getLastSnapshot snapperConfig)
  doFullBackup <- shouldCreateNewFullBackup
  -- Technically the join isn't needed - we know getLastUpload will have at least one item if there's already a full backup uploaded.
  -- Could also use >>= of MaybeT
  --previousSnapshot <- join <$> traverse (const getLatestUpload) doFullBackup
  previousSnapshot <- if doFullBackup then pure Nothing else getLatestUpload 
  --let previousSnapshot = Just 144
  --let previousSnapshot = Just 200
  --let previousSnapshot = Just 135
  --previousSnapshot <- sequence $ doFullBackup >>= (const getLastUpload)
  --previousSnapshot <- if doFullBackup then pure Nothing else Just getLatestUpload
  --fullBackup <- createNewFullBackup
  --let previousSnapshot = const . getLatestUpload <$> fullBackup
  --pure (previousSnapshot, lastSnapshot)
  -- get the paths to those snapshots
  --pure undefined 
  pure (previousSnapshot, currentSnapshot)

provisionAWS :: (GlacierConstraint r m) => m ()
provisionAWS = do
  createVault
  createDomain

{-
data ArchiveDescription = ArchiveDescription {
  _current :: SnapshotRef,
  _timestamp :: ISO8601,
  _previous :: Maybe SnapshotRef
} deriving (Show, Generic)
-}

--instance FromText ArchiveDescription where
--  toText b = stripEnd $ toText $ toBS $ Csv.encode [b]

uploadBackup :: (GlacierConstraint r m, PrimMonad m)  => String -> m ()
uploadBackup snapperConfigName = do
  (previous, current) <- getDeltaRange snapperConfigName
  --let currentNum = _snapshotNum current
  subvolume <- runSystemDBus (getSubvolumeFromConfig snapperConfigName)
  let nthSnapshot = nthSnapshotOnSubvolume subvolume
  let snapshotDescription = ArchiveSnapshotDescription current previous
  {-
  conn <- liftIO $ open "test.db"
  liftIO $ execute_ conn "CREATE TABLE IF NOT EXISTS uploads (previous TEXT, current TEXT NOT NULL, uploadId TEXT NOT NULL)"
  liftIO $ execute conn "INSERT INTO uploads (previous, current, uploadId) VALUES (?,?,?)" (nthSnapshot <$> previousNum, nthSnapshot currentNum, UploadId "foo")
  liftIO $ close conn
  -}
  glacierUpload <- btrfsSendToGlacier (nthSnapshot <$> previous) (nthSnapshot current) (Just $ toText snapshotDescription) Nothing
  let uploadStatus = UploadStatus (_archiveId glacierUpload) previous
  timestamp <- _timestamp <$> runSystemDBus (setUploadStatus snapperConfigName current uploadStatus)
  insertSnapshotUpload $ SnapshotUpload glacierUpload snapshotDescription timestamp
