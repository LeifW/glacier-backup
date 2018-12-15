{-# LANGUAGE OverloadedStrings, FlexibleInstances, DeriveGeneric, RecordWildCards, TypeApplications, TupleSections #-}
--{-# OPTIONS_GHC -fno-warn-orphans #-}
module Snapper (SnapperConfig(..), Snapshot(..), UploadStatus(..), SnapshotRef, getSubvolumeFromConfig, listSnapshots, getSnapshot, setSnapshot, setUploadStatus, createSnapshot, deleteSnapshots, dropAllButMostRecentGlacierUploads, setCleanupToTimeline, runSystemDBus, getLastSnapshot, nthSnapshotOnSubvolume) where

import Data.List (sortOn)
import Data.Foldable (traverse_)
import Control.Lens.Lens (Lens', lens)
import Control.Lens.Setter (set)
import DBus.Client (Client, call, connectSystem, disconnect)
import DBus
import Data.Map (Map)
import qualified Data.Map as Map
import Control.Exception (bracket, throwIO, Exception)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Safe (lastMay)
import Data.Maybe (maybeToList)

import Network.AWS.Data.Time (Time(..), ISO8601)
import Network.AWS.Data.Text (toText, fromText)

import Data.Text (Text)

import Data.Word
import Data.Int (Int64)

import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import System.FilePath ((</>))

import GHC.Generics (Generic)

import Util (maybeToEither, lowerMaybe, raiseToMaybe)

import AmazonkaSupport ()

runSystemDBus :: MonadIO m => (Client -> IO a) -> m a 
runSystemDBus = liftIO . bracket connectSystem disconnect

listSnapshotsMethodCall :: MethodCall
listSnapshotsMethodCall = (methodCall
  (objectPath_ "/org/opensuse/Snapper")
  (interfaceName_ "org.opensuse.Snapper")
  (memberName_ "ListSnapshots"))
  {methodCallDestination = Just $ busName_ "org.opensuse.Snapper"}

getConfigMethodCall :: MethodCall
getConfigMethodCall = listSnapshotsMethodCall { methodCallMember = "GetConfig" }

getSnapshotMethodCall :: MethodCall
getSnapshotMethodCall = listSnapshotsMethodCall { methodCallMember = "GetSnapshot" }

setSnapshotMethodCall :: MethodCall
setSnapshotMethodCall = listSnapshotsMethodCall { methodCallMember = "SetSnapshot" }

createSnapshotMethodCall :: MethodCall
createSnapshotMethodCall = listSnapshotsMethodCall { methodCallMember = "CreateSingleSnapshot" }

deleteSnapshotsMethodCall :: MethodCall
deleteSnapshotsMethodCall = listSnapshotsMethodCall { methodCallMember = "DeleteSnapshots" }

type SnapshotRef = Word32

data UploadStatus = UploadStatus Text (Maybe SnapshotRef)
  deriving (Eq, Show, Generic)

uploadStatusToMap :: UploadStatus -> Map Text Text
uploadStatusToMap (UploadStatus archiveId delta) = Map.fromList $
    ("archiveId", archiveId) :
    maybeToList (("deltaFrom",) . toText <$> delta)

uploadStatusFromMap :: Map Text Text -> Maybe UploadStatus 
uploadStatusFromMap m = do
  archiveId <- Map.lookup "archiveId" m
  Just $ UploadStatus archiveId (either (error . ("Can't parse number : " ++) ) id . fromText <$> Map.lookup "deltaFrom" m)

data Cleanup = Number | Timeline | None | GlacierBackup deriving (Eq, Show, Generic)
  
data Snapshot = Snapshot {
  _snapshotNum :: SnapshotRef,
  _timestamp :: ISO8601,
  _uploadedStatus :: Maybe UploadStatus,
  _cleanup :: Cleanup,
  _description :: Maybe Text
} deriving (Eq, Show, Generic)

uploadedStatus :: Lens' Snapshot (Maybe UploadStatus)
uploadedStatus = lens _uploadedStatus (\s us -> s { _uploadedStatus = us } )

cleanup :: Lens' Snapshot Cleanup
cleanup = lens _cleanup (\s c -> s { _cleanup = c } )

cleanupToString :: Cleanup -> Text
cleanupToString Number = "number"
cleanupToString Timeline = "timeline"
cleanupToString None = ""
cleanupToString GlacierBackup = "glacier"

cleanupFromString :: Text -> Cleanup
cleanupFromString "number" = Number
cleanupFromString "timeline" = Timeline
cleanupFromString "" = None
cleanupFromString "glacier" = GlacierBackup

instance IsVariant Cleanup where
  toVariant = toVariant . cleanupToString
  fromVariant v = cleanupFromString <$> fromVariant @Text v

instance IsVariant (Maybe String) where
  toVariant = toVariant . lowerMaybe
  fromVariant v = raiseToMaybe <$> fromVariant v

instance IsVariant (Maybe Text) where
  toVariant = toVariant . lowerMaybe
  fromVariant v = raiseToMaybe <$> fromVariant v

timeFromTimestamp :: Integral a => a -> Time format
timeFromTimestamp = Time . posixSecondsToUTCTime . fromIntegral 

snapshotFromTuple :: (Word32, Word16, Word32, Int64, Word32, Text, Text, Map Text Text) -> Snapshot
snapshotFromTuple (num, _, _, timestamp, userId, description, cleanup, userdata)  = Snapshot num (timeFromTimestamp timestamp) (uploadStatusFromMap userdata) (cleanupFromString cleanup) (raiseToMaybe description)

instance IsVariant Snapshot where
  fromVariant v = snapshotFromTuple <$> fromVariant v
--  toVariant (Snapshot num _)  = toVariant @Word32 $ fromIntegral num

-- There's no IsVariant a => IsVariant [a] rule, just IsValue a => IsVariant [a].
-- And IsValue is a closed class, per the documentation.
instance IsVariant [Snapshot] where
  fromVariant = fmap (map snapshotFromTuple) . fromVariant
  --fromVariant v = snapshotFromTuple <$> fromVariant v

data SnapperConfig = SnapperConfig {
  subvolume :: FilePath,
  configSettings :: Map String String
} deriving (Eq, Show)

snapperConfigFromTuple :: (String, String, Map String String) -> SnapperConfig
snapperConfigFromTuple (_, path, settings) = SnapperConfig path settings

instance IsVariant SnapperConfig where
  fromVariant = fmap snapperConfigFromTuple . fromVariant

--listConfigsMethodCall = listSnapshotsMethodCall { methodCallMember = memberName_ "ListConfigs", methodCallDestination = Just "org.freedesktop.DBus"}
--listConfigsMethodCall = listSnapshotsMethodCall { methodCallMember = memberName_ "ListConfigs", methodCallDestination = Just "org.opensuse.Snapper"}
doMethodCall :: IsVariant a => MethodCall -> [Variant] -> Client -> IO a
doMethodCall method args client = do
  callResult <- call client method { methodCallBody = args }
  --either (throwIO ) pure  $ convertDBusResult . methodReturnBody =<< callResult 
  successfulResult <- throwIOEither callResult 
  throwIOEither $ convertDBusResult $ methodReturnBody successfulResult
--doMethodCall :: IsVariant b => MethodCall -> -> Client -> IO (Either MethodError b)

throwIOEither :: Exception e => Either e a -> IO a
throwIOEither = either throwIO pure

listSnapshots :: String -> Client -> IO [Snapshot]
listSnapshots config = doMethodCall listSnapshotsMethodCall [toVariant config]

dropAllButMostRecentGlacierUploads :: Int -> String -> Client -> IO ()
dropAllButMostRecentGlacierUploads n config client = do
  snapshots <- listSnapshots config client
  let glacierSnapshots = sortOn _timestamp [s | s <- snapshots, _cleanup s == GlacierBackup]
  traverse_ (\s -> setSnapshot config (set cleanup Timeline s) client) $ drop n $ reverse glacierSnapshots
  --let glacierSnapshots = filter (\s -> _cleanup s == GlacierBackup) snapshots
  --pure ()

createSnapshot :: String -> Maybe String -> Client -> IO SnapshotRef
createSnapshot config description = doMethodCall createSnapshotMethodCall [
    toVariant config,
    toVariant description,
    toVariant GlacierBackup,
    toVariant $ Map.empty @String @String
  ]
  
--getConfig :: String -> Client -> IO (Either MethodError (String, String, Map String String))
getConfig :: String -> Client -> IO SnapperConfig
getConfig config = doMethodCall getConfigMethodCall [toVariant config]

getSnapshot :: String -> SnapshotRef -> Client -> IO Snapshot
getSnapshot config snapshotId = doMethodCall getSnapshotMethodCall [
    toVariant config,
    toVariant snapshotId
  ]

deleteSnapshots :: String -> [SnapshotRef] -> Client -> IO ()
deleteSnapshots config snapshotIds = doMethodCall deleteSnapshotsMethodCall [
    toVariant config,
    toVariant snapshotIds
  ]

setSnapshot :: String -> Snapshot -> Client -> IO ()
setSnapshot config Snapshot{..} = doMethodCall setSnapshotMethodCall [
    toVariant config,
    toVariant _snapshotNum,
    toVariant _description,
    toVariant _cleanup,
    toVariant $ maybe Map.empty uploadStatusToMap _uploadedStatus
  ]

setUploadStatus :: String -> SnapshotRef -> Maybe UploadStatus -> Client -> IO Snapshot
setUploadStatus config snapshotId = updateSnapshot config snapshotId . set uploadedStatus
--setUploadStatus config snapshotId uploadStatus = updateSnapshot config snapshotId $ \s -> s { _uploadedStatus = Just uploadStatus }

setCleanup :: String -> SnapshotRef -> Cleanup -> Client -> IO Snapshot
setCleanup config snapshotId = updateSnapshot config snapshotId . set cleanup

setCleanupToTimeline :: String -> SnapshotRef -> Client -> IO Snapshot
setCleanupToTimeline config snapshotId = setCleanup config snapshotId Timeline

updateSnapshot :: String -> SnapshotRef -> (Snapshot -> Snapshot) -> Client -> IO Snapshot
updateSnapshot config snapshotId snapshotUpdate client = do
  snapshot <- getSnapshot config snapshotId client
  let updatedSnapshot = snapshotUpdate snapshot
  setSnapshot config updatedSnapshot client
  pure updatedSnapshot

getSubvolumeFromConfig :: String -> Client -> IO FilePath
getSubvolumeFromConfig config client = subvolume <$> getConfig config client

-- The list of snapshots from Snapper always starts with "Snapshot 0", which isn't actually a snapshot, just the current state of the partition.
-- So throw it out, as we're only concerned with snapshots.
lastSnapshot :: [Snapshot] -> Maybe Snapshot
lastSnapshot (Snapshot 0 _ _  _ _ : t) = lastMay t
--lastSnapshot _ = Nothing

nthSnapshotOnSubvolume :: FilePath -> SnapshotRef -> FilePath
nthSnapshotOnSubvolume subvolume num =
  subvolume </> ".snapshots" </> show num </> "snapshot"

data WrappedMethodError = WrappedMethodError MethodError

instance Show WrappedMethodError where
  show (WrappedMethodError e) = methodErrorMessage e
instance Exception WrappedMethodError
instance Exception MethodError


getLastSnapshot :: String -> Client -> IO (Maybe Snapshot)
getLastSnapshot config client = lastSnapshot <$> listSnapshots config client
--  subvolume <- getSubvolumeFromConfig config client
--  snapshots <- listSnapshots config client
--  pure $ lastSnapshot snapshots
  --pure $ nthSnapshotOnSubvolume subvolume <$> lastSnapshot snapshots

-- TODO: Replace the error's with something more seemly
convertDBusResult :: IsVariant a => [Variant] -> Either MethodError a
convertDBusResult [] = maybeToEither (error  "Only Unit is supported as a return type for no results") $ fromVariant $ toVariant ()
--convertDBusResult [] = maybeToEither (clientArgumentUnpackingError []) $ fromVariant $ toVariant ()
convertDBusResult [singleResult] = maybeToEither (error $ "can't extract variant" ++ show [singleResult]) $ fromVariant singleResult
--convertDBusSingleResult [singleResult] = maybeToEither (clientArgumentUnpackingError [singleResult]) $ fromVariant singleResult
convertDBusResult results = Left $ error $ "unexpected array of results" ++ show results
--convertDBusSingleResult results = Left $ clientArgumentUnpackingError results
