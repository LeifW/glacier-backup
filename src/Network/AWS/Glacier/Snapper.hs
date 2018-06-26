{-# LANGUAGE OverloadedStrings, TypeApplications, FlexibleInstances, DeriveGeneric, DeriveDataTypeable, RecordWildCards, TupleSections  #-}
module Snapper (SnapperConfig(..), Snapshot(..), UploadStatus(..), SnapshotRef, getSubvolumeFromConfig, listSnapshots, runSystemDBus, getLastSnapshot) where
import DBus.Client --(Client, call, connectSession, connectSystem)
import DBus
import DBus.Generation (clientArgumentUnpackingError)
import Data.Map (Map)
import qualified Data.Map as Map
import Control.Exception (bracket, throwIO, Exception)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Safe (lastMay)
import Data.Maybe (fromMaybe, maybeToList)

import Network.AWS.Data.Time (Time(..), ISO8601)

import Data.Text (Text, pack, unpack)

import Data.Word
import Data.Int

import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import System.FilePath ((</>))

import Data.Natural (Natural)

import Data.Data (Data)
import GHC.Generics (Generic)

import AmazonkaSupport
import Util (justIf, eitherToMaybe, maybeToEither, lowerMaybe, raiseToMaybe)

instance IsVariant Natural where
  fromVariant v = case typeOf v of
                    TypeWord8  ->  fromIntegral <$> fromVariant @Word8 v
                    TypeWord16  -> fromIntegral <$> fromVariant @Word16 v
                    TypeWord32  -> fromIntegral <$> fromVariant @Word32 v
                    TypeWord64  -> fromIntegral <$> fromVariant @Word64 v
                    _ -> Nothing --error $ "Unexpected type " ++ show other
               

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

setSnapshotMethodCall :: MethodCall
setSnapshotMethodCall = listSnapshotsMethodCall { methodCallMember = "SetSnapshot" }

{-
data SnapshotRef = SnapshotRef {
  snapshotNum :: Int,
  snapshotTime ::  UTCTime
} deriving (Eq, Show, Generic, Data)
-}
type SnapshotRef = Word32

data UploadStatus = UploadStatus {
  uploadId :: Text,
  deltaFrom :: Maybe SnapshotRef
} deriving (Eq, Show, Generic, Data)

uploadStatusToMap :: UploadStatus -> Map String String
uploadStatusToMap (UploadStatus id delta) = Map.fromList $
    ("uploadId", unpack id) :
    maybeToList (("deltaFrom",) . show <$> delta)

uploadStatusFromMap :: Map String String -> Maybe UploadStatus 
uploadStatusFromMap m = do
  uploadId <- pack <$> Map.lookup "uploadId" m
  Just $ UploadStatus uploadId (read <$> Map.lookup "deltaFrom" m)
  
data Snapshot = Snapshot {
  snapshotNum :: SnapshotRef,
  timestamp :: ISO8601,
  --timestamp :: UTCTime,
  uploadedStatus :: Maybe UploadStatus,
  cleanup :: Cleanup,
  description :: Maybe String
} deriving (Eq, Show, Generic, Data)

data Cleanup = Number | Timeline | None deriving (Eq, Show, Generic, Data)

cleanupToString :: Cleanup -> String
cleanupToString Number = "number"
cleanupToString Timeline = "timeline"
cleanupToString None = ""

cleanupFromString :: String -> Cleanup
cleanupFromString "number" = Number
cleanupFromString "timeline" = Timeline
cleanupFromString "" = None

instance IsVariant Cleanup where
  toVariant = toVariant . cleanupToString
  fromVariant v = cleanupFromString <$> fromVariant @String v

{-
stringToMaybe :: String -> Maybe String
stringToMaybe s = justIf (not $ null s) s

maybeToString :: Maybe String -> String
maybeToString = fromMaybe ""
-}

instance IsVariant (Maybe String) where
  toVariant = toVariant . lowerMaybe
  fromVariant v = raiseToMaybe <$> fromVariant v

timeFromTimestamp :: Integral a => a -> Time format
timeFromTimestamp = Time . posixSecondsToUTCTime . fromIntegral 

--snapshotFromTuple :: (Word32, Word16, Word32, Int64, Word32, String, Cleanup, Map String String) -> SnapperSnapshot
snapshotFromTuple :: (Word32, Word16, Word32, Int64, Word32, String, String, Map String String) -> Snapshot
snapshotFromTuple (num, _, _, timestamp, userId, description, cleanup, userdata)  = Snapshot num (timeFromTimestamp timestamp) (uploadStatusFromMap userdata) (cleanupFromString cleanup) (raiseToMaybe description)
-- TODO Replace the above Nothingwith uploadStatus, extractedfrom userdata
--snapshotFromTuple (num, _, _, dateTime, userId, description, cleanup, userdata)  = Snapshot (fromIntegral num) (posixSecondsToUTCTime $ fromIntegral dateTime)

instance IsVariant Snapshot where
  fromVariant v = snapshotFromTuple <$> fromVariant v
--  toVariant (Snapshot num _)  = toVariant @Word32 $ fromIntegral num

-- There's no IsVariant a => IsVariant [a] rule, just IsValue a => IsVariant [a].
-- And IsValue is a closed class, per the documentation.
--instance IsVariant [SnapshotRef] where
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

--getConfig :: String -> Client -> IO (Either MethodError (String, String, Map String String))
getConfig :: String -> Client -> IO SnapperConfig
getConfig config = doMethodCall getConfigMethodCall [toVariant config]

--setSnapshot :: String -> Snapshot -> String -> Cleanup -> Map String String -> Client -> IO String
--setSnapshot config snapshot description cleanup userdata = doMethodCall setSnapshotMethodCall [toVariant config, toVariant snapshot, toVariant description, toVariant cleanup, toVariant userdata]
--
setSnapshot :: String -> Snapshot -> Client -> IO ()
setSnapshot config Snapshot{..} = doMethodCall setSnapshotMethodCall [
    toVariant config,
    toVariant snapshotNum,
    toVariant description,
    toVariant cleanup,
    toVariant $ maybe Map.empty uploadStatusToMap uploadedStatus
  ]
--setSnapshot config Snapshot{..} = doMethodCall setSnapshotMethodCall [toVariant config, toVariant @Word32 (fromIntegral snapshotNum), toVariant description, toVariant cleanup, toVariant (Map.empty @String @String)]
--setSnapshot config (Snapshot snapshotNum timestamp description cleanup userdata = doMethodCall setSnapshotMethodCall [toVariant config, toVariant snapshot, toVariant description, toVariant cleanup, toVariant userdata]
--setSnapshot config snapshot description cleanup userdata = doMethodCall setSnapshotMethodCall [toVariant config, toVariant (50 :: Word32),  toVariant ("" :: String), toVariant userdata]

getSubvolumeFromConfig :: String -> Client -> IO FilePath
getSubvolumeFromConfig config client = subvolume <$> getConfig config client

-- The list of snapshots from Snapper always starts with "Snapshot 0", which isn't actually a snapshot, just the current state of the partition.
-- So throw it out, as we're only concerned with snapshots.
lastSnapshot :: [Snapshot] -> Maybe Snapshot
lastSnapshot (Snapshot 0 _ _  _ _ : t) = lastMay t
--lastSnapshot _ = Nothing

--nthSnapshotOnSubvolume :: FilePath -> Snapshot -> FilePath
--nthSnapshotOnSubvolume subvolume snapshot =
--  subvolume </> ".snapshots" </> show (snapshotNum snapshot) </> "snapshot"

nthSnapshotOnSubvolume :: FilePath -> Word32 -> FilePath
nthSnapshotOnSubvolume subvolume num =
  subvolume </> ".snapshots" </> show num </> "snapshot"

{-
snapshotDirFromConfig :: SnapperConfig -> Snapshot -> FilePath
snapshotDirFromConfig (SnapperConfig subvolume _)  (Snapshot num _) =
  subvolume </> ".snapshots" </> show num </> "snapshot"
-}


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
