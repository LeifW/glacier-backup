{-# LANGUAGE OverloadedStrings, TypeApplications, FlexibleInstances #-}
module Snapper (SnapperConfig(..), Snapshot(..), getConfig, listSnapshots, lastSnapshot, runSystemDBus) where
import DBus.Client --(Client, call, connectSession, connectSystem)
import DBus
import DBus.Generation (clientArgumentUnpackingError)
import Data.Map (Map)
import Control.Exception (bracket, throwIO, Exception)
import Safe (lastMay)

import Data.Word
import Data.Int

import Data.Time.Clock (UTCTime)
import Data.Time.Clock.POSIX
import System.FilePath ((</>))

import Data.Natural

instance IsVariant Natural where
  fromVariant v = case typeOf v of
                    TypeWord8  ->  fromIntegral <$> fromVariant @Word8 v
                    TypeWord16  -> fromIntegral <$> fromVariant @Word16 v
                    TypeWord32  -> fromIntegral <$> fromVariant @Word32 v
                    TypeWord64  -> fromIntegral <$> fromVariant @Word64 v
                    _ -> Nothing --error $ "Unexpected type " ++ show other
               

runSystemDBus :: (Client -> IO a) -> IO a 
runSystemDBus = bracket connectSystem disconnect

listSnapshotsMethodCall :: MethodCall
listSnapshotsMethodCall = (methodCall
  (objectPath_ "/org/opensuse/Snapper")
  (interfaceName_ "org.opensuse.Snapper")
  (memberName_ "ListSnapshots"))
  {methodCallDestination = Just $ busName_ "org.opensuse.Snapper"}

getConfigMethodCall :: MethodCall
getConfigMethodCall = listSnapshotsMethodCall { methodCallMember = "GetConfig" }


data Snapshot = Snapshot {
  snapshotNum :: Int,
  snapshotTime ::  UTCTime
} deriving (Eq, Show)

snapshotFromTuple :: (Word32, Word16, Word32, Int64, Word32, String, String, Map String String) -> Snapshot
snapshotFromTuple (num, _, _, dateTime, _, _, _, _ )  = Snapshot (fromIntegral num) (posixSecondsToUTCTime $ fromIntegral dateTime)

-- Can't just make an instance for Snapshot because of the IsValue typeclass...
instance IsVariant [Snapshot] where
  fromVariant = fmap (map snapshotFromTuple) . fromVariant
  --fromVariant v = snapshotFromTuple <$> fromVariant v


data SnapperConfig = SnapperConfig {
  configName :: String,
  subvolume :: FilePath,
  configSettings :: Map String String
} deriving (Eq, Show)

snapperConfigFromTuple :: (String, String, Map String String) -> SnapperConfig
snapperConfigFromTuple (name, path, settings) = SnapperConfig name path settings

instance IsVariant SnapperConfig where
  fromVariant = fmap snapperConfigFromTuple . fromVariant

--listConfigsMethodCall = listSnapshotsMethodCall { methodCallMember = memberName_ "ListConfigs", methodCallDestination = Just "org.freedesktop.DBus"}
--listConfigsMethodCall = listSnapshotsMethodCall { methodCallMember = memberName_ "ListConfigs", methodCallDestination = Just "org.opensuse.Snapper"}
doMethodCall :: (IsVariant a, IsVariant b) => MethodCall -> [a] -> Client -> IO (Either MethodError b)
doMethodCall method args client = do
  callResult <- call client method { methodCallBody = toVariant <$> args }
  pure $ convertDBusSingleResult . methodReturnBody =<< callResult 
--doMethodCall :: IsVariant b => MethodCall -> -> Client -> IO (Either MethodError b)

listSnapshots :: String -> Client -> IO (Either MethodError [Snapshot])
listSnapshots config = doMethodCall listSnapshotsMethodCall [config]

--getConfig :: String -> Client -> IO (Either MethodError (String, String, Map String String))
getConfig :: String -> Client -> IO (Either MethodError SnapperConfig)
getConfig config = doMethodCall getConfigMethodCall [config]

-- The list of snapshots from Snapper always starts with "Snapshot 0", which isn't actually a snapshot, just the current state of the partition.
-- So throw it out, as we're only concerned with snapshots.
lastSnapshot :: [Snapshot] -> Maybe Snapshot
lastSnapshot (Snapshot 0 _ : t) = lastMay t
lastSnapshot _ = Nothing

{-
instance Exception MethodError
getLastSnapshot :: String -> IO (Maybe FilePath)
getLastSnapshot config = do
   
  let snapshotsDir = 
  snapshots <- runSystemDBus (listSnapshots config) >>= either throwIO pure
  pure $  lastSnapshot snapshots
--}

maybeToEither :: e -> Maybe a -> Either e a
maybeToEither e = maybe (Left e) Right

convertDBusSingleResult :: IsVariant a => [Variant] -> Either MethodError a
convertDBusSingleResult [singleResult] = maybeToEither (clientArgumentUnpackingError [singleResult]) $ fromVariant singleResult
convertDBusSingleResult results = Left $ clientArgumentUnpackingError results
