{-# LANGUAGE DeriveGeneric, DeriveDataTypeable, StandaloneDeriving, FlexibleInstances, PackageImports, TypeApplications, RecordWildCards, OverloadedStrings #-}
module SimpleDB (SnapshotUpload(..), getLatestUpload, insertSnapshotUpload) where

import Safe (headMay)

import Data.Maybe (maybeToList)
--import Network.AWS (MonadAWS, runAWS, AWS, liftAWS, Credentials(..), Region(..), HasEnv, environment)
import Control.Monad.Trans.AWS (runAWST, runResourceT, AWST, AWST', AWSConstraint, send, Env, LogLevel(..), newLogger, envLogger, envRegion, newEnv)
import Network.AWS.SDB
import Network.AWS.Data.Text
import Network.AWS.Data.Time

import Control.Lens
import Control.Monad (void, (>=>), (<=<))
import Control.Monad.Catch (MonadThrow, Exception, throwM)
import Data.Bifunctor

import Data.Int (Int64)
import Data.Text (Text)
import Data.ByteString (ByteString)
import "cryptonite" Crypto.Hash (Digest, SHA256, HashAlgorithm, digestFromByteString)
import Data.ByteArray.Encoding
import Data.Text.Encoding (encodeUtf8)
import Unsafe.Coerce (unsafeCoerce)

import Data.Data (Data)
import GHC.Generics (Generic)

import GlacierReaderT (HasGlacierSettings(..), _vaultName)
import MultipartGlacierUpload (GlacierUpload(..))
import Snapper (Snapshot(..), UploadStatus(deltaFrom), SnapshotRef)
--import Snapper (UploadStatus(deltaFrom), SnapshotRef)
import ArchiveSnapshotDescription

import Util

import Data.Aeson
--deriving instance Data (Digest a)
--deriving instance Data SHA256

--instance Data (Digest SHA256) where
--instance (Data a) => Data (Digest a) where
{-
instance Typeable a => Data (Digest a) where
    gfoldl k z digest = unsafeCoerce $ gfoldl @(Block Word8) k z (unsafeCoerce digest)
    gunfold k z c = unsafeCoerce $ gunfold @(Block Word8) k z c
    toConstr _ = digestConstructor
    dataTypeOf _ = digestType
    --gfoldl k z digest = z (unsafeCoerce @(Block Word8) @(Digest SHA256)) `k` (unsafeCoerce @(Digest SHA256) @(Block Word8) digest)
-}

    --gfoldl k z digest = unsafeCoerce $ gfoldl @(Block Word8) k z (unsafeCoerce digest)

--digestConstructor = mkConstr digestType "Digest" [] Prefix
--digestType = mkNoRepType "Crypto.Hash.Types.Digest"
--digestConstructor = mkConstr digestType "Digest" [] Prefix
--digestType = mkDataType "Crypto.Hash.Types.Digest" [digestConstructor]

--instance HashAlgorithm a => FromText (Digest a) where
digestFromHex :: HashAlgorithm a => Text -> Either String (Digest a)
digestFromHex = maybeToEither "Can't parse Digest from bytes" . digestFromByteString <=<
                convertFromBase @ByteString @ByteString Base16 . encodeUtf8

data SnapshotUpload = SnapshotUpload {
  glacierUpload :: GlacierUpload,
  snapshotInfo :: ArchiveSnapshotDescription
} deriving (Show, Data, Generic)
{-
data SnapshotUpload = SnapshotUpload {
  glacierUpload :: GlacierUpload,
  snapshotNum :: SnapshotRef,
  timestamp :: ISO8601,
  previous :: Maybe SnapshotRef
} deriving (Show, Data, Generic)
-}
{-
data SnapshotUpload = SnapshotUpload {
  archiveId :: Text,
  size :: Int64,
  checksum :: Digest SHA256,
  snapshotNum :: SnapshotRef,
  timestamp :: ISO8601,
  previous :: Maybe SnapshotRef
} deriving (Show, Eq, Data, Generic)
-}

instance ToJSON (Digest SHA256) where
  toJSON sha = String $ toText sha
--instance ToJSON Snapshot
--instance ToJSON UploadedItem

formatISOTime :: UTCTime -> Text
formatISOTime time = toText (Time time :: ISO8601)

fromISOTime :: Text -> Either String UTCTime
fromISOTime t = fromTime <$> fromText @ISO8601 t

glacierUploadToItem :: Text -> SnapshotUpload -> PutAttributes
glacierUploadToItem domainName (SnapshotUpload glacierUpload snapshotInfo) = putAttributes domainName (toText currentSnapshotNum) & set paAttributes ([
    replaceableAttribute "archiveId" archiveId,
    replaceableAttribute "size" (toText size),
    replaceableAttribute "treeHashChecksum" (toText treeHashChecksum),
    replaceableAttribute "timestamp" (toText timestamp)
  ] ++ (replaceableAttribute "previousSnapshot" . toText <$> maybeToList previousSnapshotNum)
  )
  where
    GlacierUpload archiveId treeHashChecksum size = glacierUpload
    ArchiveSnapshotDescription currentSnapshotNum timestamp previousSnapshotNum = snapshotInfo
    --previous = uploadedStatus snapshot >>= deltaFrom


attributeToPair :: Attribute -> (Text, Text)
attributeToPair a = (a ^. aName, a ^. aValue)

lookupEither :: (Eq k, Show k) => k -> [(k, v)] -> Either String v
lookupEither k l = maybeToEither ("Couldn't find " <> show k <> " in " <> show (map fst l)) $ lookup k l

--lookupFromItem :: FromText a => Text -> Item -> Either String a
--lookupFromItem k item = lookupEither k (map attributeToPair $ item ^. iAttributes) >>= fromText

lookupFromItem :: Text -> Item -> Either String Text
lookupFromItem k item = lookupEither k (map attributeToPair $ item ^. iAttributes)

--snapshotFromItem :: Item -> Either String Snapshot
--snapshotFromItem item = Snapshot <$> fromText (item ^. iName) <*> (lookupFromItem "timestamp" item >>= fromISOTime)
snapshotIdFromItem :: Item -> Either String SnapshotRef
snapshotIdFromItem item = fromText $ item ^. iName

insertSnapshotUpload :: (AWSConstraint r m, HasGlacierSettings r) => SnapshotUpload -> m ()
insertSnapshotUpload row = do
  vaultName <- _vaultName <$> view glacierSettingsL  
  void $ send $ glacierUploadToItem vaultName row

throwEither :: (MonadThrow m, Exception e) => Either e a -> m a
throwEither = either throwM pure

data SimpleDBParseException = SimpleDBParseException String deriving Show

instance Exception SimpleDBParseException

getLatestUpload :: (AWSConstraint r m, HasGlacierSettings r) => m (Maybe SnapshotRef)
getLatestUpload = do
  --GlacierSettings _ vaultName <- vaultName <$> view glacierSettingsL  
  vaultName <- _vaultName <$> view glacierSettingsL  
  resp <- send $ set sConsistentRead (Just True) $ select $ "SELECT timestamp FROM " <> vaultName <> " ORDER BY timestamp DESC LIMIT 1"
  let item = headMay $ resp ^. srsItems
  traverse (throwEither . first SimpleDBParseException . snapshotIdFromItem) item
