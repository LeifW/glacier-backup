{-# LANGUAGE DeriveGeneric, DeriveDataTypeable, StandaloneDeriving, FlexibleInstances, PackageImports, TypeApplications, RecordWildCards, OverloadedStrings #-}
module SimpleDB (SnapshotUpload(..), getLatestUpload, insertSnapshotUpload, createDomain, listDom, select, runSdbInAmazonka) where

import Safe (headMay)

import Control.Monad.IO.Class
import Control.Monad.IO.Unlift

import Unsafe.Coerce (unsafeCoerce)

import Data.Maybe (maybeToList)
--import Network.AWS (MonadAWS, runAWS, AWS, liftAWS, Credentials(..), Region(..), HasEnv, environment)
import Network.AWS.Env (envAuth, envLogger, envRegion)
--import Network.AWS.Auth (Auth(..))
import Network.AWS.Types (Auth(..), AuthEnv(..), AccessKey(..), SecretKey(..), Service(_svcEndpoint), Endpoint(_endpointHost))
import Network.AWS.Data.Sensitive (desensitise)
import Data.Coerce
import Control.Monad.Trans.AWS (runAWST, runResourceT, AWST, AWST', AWSConstraint, send, Env, envRegion, newEnv)
import qualified Control.Monad.Trans.AWS as Amazonka
import Data.Binary.Builder (Builder)
--import Network.AWS.SDB (Item, PutAttributes, Attribute, putAttributes, replaceableAttribute, paAttributes, aName, aValue, iName, iAttributes)
--import Network.AWS.SDB hiding (createDomain)
import qualified Network.AWS.SDB as SDB
import Network.AWS.Data.Text
import Network.AWS.Data.Time

import Aws (simpleAws, Configuration(..), makeCredentials, Credentials(..), Logger, TimeInfo(Timestamp))
import qualified Aws
--import Aws.SimpleDb (SdbConfiguration, CreateDomain(..), sdbHttpsPost, listDomains, ListDomains(..), ListDomainsResponse(..))
import Aws.SimpleDb hiding (createDomain)
import qualified Aws.SimpleDb as SimpleDb

import Control.Lens
import Control.Monad (void, (>=>), (<=<))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Catch (MonadThrow, Exception, throwM)
import Data.Bifunctor

import Data.Int (Int64)
import Data.Text (Text)
import Data.ByteString (ByteString)
import "cryptonite" Crypto.Hash (Digest, SHA256, HashAlgorithm, digestFromByteString)
import Data.ByteArray.Encoding
import Data.Text.Encoding (encodeUtf8, encodeUtf8Builder)

import Data.Data (Data)
import GHC.Generics (Generic)

import MultipartGlacierUpload (GlacierConstraint, GlacierUpload(..),  HasGlacierSettings(..), _vaultName)
import Snapper (Snapshot(..), UploadStatus(_deltaFrom), SnapshotRef)
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
  glacierUploadResult :: GlacierUpload,
  snapshotInfo :: ArchiveSnapshotDescription,
  timestamp :: ISO8601
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
glacierUploadToItem domainName (SnapshotUpload glacierUpload snapshotInfo timestamp) = putAttributes (toText currentSnapshotNum) ([
    replaceAttribute "archiveId" archiveId,
    replaceAttribute "size" (toText size),
    replaceAttribute "treeHashChecksum" (toText treeHashChecksum),
    replaceAttribute "timestamp" (toText timestamp)
  ] ++ (replaceAttribute "previousSnapshot" . toText <$> maybeToList previousSnapshotNum)
  ) domainName
  where
    GlacierUpload archiveId treeHashChecksum size = glacierUpload
    ArchiveSnapshotDescription currentSnapshotNum previousSnapshotNum = snapshotInfo

{-
glacierUploadToItem :: Text -> SnapshotUpload -> SDB.PutAttributes
glacierUploadToItem domainName (SnapshotUpload glacierUpload snapshotInfo timestamp) = SDB.putAttributes domainName (toText currentSnapshotNum) & set SDB.paAttributes ([
    SDB.replaceableAttribute "archiveId" archiveId,
    SDB.replaceableAttribute "size" (toText size),
    SDB.replaceableAttribute "treeHashChecksum" (toText treeHashChecksum),
    SDB.replaceableAttribute "timestamp" (toText timestamp)
  ] ++ (SDB.replaceableAttribute "previousSnapshot" . toText <$> maybeToList previousSnapshotNum)
  )
  where
    GlacierUpload archiveId treeHashChecksum size = glacierUpload
    ArchiveSnapshotDescription currentSnapshotNum previousSnapshotNum = snapshotInfo
    --previous = uploadedStatus snapshot >>= deltaFrom
-}

attributeToPair :: SDB.Attribute -> (Text, Text)
attributeToPair a = (a ^. SDB.aName, a ^. SDB.aValue)

lookupEither :: (Eq k, Show k) => k -> [(k, v)] -> Either String v
lookupEither k l = maybeToEither ("Couldn't find " <> show k <> " in " <> show (map fst l)) $ lookup k l

--lookupFromItem :: FromText a => Text -> Item -> Either String a
--lookupFromItem k item = lookupEither k (map attributeToPair $ item ^. iAttributes) >>= fromText

lookupFromItem :: Text -> SDB.Item -> Either String Text
lookupFromItem k item = lookupEither k (map attributeToPair $ item ^. SDB.iAttributes)

--snapshotFromItem :: Item -> Either String Snapshot
--snapshotFromItem item = Snapshot <$> fromText (item ^. iName) <*> (lookupFromItem "timestamp" item >>= fromISOTime)
snapshotIdFromItem :: Item a -> Either String SnapshotRef
snapshotIdFromItem item = fromText $ itemName item

insertSnapshotUpload :: (GlacierConstraint r m) => SnapshotUpload -> m ()
insertSnapshotUpload row = do
  domainName <- _vaultName <$> view glacierSettingsL  
  void $ runSdbInAmazonka $ glacierUploadToItem domainName row

getAwsCreds :: (GlacierConstraint r m) => m Credentials
getAwsCreds = do
  Auth auth <- view envAuth
  makeCredentials (coerce $ _authAccess auth) (coerce $ desensitise $ _authSecret auth)

getAwsLogger :: (GlacierConstraint r m) => m Logger
getAwsLogger = amazonkaToAwsLogger <$> view envLogger
  
amazonkaToAwsLogger :: (Amazonka.LogLevel -> Builder -> IO ()) -> (Aws.LogLevel -> Text -> IO ())
amazonkaToAwsLogger amazonkaLogger logLevel msg = amazonkaLogger (awsToAmazonkaLogLevel logLevel) (encodeUtf8Builder msg)

{- Whoops, wrong direction
amazonkaToAwsLogLevel :: Amazonka.LogLevel -> Aws.LogLevel
amazonkaToAwsLogLevel Amazonka.Trace = Aws.Debug
amazonkaToAwsLogLevel Amazonka.Debug = Aws.Info
amazonkaToAwsLogLevel Amazonka.Info = Aws.Warning
amazonkaToAwsLogLevel Amazonka.Error = Aws.Error
-}

-- Umm... Amazonka has no "Warn" and Aws has no "Trace
-- I'll think about this later.
awsToAmazonkaLogLevel :: Aws.LogLevel -> Amazonka.LogLevel
awsToAmazonkaLogLevel Aws.Debug = Amazonka.Trace 
awsToAmazonkaLogLevel Aws.Info = Amazonka.Debug 
awsToAmazonkaLogLevel Aws.Warning = Amazonka.Info 
awsToAmazonkaLogLevel Aws.Error = Amazonka.Error 


getAwsConfiguration :: (GlacierConstraint r m) => m Configuration
getAwsConfiguration = do
  creds <- getAwsCreds
  logger <- getAwsLogger
  pure $ Configuration Timestamp creds logger Nothing

getSdbConfig :: GlacierConstraint r m => m (SdbConfiguration Aws.NormalQuery)
getSdbConfig = do
  region <- view envRegion
  pure $ sdbHttpsPost $ _endpointHost $ _svcEndpoint SDB.sdb region
  
-- Only runs Sdb Requests - see the unsafeCoerce applied to the SdbConfiguration
runSdbInAmazonka :: (Aws.Transaction req a, Aws.AsMemoryResponse a, GlacierConstraint r m) => req -> m (Aws.MemoryResponse a)
runSdbInAmazonka req = do
  awsConfig <- getAwsConfiguration 
  sdbConfig <- getSdbConfig
  simpleAws awsConfig (unsafeCoerce sdbConfig) req

listDom :: (GlacierConstraint r m) => m [Text]
listDom = do
  ListDomainsResponse domains _ <- runSdbInAmazonka listDomains
  --ListDomainsResponse domains _ <- simpleAws awsConfig sdbConfig listDomains
  pure domains

createDomain :: (GlacierConstraint r m) => m ()
createDomain = do
  domainName <- _vaultName <$> view glacierSettingsL  
  --awsConfig <- getAwsConfiguration 
  --sdbConfig <- getSdbConfig
  --void $ simpleAws awsConfig sdbConfig $ createDomain domainName
  void $ runSdbInAmazonka $ CreateDomain domainName
  --void $ (simpleAws <$> getAwsConfiguration <*> getSdbConfig) <$> CreateDomain domainName
  

throwEither :: (MonadThrow m, Exception e) => Either e a -> m a
throwEither = either throwM pure

data SimpleDBParseException = SimpleDBParseException String deriving Show

instance Exception SimpleDBParseException

getLatestUpload :: (GlacierConstraint r m) => m (Maybe SnapshotRef)
getLatestUpload = do
  --GlacierSettings _ vaultName <- vaultName <$> view glacierSettingsL  
  vaultName <- _vaultName <$> view glacierSettingsL  
  --resp <- runResourceT $ send $ set SDB.sConsistentRead (Just True) $ SDB.select $ "SELECT timestamp FROM " <> vaultName <> " ORDER BY timestamp DESC LIMIT 1"
  -- put in a dummy where clause "WHERE timestamp IS NOT NULL", because otherwise SimpleDB complains:
  -- "Invalid sort expression. The sort attribute must be present in at least one of the predicates, and the predicate cannot contain the is null operator."
  -- See https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SortingDataSelect.html
  resp <- runSdbInAmazonka $ select $ "SELECT timestamp FROM " <> vaultName <> " WHERE timestamp IS NOT NULL ORDER BY timestamp DESC LIMIT 1"
  let item = headMay $ srItems resp
  traverse (throwEither . first SimpleDBParseException . snapshotIdFromItem) item
