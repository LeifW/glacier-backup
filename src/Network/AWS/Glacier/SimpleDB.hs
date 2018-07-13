{-# LANGUAGE DeriveGeneric,  FlexibleInstances, PackageImports, TypeApplications, OverloadedStrings #-}
module SimpleDB (SnapshotUpload(..), getLatestUpload, listUploads, insertSnapshotUpload, createDomain, listDom, select, runSdbInAmazonka) where

import Safe (headMay)
import Data.List (sortOn)

import Unsafe.Coerce (unsafeCoerce)

import Control.Monad.IO.Class (liftIO)


import Data.Maybe (maybeToList)
import Network.AWS.Env (envAuth, envLogger, envRegion)
import Network.AWS.Types (Auth(..), AuthEnv(..), AccessKey(..), SecretKey(..), Service(_svcEndpoint), Endpoint(_endpointHost))
import Network.AWS.Data.Sensitive (desensitise)
import Data.Coerce (coerce)
import qualified Control.Monad.Trans.AWS as Amazonka
import Data.Binary.Builder (Builder)
import qualified Network.AWS.SDB as SDB
import Network.AWS.Data.Text
import Network.AWS.Data.Time

import Aws (simpleAws, Configuration(..), makeCredentials, Credentials(..), Logger, TimeInfo(Timestamp))
import qualified Aws
import Aws.SimpleDb hiding (createDomain)

import Control.Lens
import Control.Monad (void, (>=>), (<=<))
import Control.Monad.Catch (MonadThrow, Exception, throwM)
import Data.Bifunctor

import Data.Text (Text, pack, unpack)
import Data.ByteString (ByteString)
import "cryptonite" Crypto.Hash (Digest, HashAlgorithm, digestFromByteString)
import Data.ByteArray.Encoding (Base(Base16), convertFromBase)
import Data.Text.Encoding (encodeUtf8, encodeUtf8Builder)

import GHC.Generics (Generic)

import Formatting
--import Text.Layout.Table

import MultipartGlacierUpload (NumBytes, GlacierConstraint, GlacierUpload(..),  HasGlacierSettings(..), _vaultName)
import Snapper (SnapshotRef)
import ArchiveSnapshotDescription

import Util

digestFromHex :: HashAlgorithm a => Text -> Either String (Digest a)
digestFromHex = maybeToEither "Can't parse Digest from bytes" . digestFromByteString <=<
                convertFromBase @ByteString @ByteString Base16 . encodeUtf8

data SnapshotUpload = SnapshotUpload {
  _glacierUploadResult :: GlacierUpload,
  _snapshotInfo :: ArchiveSnapshotDescription,
  _timestamp :: ISO8601
} deriving (Show, Generic)

data DbColumnHeader = PreviousSnapshot | TimeStamp | Size | ArchiveId | TreeHashChecksum deriving (Show, Read, Eq, Ord, Enum, Bounded)

instance ToText DbColumnHeader where toText = pack . show

columnHeaders :: [String]
columnHeaders = "SnapshotId" : map @DbColumnHeader show [minBound..maxBound]
--columnHeaders = "SnapshotId" : map show ( [minBound..maxBound] :: [DbColumnHeader] )

attr :: ToText v => DbColumnHeader -> v -> Attribute SetAttribute
attr k v = replaceAttribute (pack $ show k) (toText v)

formatISOTime :: UTCTime -> Text
formatISOTime time = toText (Time time :: ISO8601)

fromISOTime :: Text -> Either String UTCTime
fromISOTime t = fromTime <$> fromText @ISO8601 t

glacierUploadToItem :: Text -> SnapshotUpload -> PutAttributes
glacierUploadToItem domainName (SnapshotUpload glacierUpload snapshotInfo timestamp) = putAttributes (toText currentSnapshotNum) (
  (attr PreviousSnapshot <$> maybeToList previousSnapshotNum) ++ 
  [
    attr TimeStamp timestamp,
    attr Size size,
    attr ArchiveId archiveId,
    attr TreeHashChecksum treeHashChecksum
  ] 
  ) domainName
  where
    GlacierUpload archiveId treeHashChecksum size = glacierUpload
    ArchiveSnapshotDescription currentSnapshotNum previousSnapshotNum = snapshotInfo


{-
attributeToPair :: SDB.Attribute -> (Text, Text)
attributeToPair a = (a ^. SDB.aName, a ^. SDB.aValue)

lookupEither :: (Eq k, Show k) => k -> [(k, v)] -> Either String v
lookupEither k l = maybeToEither ("Couldn't find " <> show k <> " in " <> show (map fst l)) $ lookup k l

lookupFromItem :: Text -> SDB.Item -> Either String Text
lookupFromItem k item = lookupEither k (map attributeToPair $ item ^. SDB.iAttributes)
-}

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
  loggr <- getAwsLogger
  pure $ Configuration Timestamp creds loggr Nothing

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
  pure domains

createDomain :: (GlacierConstraint r m) => m ()
createDomain = do
  domainName <- _vaultName <$> view glacierSettingsL  
  void $ runSdbInAmazonka $ CreateDomain domainName
  

throwEither :: (MonadThrow m, Exception e) => Either e a -> m a
throwEither = either throwM pure

data SimpleDBParseException = SimpleDBParseException String deriving Show

instance Exception SimpleDBParseException

listUploads :: (GlacierConstraint r m) => m ()
listUploads = do
  vaultName <- _vaultName <$> view glacierSettingsL  
  SelectResponse items _ <- runSdbInAmazonka $ select $ "SELECT * FROM " <> vaultName
  --SelectResponse items _ <- runSdbInAmazonka $ select $ "SELECT * FROM " <> vaultName <> " WHERE " <> toText TimeStamp <> " IS NOT NULL ORDER BY " <> toText TimeStamp
  mapM_ (liftIO . print) items
  --mapM_ (liftIO . print . itemToRow) items

--The string stuff is only for displaying the table layout for the "uploads" command cause the table layout library uses strings.
--Also read and show can be derived, but use String.

itemToRow :: Item [Attribute Text] -> [String]
itemToRow (Item name attributes) = unpack name : (map formatAttribute . sortOn fst . map nameAttribute) attributes
--itemToRow (Item name attributes) = unpack name : map (unpack . attributeData) attributes
--map formatAttribute . sortOn fst . map read

nameAttribute :: Attribute Text -> (DbColumnHeader, String)
nameAttribute (ForAttribute name value) = (read $ unpack name, unpack value)

formatAttribute :: (DbColumnHeader, String) -> String
formatAttribute (Size, v) = formatToString (bytes @Double (fixed 2 % " ")) (read @NumBytes v) 
formatAttribute (_, v) = v


getLatestUpload :: (GlacierConstraint r m) => m (Maybe SnapshotRef)
getLatestUpload = do
  vaultName <- _vaultName <$> view glacierSettingsL  
  -- put in a dummy where clause "WHERE timestamp IS NOT NULL", because otherwise SimpleDB complains:
  -- "Invalid sort expression. The sort attribute must be present in at least one of the predicates, and the predicate cannot contain the is null operator."
  -- See https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SortingDataSelect.html
  resp <- runSdbInAmazonka $ select $ "SELECT timestamp FROM " <> vaultName <> " WHERE timestamp IS NOT NULL ORDER BY timestamp DESC LIMIT 1"
  let item = headMay $ srItems resp
  traverse (throwEither . first SimpleDBParseException . snapshotIdFromItem) item
