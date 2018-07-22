{-# LANGUAGE DeriveGeneric,  FlexibleInstances, PackageImports, TypeApplications, OverloadedStrings, TupleSections #-}
module SimpleDB (SnapshotUpload(..), getLatestUpload, listUploads, insertSnapshotUpload, createDomain, listDom, select, runSdbInAmazonka) where

import Safe (headMay)
import Data.Maybe (catMaybes)
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
import Network.AWS.Data.ByteString (ToByteString(toBS), showBS)
import Network.AWS.Data.Time

import Aws (simpleAws, Configuration(..), makeCredentials, Credentials(..), Logger, TimeInfo(Timestamp))
import qualified Aws
import Aws.SimpleDb hiding (createDomain)

import Control.Lens (view)
import Control.Monad (void, (>=>), (<=<))
import Control.Monad.Catch (MonadThrow, Exception, throwM)
import Data.Bifunctor (bimap, first)

import Data.String (IsString)
import Data.Text (Text, pack, unpack)
import Data.Text.Encoding (decodeUtf8, encodeUtf8, encodeUtf8Builder)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import "cryptonite" Crypto.Hash (Digest, HashAlgorithm, digestFromByteString)
import Data.ByteArray.Encoding (Base(Base16), convertFromBase)

import GHC.Generics (Generic)

import Data.Map.Strict (Map, fromList, toList, union, elems)
import qualified Data.HashMap.Strict as HM
import qualified Formatting as F
import Text.Layout.Table (ColSpec(..), RowGroup, rowG, left, right, fixedCol, unicodeRoundS, titlesH, tableString)

import MultipartGlacierUpload (NumBytes, GlacierConstraint, GlacierUpload(..),  HasGlacierSettings(..), _vaultName)
import Snapper (SnapshotRef)
import ArchiveSnapshotDescription

import Data.Csv
import qualified Data.Csv as Csv

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
instance ToByteString DbColumnHeader where toBS = BSC.pack . show

dbColumnHeaders :: [DbColumnHeader]
dbColumnHeaders = [minBound..maxBound]

snapshotIdHeader :: IsString a => a
snapshotIdHeader = "SnapshotId"

formatISOTime :: UTCTime -> Text
formatISOTime time = toText (Time time :: ISO8601)

fromISOTime :: Text -> Either String UTCTime
fromISOTime t = fromTime <$> fromText @ISO8601 t

attr :: ToText v => DbColumnHeader -> v -> Attribute SetAttribute
attr k v = replaceAttribute (pack $ show k) (toText v)

-- NonEmpty list-shaped, kinda
data DbRow a = DbRow {
  _id :: Text,
  _values :: [(DbColumnHeader, a)]
} deriving Show

instance ToNamedRecord (DbRow Text) where
  toNamedRecord (DbRow _id _values) = namedRecord $ (snapshotIdHeader, toBS _id) : map (bimap toBS toField) (toList $ completeSet _values)

instance FromNamedRecord (DbRow Text) where
  parseNamedRecord m = do
    _id <- Csv.lookup m snapshotIdHeader
    pure $ DbRow (decodeUtf8 _id) (map (bimap (read . showBS) decodeUtf8) $ HM.toList $ HM.delete snapshotIdHeader $ HM.filter (not . BS.null) m)

instance DefaultOrdered (DbRow Text) where
  headerOrder _ = header $ snapshotIdHeader : map toBS dbColumnHeaders

att :: ToText v => DbColumnHeader -> v -> (DbColumnHeader, Text)
att k v = (k, toText v)

snapshotUploadToRow :: SnapshotUpload -> DbRow Text
snapshotUploadToRow (SnapshotUpload glacierUpload snapshotInfo timestamp) = DbRow (toText currentSnapshotNum) $
  (att PreviousSnapshot <$> maybeToList previousSnapshotNum) ++
  [
    att TimeStamp timestamp,
    att Size size,
    att ArchiveId archiveId,
    att TreeHashChecksum treeHashChecksum
  ] 
  where
    GlacierUpload archiveId treeHashChecksum size = glacierUpload
    ArchiveSnapshotDescription currentSnapshotNum previousSnapshotNum = snapshotInfo

--rowToItem domainName s = putAttributes currentSnapshotNum [replaceAttribute (pack $ show k) v | (k, v) <-  values] domainName
--snapshotUploadToItem :: Text -> Row -> PutAttributes
putSnapshotUpload :: Text -> SnapshotUpload -> PutAttributes
putSnapshotUpload domainName s = putAttributes currentSnapshotNum [replaceAttribute (pack $ show k) v | (k, v) <-  values] domainName
  where DbRow currentSnapshotNum values = snapshotUploadToRow s

snapshotIdFromItem :: Item a -> Either String SnapshotRef
snapshotIdFromItem item = fromText $ itemName item

insertSnapshotUpload :: (GlacierConstraint r m) => SnapshotUpload -> m ()
insertSnapshotUpload snapshotUpload = do
  domainName <- _vaultName <$> view glacierSettingsL  
  void $ runSdbInAmazonka $ putSnapshotUpload domainName snapshotUpload

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
  --SelectResponse items _ <- runSdbInAmazonka $ select $ "SELECT * FROM " <> vaultName
  SelectResponse items _ <- runSdbInAmazonka $ select $ "SELECT * FROM " <> vaultName <> " WHERE " <> toText TimeStamp <> " IS NOT NULL ORDER BY " <> toText TimeStamp
  liftIO $ putStrLn $ formatDisplayTable $ map itemToRow items


--The string stuff is only for displaying the table layout for the "uploads" command cause the table layout library uses strings.
--Also read and show can be derived, but use String.

itemToRow :: Item [Attribute Text] -> DbRow Text
itemToRow (Item name attributes) = DbRow name $ map nameAttribute attributes

-- Fill in missing columns with `Nothing`:
completeSet :: [(DbColumnHeader, a)] -> Map DbColumnHeader (Maybe a)
completeSet entries = fmap Just (fromList entries) `union` fromList (map (,Nothing) dbColumnHeaders)

formatDisplayTable :: [DbRow Text] -> String
formatDisplayTable rows = tableString
  (fixedCol 10 right : map columnFormat dbColumnHeaders)
  unicodeRoundS
  (titlesH $ "SnapshotId" : map show dbColumnHeaders)
  (map (rowG . formatDisplayRow) rows)

formatDisplayRow :: DbRow Text -> [String]
formatDisplayRow (DbRow _id _values) = unpack _id : (map lowerMaybe . elems . completeSet . map formatAttribute) _values

nameAttribute :: Attribute Text -> (DbColumnHeader, Text)
nameAttribute (ForAttribute name value) = (read $ unpack name, value)

formatAttribute :: (DbColumnHeader, Text) -> (DbColumnHeader, String)
formatAttribute (Size, v) = (Size, F.formatToString (F.bytes @Double (F.fixed 2 F.% " ")) (read @NumBytes $ unpack v))
formatAttribute (header, v) = (header, unpack v)

-- Make a column -> type typeclass?
-- Numbers (including timestamp) are right-justified
-- Text is left-justified and truncated.
columnFormat :: DbColumnHeader -> ColSpec
columnFormat PreviousSnapshot = fixedCol 16 right
columnFormat TimeStamp = fixedCol 20 right
columnFormat Size = fixedCol 12 right
columnFormat ArchiveId = fixedCol 10 left
columnFormat TreeHashChecksum = fixedCol 16 left

{-
 Current example output:
╭────────────┬──────────────────┬──────────────────────┬──────────────┬────────────┬──────────────────╮
│ SnapshotId │ PreviousSnapshot │      TimeStamp       │     Size     │ ArchiveId  │ TreeHashChecksum │
╞════════════╪══════════════════╪══════════════════════╪══════════════╪════════════╪══════════════════╡
│        297 │              275 │ 2018-07-13T06:03:21Z │     25.29 MB │ zsrNu0Jhv… │ 42a2698858551f7… │
╰────────────┴──────────────────┴──────────────────────┴──────────────┴────────────┴──────────────────╯
-}
getLatestUpload :: (GlacierConstraint r m) => m (Maybe SnapshotRef)
getLatestUpload = do
  vaultName <- _vaultName <$> view glacierSettingsL  
  -- put in a dummy where clause "WHERE timestamp IS NOT NULL", because otherwise SimpleDB complains:
  -- "Invalid sort expression. The sort attribute must be present in at least one of the predicates, and the predicate cannot contain the is null operator."
  -- See https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SortingDataSelect.html
  resp <- runSdbInAmazonka $ select $ "SELECT " <> toText TimeStamp <> " FROM " <> vaultName <> " WHERE " <> toText TimeStamp <> " IS NOT NULL ORDER BY " <> toText TimeStamp <> " DESC LIMIT 1"
  let item = headMay $ srItems resp
  traverse (throwEither . first SimpleDBParseException . snapshotIdFromItem) item
