{-# LANGUAGE DeriveGeneric, DeriveAnyClass, FlexibleInstances, TypeApplications, OverloadedStrings, TupleSections, ApplicativeDo #-}
module SimpleDB (SnapshotUpload(..), getLatestUpload, printUploadsTable, uploadsList, insertSnapshotUpload, createDomain, getSnapshot, deleteRow, dumpCsv, loadCsv) where

import Safe (headMay)

import Unsafe.Coerce (unsafeCoerce)

import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.IORef.MonadIO (readIORef)

import Data.Function (on)
import Data.Foldable (traverse_)
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
import Data.Time.LocalTime --(utcToZonedTime)
import Data.Time.Format

import Aws (simpleAws, Configuration(..), makeCredentials, Credentials(..), Logger, TimeInfo(Timestamp))
import qualified Aws
import Aws.SimpleDb hiding (createDomain)

import Control.Lens (view)
import Control.Monad (void)
import Control.Monad.Catch (MonadThrow, Exception, throwM)
import Data.Bifunctor (bimap)

--import Data.String (IsString)
import Data.Text (Text, pack, unpack)
import Data.Text.Encoding (decodeUtf8, encodeUtf8Builder)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC

import GHC.Generics (Generic)

--import Data.Map.Strict (Map, fromList, toList, union, elems)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map -- (Map, fromList, toList, union, elems)
import qualified Data.HashMap.Strict as HM
import qualified Formatting as F
import Text.Layout.Table --(ColSpec(), rowG, left, right, fixedCol, unicodeRoundS, titlesH, tableString)

import MultipartGlacierUpload (NumBytes, GlacierConstraint, GlacierUpload(..),  HasGlacierSettings(..), _vaultName)
import Snapper (SnapshotRef)
import ArchiveSnapshotDescription

import Data.Csv
import qualified Data.Csv as Csv

import Util (lowerMaybe, throwLeftAs)
import AmazonkaSupport (fromTextThrow, digestFromHexThrow)


data SnapshotUpload = SnapshotUpload {
  _glacierUploadResult :: GlacierUpload,
  _snapshotInfo :: ArchiveSnapshotDescription,
  _timestamp :: ISO8601
} deriving (Show, Generic)

instance Eq SnapshotUpload where
  (==) = (==) `on` _timestamp

instance Ord SnapshotUpload where
  compare = compare `on` _timestamp

data DbColumnHeader = PreviousSnapshot | TimeStamp | Size | ArchiveId | TreeHashChecksum deriving (Show, Read, Eq, Ord, Enum, Bounded)

instance ToText DbColumnHeader where toText = pack . show
instance ToByteString DbColumnHeader where toBS = BSC.pack . show

dbColumnHeaders :: [DbColumnHeader]
dbColumnHeaders = [minBound..maxBound]

--snapshotIdHeader :: IsString a => a
snapshotIdHeader :: ByteString
snapshotIdHeader = "SnapshotId"

--formatISOTime :: UTCTime -> Text
--formatISOTime time = toText (Time time :: ISO8601)

fromISOTime :: Text -> Either String UTCTime
fromISOTime t = fromTime <$> fromText @ISO8601 t

--attr :: ToText v => DbColumnHeader -> v -> Attribute SetAttribute
--attr k v = replaceAttribute (pack $ show k) (toText v)

-- NonEmpty list-shaped, kinda
data DbRow a = DbRow {
  _id :: Text,
  _values :: [(DbColumnHeader, a)]
} deriving Show

instance ToNamedRecord (DbRow Text) where
  toNamedRecord (DbRow _id _values) = namedRecord $ (snapshotIdHeader, toBS _id) : map (bimap toBS toField) (Map.toList $ addMissing _values)

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

snapshotIdFromItem :: MonadThrow m => Item a -> m SnapshotRef
snapshotIdFromItem item = fromTextThrow $ itemName item

insertSnapshotUpload :: (GlacierConstraint r m) => SnapshotUpload -> m ()
insertSnapshotUpload snapshotUpload = do
  domainName <- _vaultName <$> view glacierSettingsL  
  void $ runSdbInAmazonka $ putSnapshotUpload domainName snapshotUpload

getSnapshot :: (GlacierConstraint r m) => SnapshotRef -> m SnapshotUpload
getSnapshot snapId = do
  domainName <- _vaultName <$> view glacierSettingsL  
  let _id = toText snapId
  GetAttributesResponse attributes <- runSdbInAmazonka $ getAttributes _id domainName
  rowToSnapshotUpload $ DbRow _id $ map nameAttribute attributes
  --sortOn fst $ map nameAttribute attributes
  
rowToSnapshotUpload :: MonadThrow m => DbRow Text -> m SnapshotUpload
rowToSnapshotUpload (DbRow _id _values) =
  glacierUploadFromList _id $ Map.toList $ addMissing _values

-- completeMissing Just
glacierUploadFromList :: MonadThrow m => Text -> [(DbColumnHeader, Maybe Text)] -> m SnapshotUpload
glacierUploadFromList currentSnapshot [(PreviousSnapshot, ps), (TimeStamp, Just ts), (Size, Just s), (ArchiveId, Just aid), (TreeHashChecksum, Just thcs)] = do
  snapshotInfo <- ArchiveSnapshotDescription <$> fromTextThrow currentSnapshot <*> traverse fromTextThrow ps
  glacierUpload <- GlacierUpload <$> fromTextThrow aid <*> digestFromHexThrow thcs <*> fromTextThrow s
  timestamp <- fromTextThrow ts
  pure $ SnapshotUpload glacierUpload snapshotInfo  timestamp
glacierUploadFromList _ other = throwM $ ColumnMisMashException $ "Can't parse upload info from incomplete / unordered columns: " ++ show other

authEnvFromAuth :: MonadIO m => Auth -> m AuthEnv
authEnvFromAuth (Auth auth) = pure auth
authEnvFromAuth (Ref _ authRef) = readIORef authRef

getAwsCreds :: (GlacierConstraint r m) => m Credentials
getAwsCreds = do
  --Auth auth <- fromMaybe (throwM "Ref creds not supported") <$> preview envAuth
  auth <- view envAuth >>= authEnvFromAuth
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

-- Umm... Amazonka has no "Warn" and Aws has no "Trace"
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

deleteRow :: (GlacierConstraint r m) => SnapshotRef -> m ()
deleteRow s = do
  domainName <- _vaultName <$> view glacierSettingsL  
  void $ runSdbInAmazonka $ DeleteAttributes (toText s) [] [] domainName

--throwLeftAs :: (MonadThrow m, Exception e) => (String -> e) -> Either String a -> m a
--throwLeftAs ex = either (throwM . ex) pure

--throwEither :: (MonadThrow m, Exception e) => Either e a -> m a
--throwEither = either throwM pure

data TableException = ColumnMisMashException String | CsvError String deriving (Show, Exception)

selectUploads :: (GlacierConstraint r m) => m [DbRow Text]
selectUploads = do
  domainName <- _vaultName <$> view glacierSettingsL  
  SelectResponse items _ <- runSdbInAmazonka $ select $ "SELECT * FROM " <> domainName <> " WHERE " <> toText TimeStamp <> " IS NOT NULL ORDER BY " <> toText TimeStamp
  pure $ map itemToRow items

uploadsList :: (GlacierConstraint r m) => m [SnapshotUpload]
uploadsList = do 
  uploads <- selectUploads
  traverse rowToSnapshotUpload uploads

printUploadsTable :: (GlacierConstraint r m) => m ()
printUploadsTable = do
  tz <- liftIO getCurrentTimeZone
  --writeCsv
  --SelectResponse items _ <- runSdbInAmazonka $ select $ "SELECT * FROM " <> vaultName
  rows <- selectUploads
  liftIO $ putStrLn $ formatDisplayTable tz rows


--The string stuff is only for displaying the table layout for the "uploads" command cause the table layout library uses strings.
--Also read and show can be derived, but use String.

itemToRow :: Item [Attribute Text] -> DbRow Text
itemToRow (Item name attributes) = DbRow name $ map nameAttribute attributes

-- Fill in missing columns with `Nothing`:
addMissing :: [(DbColumnHeader, a)] -> Map DbColumnHeader (Maybe a)
addMissing entries = fmap Just (Map.fromList entries) `Map.union` Map.fromList (map (,Nothing) dbColumnHeaders)

formatDisplayTable :: TimeZone -> [DbRow Text] -> String
formatDisplayTable tz rows = tableString
  (fixedCol 10 right : map columnFormat dbColumnHeaders)
  unicodeRoundS
  (titlesH $ "SnapshotId" : map show dbColumnHeaders)
  (map (rowG . formatDisplayRow tz) rows)

formatDisplayRow :: TimeZone -> DbRow Text -> [String]
formatDisplayRow tz (DbRow _id _values) = unpack _id : (map lowerMaybe . Map.elems . addMissing . map (formatAttribute tz)) _values

nameAttribute :: Attribute Text -> (DbColumnHeader, Text)
nameAttribute (ForAttribute name value) = (read $ unpack name, value)

formatAttribute :: TimeZone -> (DbColumnHeader, Text) -> (DbColumnHeader, String)
formatAttribute _ (Size, v) = (Size, F.formatToString (F.bytes @Double (F.fixed 2 F.% " ")) (read @NumBytes $ unpack v))
formatAttribute tz (TimeStamp, v) = (TimeStamp, either error (formatTime defaultTimeLocale (dateTimeFmt defaultTimeLocale) . utcToZonedTime tz) $ fromISOTime v)
--formatAttribute tz (TimeStamp, v) = (TimeStamp, either error (formatTime defaultTimeLocale rfc822DateFormat . utcToZonedTime tz) $ fromISOTime v)
formatAttribute _ (header, v) = (header, unpack v)

-- Make a column -> type typeclass?
-- Numbers (including timestamp) are right-justified
-- Text is left-justified and truncated.
columnFormat :: DbColumnHeader -> ColSpec
columnFormat PreviousSnapshot = fixedCol 16 right
columnFormat TimeStamp = column expand right noAlign noCutMark
--columnFormat TimeStamp = fixedCol 29 right
columnFormat Size = fixedCol 9 right
columnFormat ArchiveId = fixedCol 10 left
columnFormat TreeHashChecksum = fixedCol 16 left

loadCsv :: (GlacierConstraint r m) => FilePath -> m ()
loadCsv file = do
  csv <- liftIO $ BSL.readFile file
  (_, rows) <- throwLeftAs CsvError $ decodeByName csv
  --(_, rows) <- throwLeftAs CsvError $ decodeByName @(DbRow Text) csv
  snapshotsToInsert <- traverse rowToSnapshotUpload rows
  traverse_ insertSnapshotUpload snapshotsToInsert

{-
selectAll :: (GlacierConstraint r m) => m [[Text]]
selectAll = do
  vaultName <- _vaultName <$> view glacierSettingsL  
  SelectResponse items _ <- runSdbInAmazonka $ select $ "SELECT * FROM " <> vaultName
  pure $ map (\(Item _id _data) ->  _id : map attributeData _data) items
-}

dumpCsv :: (GlacierConstraint r m) => m ()
dumpCsv = do
  --deleteRow 261
  --deleteRow 275
  rows <- selectUploads
  liftIO $ BSLC.putStrLn $ encodeDefaultOrderedByName rows
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
  traverse snapshotIdFromItem item
