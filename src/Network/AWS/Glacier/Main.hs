{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass, RecordWildCards, TypeApplications #-}
module Main where

import System.IO (stdout)
import Data.Text (Text, pack, unpack, toLower, intercalate)

import GHC.Generics (Generic)

import Options.Applicative
import Data.Aeson (Value(..), ToJSON, FromJSON(..), object, (.=))
import Data.Yaml.Config (loadYamlSettings, useEnv)

import Control.Monad.Reader (runReaderT)
import Control.Monad.IO.Class (liftIO)
import Network.AWS.Data.Text (ToText, FromText, fromText, toText)
import Network.AWS.Data.JSON (parseJSONText)
import Control.Monad.Trans.AWS (Region, Credentials(Discover), LogLevel(..), envLogger, envRegion, newEnv, newLogger)
import Network.AWS.Glacier.Types (Tier(..))
import Control.Lens.Setter (set)

import LiftedGlacierRequests (GlacierEnv(..), GlacierSettings(..))
import AllowedPartSizes (PartSize)
import UploadSnapshot
import SimpleDB (dumpCsv, loadCsv, printUploadsTable)
import Restore

instance FromJSON LogLevel where parseJSON = parseJSONText "LogLevel"

lowerText :: ToText a => a -> Text
lowerText = toLower . toText

showDefaultText :: ToText a => Mod f a
showDefaultText = showDefaultWith $ unpack . lowerText

tierLabels :: String
tierLabels = unpack $ intercalate " | " $ map lowerText [minBound @ Tier .. maxBound]

-- Like 'auto' but for using FromText instead of Read.
fromFromText :: FromText a => ReadM a
fromFromText = eitherReader $ fromText . pack

data Config = Config {
  snapper_config_name :: String, -- Defaults to "root"
  region :: Region,
  glacier_vault_name :: Text,
  upload_part_size_MB :: PartSize,
  aws_account_id :: Text, -- A 12-digit number, defaults to account of credentials. (-)
  --aws_account_id :: Maybe Int64 -- A 12-digit number, defaults to account of credentials. (-)
  log_level :: LogLevel
} deriving (Show, Generic, FromJSON)

configDefaults :: Value
configDefaults = object [
    "snapper_config_name" .= String "root",
    "aws_account_id"      .= String "-",
    "upload_part_size_MB" .= Number 32,
    "log_level"           .= String "info"
  ]

data Command = Provision | ListUploads | Delete SnapshotRef | DumpCSV | LoadCSV FilePath | InitiateRetrieval Tier | Restore FilePath

data CmdOpts = CmdOpts {
  configFile :: !FilePath,
  subCommand :: !(Maybe Command)
}

argParser :: ParserInfo CmdOpts
argParser = info (helper <*> programOpts) $
     fullDesc
  <> progDesc "Uploads snapshot to Glacier when no command given."
  <> header "glacier-backup - Upload btrfs snapshots to Glacier (incrementally)"

programOpts :: Parser CmdOpts
programOpts = CmdOpts
  <$> strOption (short 'c' <> long "config-file" <> metavar "PATH" <> showDefault <> value "/etc/glacier-backup.yml" <> help "Path to config file.")
  <*> optional (hsubparser commandOpt)

commandOpt :: Mod CommandFields Command
commandOpt =
     command "provision" (info (pure Provision) (progDesc "Create the Glacier vault and SimpleDB domain (table)"))
  <> command "uploads" (info (pure ListUploads) (progDesc "List the snapshots uploaded to Glacier"))
  <> command "delete" (info (Delete <$> argument auto (metavar "SNAPSHOT_NUMBER")) (progDesc "Delete a snapshot from Glacier (and SimpleDB)"))
  <> command "dumpcsv" (info (pure DumpCSV) (progDesc "Dump the index database to .csv"))
  <> command "loadcsv" (info (LoadCSV <$> argument str (metavar "CSV_FILE")) (progDesc "Load a .csv file into the index database."))
  <> command "initiate-retrieval" (info (InitiateRetrieval <$> option fromFromText (long "tier" <> short 't' <> metavar tierLabels <> showDefaultText <> value TBulk)) (progDesc "print a thing"))
  <> command "restore" (info (Restore <$> argument str (metavar "DIR")) (progDesc "Download the btrfs snapshots and restore subvolume to path."))

setupConfig :: FilePath -> IO (String, GlacierEnv)
setupConfig configFilePath = do
  Config{..} <- loadYamlSettings [configFilePath] [configDefaults] useEnv
  let glacierConfig = GlacierSettings aws_account_id glacier_vault_name upload_part_size_MB 
  lgr <- newLogger log_level stdout
  awsEnv <- set envRegion region . set envLogger lgr <$> newEnv Discover
  pure (snapper_config_name, GlacierEnv awsEnv glacierConfig)

main :: IO ()
main = do
  CmdOpts configFilePath subCommand <- execParser argParser
  (snapper_config_name, glacierEnv) <- setupConfig configFilePath
  flip runReaderT glacierEnv $ case subCommand of
    Nothing -> uploadBackup snapper_config_name
    Just Provision -> provisionAWS
    Just ListUploads -> printUploadsTable
    Just (Delete snapshot) -> deleteUpload snapper_config_name snapshot
    Just DumpCSV -> dumpCsv
    Just (LoadCSV file) -> loadCsv file
    Just (InitiateRetrieval tier) -> initiateRetrieval tier >>= mapM_ (liftIO . print)
    Just (Restore dir) -> restore dir

