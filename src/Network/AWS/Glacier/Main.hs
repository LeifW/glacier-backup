{-# LANGUAGE OverloadedStrings, StandaloneDeriving, DeriveGeneric, DeriveAnyClass, RecordWildCards #-}
module Main where

import System.IO (stdout)
import Data.Text (Text)

import GHC.Generics (Generic)

import Options.Applicative
import Options.Applicative.Types (readerAsk)
import Data.Yaml (Value(..), ToJSON, FromJSON, object, (.=))
import Data.Yaml.Config (loadYamlSettings, useEnv)

import Control.Monad.Reader (runReaderT)
import Network.AWS.Data.Text (FromText)
import qualified Network.AWS.Data.Text as AWSText
import Control.Monad.Trans.AWS (Region, Credentials(Discover), LogLevel(..), envLogger, envRegion, newEnv, newLogger)
import Control.Lens.Setter (set)

import LiftedGlacierRequests (GlacierEnv(..), GlacierSettings(..))
import AllowedPartSizes (PartSize)
import UploadSnapshot
import SimpleDB (dumpCsv, loadCsv, listUploads)

deriving instance Generic LogLevel
instance FromJSON LogLevel
instance ToJSON LogLevel

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
    "log_level"           .= String "Info"
  ]

data Command = Provision | ListUploads | Delete SnapshotRef | DumpCSV | LoadCSV FilePath

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
  <> command "dumpcsv" (info (pure DumpCSV) (progDesc "Dump the database to a .csv file"))
  <> command "loadcsv" (info (LoadCSV <$> argument str (metavar "CSV_FILE")) (progDesc "Load a .csv file into the index database."))

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
    Just ListUploads -> listUploads
    Just (Delete snapshot) -> deleteUpload snapper_config_name snapshot
    Just DumpCSV -> dumpCsv
    Just (LoadCSV file) -> loadCsv file

