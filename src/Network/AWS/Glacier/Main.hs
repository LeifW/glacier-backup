{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass, RecordWildCards #-}
module Main where

import System.IO (stdout)
import Data.Text (Text)

import GHC.Generics (Generic)

import Options.Applicative
import Data.Yaml (Value(..), FromJSON, object, (.=))
import Data.Yaml.Config (loadYamlSettings, useEnv)

import Control.Monad.Reader (runReaderT)
import Control.Monad.Trans.AWS (Region, Credentials(Discover), LogLevel(Debug), envLogger, envRegion, newEnv, newLogger)
import Control.Lens.Setter (set)

import LiftedGlacierRequests (GlacierEnv(..), GlacierSettings(..))
import AllowedPartSizes (PartSize)
import UploadSnapshot


data Config = Config {
  snapper_config_name :: String, -- Defaults to "root"
  region :: Region,
  glacier_vault_name :: Text,
  upload_part_size_MB :: PartSize,
  aws_account_id :: Text -- A 12-digit number, defaults to account of credentials. (-)
  --aws_account_id :: Maybe Int64 -- A 12-digit number, defaults to account of credentials. (-)
} deriving (Show, Generic, FromJSON)

configDefaults :: Value
configDefaults = object [
    "snapper_config_name" .= String "root",
    "aws_account_id"      .= String "-",
    "upload_part_size_MB" .= Number 32
  ]

data Command = Provision | ListUploads

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
  <*> optional (subparser commandOpt)

commandOpt :: Mod CommandFields Command
commandOpt =
     command "provision" (info (pure Provision) (progDesc "Create the Glacier vault and SimpleDB domain (table)"))
  <> command "uploads" (info (pure ListUploads) (progDesc "List the snapshots uploaded to Glacier"))

setupConfig :: FilePath -> IO (String, GlacierEnv)
setupConfig configFilePath = do
  Config{..} <- loadYamlSettings [configFilePath] [configDefaults] useEnv
  let glacierConfig = GlacierSettings aws_account_id glacier_vault_name upload_part_size_MB 
  lgr <- newLogger Debug stdout
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

