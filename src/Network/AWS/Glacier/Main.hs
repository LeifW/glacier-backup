{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass, TypeApplications, RecordWildCards #-}
module Main where

import System.Environment (getArgs)
import System.IO (stdout)
import Data.Text (Text)

import GHC.Generics (Generic)
import Data.Yaml (Value, FromJSON, toJSON, object)
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

confDef :: Value
confDef = object [
    ("snapper_config_name", "root"),
    ("aws_account_id",      "-"),
    ("upload_part_size_MB", toJSON @Int 32)
  ]

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["provision", "aws"] -> setupConfig "glacier-backup.yml" >>= runReaderT provisionAWS . snd
    --["provision", "aws"] -> setupConfig "/etc/glacier-backup" >>= flip runReaderResource provisionAWS . snd
    [fileName] -> main' fileName
    [] -> main' "glacier-backup.yml"
    --[] -> main' "/etc/glacier-backup.yml"
    _ -> error "glacier-backup: Pass in a filename or don't"

setupConfig :: FilePath -> IO (String, GlacierEnv)
setupConfig configFilePath = do
  Config{..} <- loadYamlSettings [configFilePath] [confDef] useEnv
  print snapper_config_name
  let glacierConfig = GlacierSettings aws_account_id glacier_vault_name upload_part_size_MB 
  lgr <- newLogger Debug stdout
  awsEnv <- set envRegion region . set envLogger lgr <$> newEnv Discover
  pure (snapper_config_name, GlacierEnv awsEnv glacierConfig)

main' :: FilePath -> IO ()
main' configFilePath = do
  (snapper_config_name, glacierEnv) <- setupConfig configFilePath
  runReaderT (uploadBackup snapper_config_name) glacierEnv 
