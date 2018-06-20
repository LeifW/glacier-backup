module SnapperReaderT  where

import DBus.Client (Client, connectSystem, disconnect)
import DBus (MethodError)
import Control.Exception (bracket, throwIO, Exception)

import System.FilePath ((</>))

import Control.Monad.Trans.Except
import Control.Monad.Reader
import Snapper (Snapshot(Snapshot), SnapperConfig(SnapperConfig), lastSnapshot, snapshotDirFromConfig, runSystemDBus)
import qualified Snapper

type SnapperEnv = (String, Client)

type EitherSnapperReader = ExceptT MethodError (ReaderT SnapperEnv IO)

liftEitherSnapperFunc :: (String -> Client -> m (Either e a)) -> ExceptT e (ReaderT (String, Client) m) a
liftEitherSnapperFunc = ExceptT . ReaderT . uncurry
{-
class HasSnapperEnv a where
  snapperEnv :: a -> SnapperEnv

instance HasSnapperEnv SnapperEnv where
  snapperEnv = id
-}

listSnapshots :: EitherSnapperReader [Snapshot]
listSnapshots = liftEitherSnapperFunc Snapper.listSnapshots

getConfig :: EitherSnapperReader SnapperConfig
getConfig = liftEitherSnapperFunc Snapper.getConfig 

-- Monad instance for (r ->) aka Reader
-- Seems like a lot of trouble to go through (talking to Snapper over DBus)
-- to get the equivalent of "ls -ltr" in the snapshots dir
getLastSnapshot' :: EitherSnapperReader (Maybe FilePath)
getLastSnapshot' = do
  config <- getConfig
  snapshots <- listSnapshots
  pure $ snapshotDirFromConfig config <$> lastSnapshot snapshots

getLastSnapshot :: String -> IO (Maybe FilePath)
getLastSnapshot config =
    runSystemDBus (curry (runReaderT (runExceptT getLastSnapshot')) config)
    >>= either throwIO pure
