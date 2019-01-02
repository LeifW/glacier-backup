{-# LANGUAGE DeriveAnyClass, TupleSections #-}
module Restore where

import Data.Text (Text)

import Network.AWS.Data.Body (RsBody(..), fuseStream)
import Network.AWS.Glacier.Types
import Network.AWS.Data.Text (toText)
import Network.AWS.Glacier.GetJobOutput (gjorsBody)
--import Control.Lens.Setter
import Control.Lens.Getter (view)

--import Data.Conduit ((.|))
import Data.Conduit.Zlib (ungzip)

import SimpleDB (SnapshotUpload(..), uploadsList)
import Snapper (SnapshotRef)
import LiftedGlacierRequests
--import MultipartGlacierUpload (GlacierUpload(GlacierUpload))
import ArchiveSnapshotDescription

import ConduitSupport
import System.Process (CreateProcess, proc, callProcess)
import System.Exit (ExitCode(ExitSuccess))
import System.FilePath ((</>))

import Data.Map.Strict (Map, (!?))
import qualified Data.Map.Strict as Map
--import Data.List (unfoldr, partition)
import Data.Maybe (fromJust)
import Data.Either (partitionEithers)
--import Control.Monad.Loops (unfoldrM)
import Control.Monad.Catch (MonadThrow, Exception, throwM)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Resource (MonadResource(liftResourceT), runResourceT)
import Control.Monad.Fail (MonadFail)

import Util (throwNothingAs, maybeToEither, justIf, throwExitFailure)

pipeResponseToProcess :: (MonadResource m, MonadThrow m)
  => RsBody
  -> CreateProcess
  -> m ()
pipeResponseToProcess (RsBody body) cmd = do
  responseCode <- liftResourceT $ sinkProcessWithProducer body cmd
  throwExitFailure responseCode

btrfsRecieveCmd :: FilePath -> CreateProcess
btrfsRecieveCmd path = proc "sudo" ["btrfs", "receive", path]

sudo :: MonadIO m => [String] -> m ()
sudo = liftIO . callProcess "sudo"

mkDir :: MonadIO m => FilePath -> m ()
mkDir path = sudo ["mkdir", path]

createSubvolume :: MonadIO m => FilePath -> m ()
createSubvolume path = sudo ["btrfs", "subvolume", "create", path]

--bar = pipeResponseToProcess undefined (btrfsRecieveCmd "foo")

foo :: (GlacierConstraint r m) => m (Map SnapshotRef SnapshotUpload)
foo = snapshotUploadsById <$> uploadsList
{-
do
  uploads <- uploadsList
  pure $ Map.fromAscList [(_current $ _snapshotInfo s, s) | s <- uploads]
-}

newtype MissingSnapshotException = MissingSnapshotException SnapshotUpload deriving (Show, Exception)

data JobStatusError = NoJobForSnapshot SnapshotRef | JobNotCompleted SnapshotRef deriving Show

throwLeftAs :: (MonadThrow m, Exception e) => (a -> e) -> Either a b -> m b
throwLeftAs ex = either (throwM . ex) pure

snapshotUploadsById :: [SnapshotUpload] -> Map SnapshotRef SnapshotUpload
snapshotUploadsById = Map.fromAscList . map (\s -> (_current $ _snapshotInfo s, s))

requestArchiveRetrieval :: GlacierConstraint r m => Tier -> SnapshotUpload -> m JobId
requestArchiveRetrieval tier (SnapshotUpload (GlacierUpload archiveId _ _) snapshotDescription  _) =
  archiveRetrievalJob tier archiveId (Just $ toText snapshotDescription) -- pure $ toText snapshotDescription
  --fromJust . view ijrsJobId <$> archiveRetrievalJob tier archiveId (Just $ toText snapshotDescription) -- pure $ toText snapshotDescription
-- FIXME - remove the Maybe in the schema


initiateRetrieval :: GlacierConstraint r m => Tier -> m [JobId]
initiateRetrieval tier = do
--  uploads <- uploadsList
  chain <- linkedChain --followChainEx3 (snapshotUploadsById uploads) (last uploads)
  traverse (requestArchiveRetrieval tier) chain

checkJobStatus :: Map ArchiveId JobDescription -> SnapshotUpload -> Either JobStatusError (JobId, SnapshotRef)
checkJobStatus m (SnapshotUpload (GlacierUpload archiveId _ _) (ArchiveSnapshotDescription snapId _)  _) = do
  --ArchiveRetrievalJob jobId jobStatus _ _ <- maybeToEither (NoJobForSnapshot snapId) $ Map.lookup archiveId m
  job <- maybeToEither (NoJobForSnapshot snapId) $ Map.lookup archiveId m
  --if _jobStatus job == Succeeded then Right (_jobId job, snapId) else Left (JobNotCompleted snapId)
  maybeToEither (JobNotCompleted snapId) $ justIf (_jobStatus job == Succeeded) (_jobId job, snapId)

downloadSnapshots :: GlacierConstraint r m => FilePath -> [(JobId, SnapshotRef)] -> m ()
downloadSnapshots dir jobIds = do
  let snapshotsDir = dir </> ".snapshots"
  createSubvolume snapshotsDir
  mapM_ (downloadSnapshot snapshotsDir) jobIds

downloadSnapshot :: (GlacierConstraint r m) => FilePath -> (JobId, SnapshotRef) -> m ()
downloadSnapshot snapshotsDir (jobId, snapId) = do
  let snapDir = snapshotsDir </> show snapId
  mkDir snapDir
  runResourceT $ do
    jobOutput <- getJobOutput jobId
    pipeResponseToProcess (fuseStream (view gjorsBody jobOutput) ungzip) (btrfsRecieveCmd snapDir)
    --pipeGzippedResponseToProcess (view gjorsBody jobOutput) (btrfsRecieveCmd snapDir)

restore :: GlacierConstraint r m => FilePath -> m ()
restore dir = do
  jobs <- listJobs 
  -- let m = map (\j -> (fromJust $ view gjdArchiveId j, fromJust $ view gjdStatusCode j)) jobs
  let m  = Map.fromList $ map (\j -> (_archiveId $ _glacierUpload j, j)) jobs -- :: Map ArchiveId JobDescription  
  --let m  = Map.fromList $ map (\j -> either error (,j) $ fromText $ fromJust $ view gjdArchiveId j) jobs :: Map ArchiveId GlacierJobDescription  
  chain <- linkedChain
  -- The linkChain function puts the most recent entry at the head. To apply the snapshots to the filesystem, we need to start with the oldest and go in order from there.
  -- Hence the "reverse".
  let (pendings, jobIds) = partitionEithers $ map (checkJobStatus m) (reverse chain)
  if null pendings then downloadSnapshots dir jobIds else mapM_ (liftIO . print) pendings
  --let archiveIds = map (_archiveId . _glacierUploadResult) chain
  --let moo = all (\s -> m !? (_archiveId $ _glacierUploadResult s)) chain
  --let moo = map (\s -> maybe (Map.lookup (_archiveId $ _glacierUploadResult s) m ) chain
  --mapM_ (liftIO . print) jobs
  
--lookupPrevious :: Map SnapshotRef SnapshotUpload ->
--Map.lookup lookupMap <$> (_previous . _snapshotInfo) s) (head uploads)
linkedChain :: (GlacierConstraint r m) => m [SnapshotUpload]
linkedChain = uploadsList >>= linkChain
{-
  uploads <- uploadsList
  let lookupMap = snapshotUploadsById uploads
  --let (h:t) = uploads:w
  --let ([base], deltas) = partition (isNothing . _previous . _snapshotInfo) uploads
  --foldr
  --pure uploads
  --pure $ unfoldr (\s -> (\p -> (fromJust $ Map.lookup p lookupMap, s)) <$> (_previous . _snapshotInfo) s) (head uploads)
  --unfoldrM (\s -> (\p -> (maybe (throwM  $ Map.lookup p lookupMap, s)) <$> (_previous . _snapshotInfo) s) (head uploads)
  --unfoldrM (\s -> traverse (\p -> (s,) <$> (throwNothingAs (MissingSnapshotException s) $ Map.lookup p lookupMap)) $ (_previous . _snapshotInfo) s) (head uploads)
  --unfoldrM (lookupPrevious lookupMap) (last uploads)
  pure $ followChainEx lookupMap (last uploads)
-}

linkChain :: MonadThrow m => [SnapshotUpload] -> m [SnapshotUpload]
linkChain uploads = followChainEx3 (snapshotUploadsById uploads) (last uploads)


lookupPrevious :: MonadThrow m => Map SnapshotRef SnapshotUpload -> SnapshotUpload -> m (Maybe (SnapshotUpload, SnapshotUpload))
lookupPrevious lookupMap currentSnapshot = traverse (\p -> (currentSnapshot,) <$> throwNothingAs (MissingSnapshotException currentSnapshot) (Map.lookup p lookupMap)) $ (_previous . _snapshotInfo) currentSnapshot

--followChain :: MonadThrow m => Map SnapshotRef SnapshotUpload -> SnapshotUpload -> m [SnapshotUpload]
--followChain lookupMap currentSnapshot = (currentSnapshot :) <$> maybe (pure []) (\p -> followChain lookupMap <$> throwNothingAs (MissingSnapshotException currentSnapshot) (Map.lookup p  lookupMap)) ((_previous . _snapshotInfo) currentSnapshot)
--followChain lookupMap currentSnapshot = (currentSnapshot :) <$> traverse (followChain lookupMap . throwNothingAs (MissingSnapshotException currentSnapshot) . flip Map.lookup lookupMap) $ (_previous . _snapshotInfo) currentSnapshot

followChainEx :: Map SnapshotRef SnapshotUpload -> SnapshotUpload -> [SnapshotUpload]
followChainEx lookupMap currentSnapshot = currentSnapshot : maybe [] (\p -> followChainEx lookupMap $ fromJust $ Map.lookup p lookupMap) ((_previous . _snapshotInfo) currentSnapshot)

followChainEx2 :: MonadThrow m => Map SnapshotRef SnapshotUpload -> SnapshotUpload -> m [SnapshotUpload]
followChainEx2 lookupMap currentSnapshot = (currentSnapshot :) <$> maybe (pure []) (\p -> followChainEx2 lookupMap =<< throwNothingAs (MissingSnapshotException currentSnapshot) (Map.lookup p lookupMap)) ((_previous . _snapshotInfo) currentSnapshot)

followChainEx3 :: MonadThrow m => Map SnapshotRef SnapshotUpload -> SnapshotUpload -> m [SnapshotUpload]
followChainEx3 lookupMap currentSnapshot = do
  let maybePrevRef = _previous $ _snapshotInfo currentSnapshot
  rest <- maybe (pure []) (\prevRef -> do
    prev <- throwNothingAs (MissingSnapshotException currentSnapshot) $ Map.lookup prevRef lookupMap
    followChainEx3 lookupMap prev) maybePrevRef
  pure $ currentSnapshot : rest
--  (currentSnapshot :) <$> maybe (pure []) (\p -> followChainEx2 lookupMap =<< throwNothingAs (MissingSnapshotException currentSnapshot) (Map.lookup p lookupMap)) ((_previous . _snapshotInfo) currentSnapshot)
