{-# LANGUAGE TemplateHaskell, FlexibleContexts, FlexibleInstances, PackageImports, OverloadedStrings #-}
module GlacierReaderT (SUM.NumBytes, GlacierSettings(..), GlacierEnv(..),  HasGlacierSettings(..), SUM.chunkSizeToBytes, initiate, uploadByChunks, complete, upload, runReaderResource) where

import Data.Text (Text)
import Control.Lens
--import Control.Monad.Reader.Class (asks, runReaderT)
import Control.Monad.Reader
--import Data.Has
import Control.Monad.Trans.Resource
import Control.Monad.Catch

import Control.Monad.Primitive (PrimMonad)
import "cryptonite" Crypto.Hash (Digest, SHA256)
import Data.Conduit (ConduitT, Void)
import Data.ByteString (ByteString)
import Network.AWS.Glacier (ArchiveCreationOutput)

import StreamingUploadMultipart (NumBytes)
import qualified StreamingUploadMultipart as SUM

--import Control.Monad.Trans.AWS (runAWST, runResourceT, AWST, AWST', AWSConstraint, send, HasEnv, Env, LogLevel(..), newLogger, envLogger, newEnv)
import Control.Monad.Trans.AWS

data GlacierSettings = GlacierSettings {
  _accountId :: Text,
  _vaultName :: Text
} deriving Show

--makeLenses ''GlacierSettings

--instance Has Env r => HasEnv r where
--  environment = hasLens
class HasGlacierSettings r where
  glacierSettingsL :: Lens' r GlacierSettings

instance HasGlacierSettings GlacierSettings where
  glacierSettingsL = id

data GlacierEnv = GlacierEnv {
  _awsEnv :: Env,
  _glacierSettings :: GlacierSettings
}

makeLenses ''GlacierEnv

instance HasEnv GlacierEnv  where
  environment = awsEnv --  lens _awsEnv (\x y -> x { _awsEnv = y })

--yoo :: Has Env r => r -> r
--yoo gah = gah
-- This instance isn't necessary
instance HasGlacierSettings GlacierEnv where
  glacierSettingsL = glacierSettings

-- This class isn't necessary
--class (HasEnv r, HasGlacierSettings r) => HasGlacierEnv r where
--  glacierEnvironmentL :: Lens' r GlacierEnv

--instance HasGlacierEnv GlacierEnv where
--  glacierEnvironmentL = id

initiate :: (AWSConstraint r m, HasGlacierSettings r)
         => Maybe Text
         -> Int
         -> m Text
initiate archiveDescription chunkSize = do
  --GlacierSettings accountId vaultName <- asks getter
  --GlacierSettings accountId vaultName <- asks (view glacierSettingsL)
  GlacierSettings accountId vaultName <- view glacierSettingsL
  SUM.initiate accountId vaultName archiveDescription chunkSize

uploadByChunks :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m) => Int -> Text -> ConduitT ByteString Void m (NumBytes, Digest SHA256)
uploadByChunks chunkSizeBytes uploadId = do
  --GlacierSettings accountId vaultName <- asks getter
  GlacierSettings accountId vaultName <- view glacierSettingsL
  SUM.uploadByChunks chunkSizeBytes accountId  vaultName uploadId

complete :: (AWSConstraint r m, HasGlacierSettings r)
       => Text 
       -> Digest SHA256
       -> NumBytes
       -> m ArchiveCreationOutput
complete uploadId treeHashChecksum totalArchiveSize = do
  --GlacierSettings accountId vaultName <- asks getter
  GlacierSettings accountId vaultName <- view glacierSettingsL
  SUM.complete accountId vaultName uploadId treeHashChecksum totalArchiveSize 

upload :: (AWSConstraint r m, HasGlacierSettings r, PrimMonad m)
       => Maybe Text 
       -> Int -- ^ Chunk Size in MB
       -> ConduitT ByteString Void m ArchiveCreationOutput
upload archiveDescription chunkSizeMB = do
  chunkSizeBytes <- SUM.chunkSizeToBytes chunkSizeMB
  uploadId <- lift $ initiate archiveDescription chunkSizeBytes
  (totalArchiveSize, treeHashChecksum) <- uploadByChunks chunkSizeBytes uploadId
  lift $ complete uploadId treeHashChecksum totalArchiveSize
  
  --unless (chunkSizeMB `elem` allowedChunkSizes) $ error ("upload: Chunk size must be a power of 2, e.g. one of: " ++ show allowedChunkSizes)

runReaderResource :: MonadUnliftIO m => r -> ReaderT r (ResourceT m) a -> m a
runReaderResource r m = runResourceT $ runReaderT m r
