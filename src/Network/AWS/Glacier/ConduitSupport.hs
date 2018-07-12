{-# LANGUAGE BangPatterns, FlexibleContexts #-}
module ConduitSupport where

import Data.ByteString (ByteString)
import Data.Conduit (ConduitT, Void, runConduit, (.|), await, yield)
import Data.Conduit.Process (CreateProcess, ClosedStream(..), streamingProcess, waitForStreamingProcess)
import Data.Conduit.Async (buffer)
import System.Exit (ExitCode)

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Primitive (PrimMonad)

import Data.Vector.Storable (Vector, unsafeToForeignPtr)
import Data.Word8 (Word8)
import Data.ByteString.Internal (ByteString (PS))

import qualified Data.Conduit.Combinators as C
--import ClassyPrelude (fromByteVector)

-- From ClassyPrelude
-- | Convert a storable 'Vector' into a 'ByteString'.
fromByteVector :: Vector Word8 -> ByteString
fromByteVector v =
    PS fptr offset idx
  where
    (fptr, offset, idx) = unsafeToForeignPtr v
{-# INLINE fromByteVector #-}

chunksOf :: PrimMonad m => Int -> ConduitT ByteString ByteString m ()
chunksOf size = 
     C.vectorBuilder size C.mapM_E
  .| C.map fromByteVector

zipWithIndexFrom :: (Enum e, Monad m) => e -> ConduitT a (e, a) m ()
zipWithIndexFrom i =
    loop i
  where
    --loop !i = do
    --  mx <- await
    --  case mx of
    --    Nothing -> pure ()
    --    Just x -> do
    --      yield (i, x)
    --      loop (i + 1)
    loop !i = await >>= maybe (pure ()) (\x -> do
                                                 yield (i, x)
                                                 loop (succ i)
                                        )

bufferedSourceCommandWithConsumer :: MonadUnliftIO m
  => CreateProcess
  -> ConduitT ByteString a m ()
  -> ConduitT a Void m r 
  -> m (ExitCode, r)
bufferedSourceCommandWithConsumer cmd sourceTransformer consumer = do
  (ClosedStream, (source, close), ClosedStream, cph) <- streamingProcess cmd
  res <- buffer 1 (source .| sourceTransformer) consumer
  close
  ec <- waitForStreamingProcess cph
  return (ec, res)
