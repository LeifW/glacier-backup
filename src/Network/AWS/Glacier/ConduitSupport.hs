{-# LANGUAGE BangPatterns #-}
module ConduitSupport where

import Data.ByteString (ByteString)
import Data.Conduit (ConduitT, Void, (.|), runConduit, await, yield)
import Data.Conduit.Process (CreateProcess, ClosedStream(..), Inherited(..), streamingProcess, waitForStreamingProcess)
import Data.Conduit.Async (buffer)
import System.Exit (ExitCode)

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Control.Monad.Primitive (PrimMonad)

import Data.Vector.Storable (Vector, unsafeToForeignPtr)
import Data.Word8 (Word8)
import Data.ByteString.Internal (ByteString (PS))

import qualified Data.Conduit.Combinators as C

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

zipWithIndexFrom :: (Enum i, Monad m) => i -> ConduitT a (i, a) m ()
zipWithIndexFrom !i = await >>= maybe (pure ()) (\x -> do
                                                         yield (i, x)
                                                         zipWithIndexFrom (succ i)
                                                )

bufferedSourceProcessWithConsumer :: MonadUnliftIO m
  => CreateProcess
  -> ConduitT ByteString a m ()
  -> ConduitT a Void m r 
  -> m (ExitCode, r)
bufferedSourceProcessWithConsumer cmd sourceTransformer consumer = do
  (ClosedStream, (source, close), Inherited, cph) <- streamingProcess cmd
  res <- buffer 1 (source .| sourceTransformer) consumer
  close
  ec <- waitForStreamingProcess cph
  pure (ec, res)

sinkProcessWithProducer :: MonadIO m
  => ConduitT () ByteString m ()
  -> CreateProcess
  -> m ExitCode
sinkProcessWithProducer producer cmd = do
  ((sink, close), Inherited, Inherited, cph) <- streamingProcess cmd
  runConduit $ producer .| sink
  close
  waitForStreamingProcess cph
