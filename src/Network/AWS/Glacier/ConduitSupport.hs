{-# LANGUAGE BangPatterns #-}
module ConduitSupport where

import Data.ByteString (ByteString)
import Data.Conduit (ConduitT, (.|), await, yield)
import Control.Monad.Trans.Class (lift)
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

-- Generalize to Enum?
zipWithIndex :: Monad m => ConduitT a (Int, a) m ()
zipWithIndex =
    loop 0
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
                            
