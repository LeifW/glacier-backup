{-# LANGUAGE TypeFamilies #-}
module AmazonkaSupport where

import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.AWS (AWST')
import Control.Monad.Primitive (PrimMonad, PrimState, primitive)

-- zomgwtfbbb is even happening
-- I just copied the ReaderT instance since AWST' is just a newtype wrapper for ReaderT
instance PrimMonad m => PrimMonad (AWST' r m) where
  type PrimState (AWST' r m) = PrimState m
  primitive = lift . primitive
