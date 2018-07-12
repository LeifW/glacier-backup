{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module AmazonkaSupport (runReaderResource) where

import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.AWS (AWST')
import Control.Monad.Primitive (PrimMonad, PrimState, primitive)

import Control.Monad.Reader (ReaderT, runReaderT)
import Control.Monad.Trans.Resource (ResourceT, MonadUnliftIO, runResourceT)

import Control.Applicative ((<|>))
import Network.AWS.Data.Text (Text, ToText(..), FromText(..))
import Data.Attoparsec.Text (decimal, endOfInput)
import qualified Data.Text.Lazy as LText
import Data.Text.Lazy.Builder (Builder)
import qualified Data.Text.Lazy.Builder            as Build
import qualified Data.Text.Lazy.Builder.Int        as Build
import Data.Word

import Util (lowerMaybe)

runReaderResource :: MonadUnliftIO m => r -> ReaderT r (ResourceT m) a -> m a
runReaderResource r m = runResourceT $ runReaderT m r

-- zomgwtfbbb is even happening
-- I just copied the ReaderT instance since AWST' is just a newtype wrapper for ReaderT
{- 
instance PrimMonad m => PrimMonad (AWST' r m) where
  type PrimState (AWST' r m) = PrimState m
  primitive = lift . primitive
-}

shortText :: Builder -> Text
shortText = LText.toStrict . Build.toLazyTextWith 32

instance ToText Word where toText = shortText . Build.decimal
instance ToText Word8 where toText = shortText . Build.decimal
instance ToText Word16 where toText = shortText . Build.decimal
instance ToText Word32 where toText = shortText . Build.decimal
instance ToText Word64 where toText = shortText . Build.decimal

instance FromText Word where parser = decimal <* endOfInput
instance FromText Word8 where parser = decimal <* endOfInput
instance FromText Word16 where parser = decimal <* endOfInput
instance FromText Word32 where parser = decimal <* endOfInput
instance FromText Word64 where parser = decimal <* endOfInput
  
instance ToText a => ToText (Maybe a) where
  toText = lowerMaybe . fmap toText

instance (FromText a) => FromText (Maybe a) where
  parser = (const Nothing <$> endOfInput) <|> (Just <$> parser)
