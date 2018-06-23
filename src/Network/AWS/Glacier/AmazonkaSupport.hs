{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module AmazonkaSupport () where

import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.AWS (AWST')
import Control.Monad.Primitive (PrimMonad, PrimState, primitive)

import Control.Applicative ((<|>))
--import Network.AWS.Data.Text (ToText(..), FromText(..))
import Network.AWS.Data.Text
import Data.Attoparsec.Text
import qualified Data.Text.Lazy as LText
import Data.Text.Lazy.Builder (Builder)
import qualified Data.Text.Lazy.Builder            as Build
import qualified Data.Text.Lazy.Builder.Int        as Build
import Data.Word

import Util (lowerMaybe)

-- zomgwtfbbb is even happening
-- I just copied the ReaderT instance since AWST' is just a newtype wrapper for ReaderT
instance PrimMonad m => PrimMonad (AWST' r m) where
  type PrimState (AWST' r m) = PrimState m
  primitive = lift . primitive

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
