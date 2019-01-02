{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE PackageImports, TypeApplications, DeriveAnyClass, RankNTypes #-}
module AmazonkaSupport (runReaderResource, digestFromHexThrow, getLogger, textFormat, bytesFormat, fromTextThrow, ParseException(..), ensureResponseGetterIs, ensure200Response, ensure201Response, ensure202Response, ensure204Response) where

import Data.Typeable (Typeable)

import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, runReaderT)
import Control.Monad.Trans.Resource (ResourceT, MonadUnliftIO, runResourceT)

import Control.Applicative ((<|>))
import Control.Monad ((<=<))
import Control.Monad.Catch 
import Control.Monad.Trans.AWS (HasEnv, Logger, LogLevel, envLogger)
import Network.AWS.Data.Text (Text, ToText(..), FromText(..), fromText)
import Data.Attoparsec.Text (decimal, endOfInput)
import qualified Data.Text.Lazy as LText
import Data.Text.Lazy.Encoding (encodeUtf8Builder)
import Data.Text.Lazy.Builder (Builder)
import qualified Data.Text.Lazy.Builder            as Build
import qualified Data.Text.Lazy.Builder.Int        as Build
import Formatting (Format, later, bytes, fixed, (%))
import Data.Word

import Control.Lens (Lens', view)

import Data.Text.Encoding (encodeUtf8)
import Data.ByteString (ByteString)
import Data.ByteArray.Encoding (Base(Base16), convertFromBase)
import "cryptonite" Crypto.Hash (Digest, HashAlgorithm, digestFromByteString)

import Util (lowerMaybe, throwLeftAs, maybeToEither, ensureIs)

runReaderResource :: MonadUnliftIO m => r -> ReaderT r (ResourceT m) a -> m a
runReaderResource r m = runResourceT $ runReaderT m r

liftedTextLogger :: MonadIO m => Logger -> LogLevel -> LText.Text -> m ()
liftedTextLogger lg level = liftIO . lg level . encodeUtf8Builder

getLogger  :: (MonadIO mio, MonadReader s m, HasEnv s)  => m (LogLevel -> LText.Text -> mio ())
getLogger = liftedTextLogger <$> view envLogger

textFormat :: ToText a => Format r (a -> r)
textFormat = later (Build.fromText . toText)

bytesFormat :: Integral a => Format r (a -> r)
bytesFormat = bytes @Double (fixed 3 % " ")

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
  parser = (Nothing <$ endOfInput) <|> (Just <$> parser)

data ParseException = ParseException String | DecodeChecksumException String deriving (Show, Exception)

fromTextThrow :: (MonadThrow m, FromText a) => Text -> m a
fromTextThrow = throwLeftAs ParseException . fromText

digestFromHexThrow :: (MonadThrow m, HashAlgorithm a) => Text -> m (Digest a)
digestFromHexThrow = throwLeftAs DecodeChecksumException . digestFromHex

digestFromHex :: HashAlgorithm a => Text -> Either String (Digest a)
digestFromHex = maybeToEither "Digest is wrong size for algorithm specified" . digestFromByteString <=<
                convertFromBase @ByteString @ByteString Base16 . encodeUtf8

ensureResponseGetterIs :: (MonadThrow m, Eq a, Show a, Typeable a) => String -> a -> Lens' r a -> r -> m ()
ensureResponseGetterIs label expected getter = ensureIs label expected . view getter

ensureResponseCodeIs :: MonadThrow m => Int -> Lens' a Int -> a -> m ()
ensureResponseCodeIs = ensureResponseGetterIs "response code"

ensure200Response, ensure201Response, ensure202Response, ensure204Response :: MonadThrow m => Lens' a Int -> a -> m ()
ensure200Response = ensureResponseCodeIs 200
ensure201Response = ensureResponseCodeIs 201
ensure202Response = ensureResponseCodeIs 202
ensure204Response = ensureResponseCodeIs 204

