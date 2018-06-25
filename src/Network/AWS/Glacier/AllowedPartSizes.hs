module AllowedPartSizes (PartSize, getNumBytes, validPartSize)  where

import Data.Aeson.Types (FromJSON(..), ToJSON(..))
import Network.AWS.Data.Text (ToText(..), FromText(..))

-- Powers of two from 1 MB to 4 GB.
allowedPartSizes :: [Int]
allowedPartSizes = map (2^) [0..12 :: Int]

-- Can't use Int64 for chunk size, because we're limited by the signature of
-- Conduit's vectorBuilder for the chunking, which takes an Int.
-- Theoretically that limits us to a chunk size of 2GB on 32-bit machines
-- which seems like a far-fetched scenario for a number of reasons (2GB upload chunk in a 4GB address space?)
newtype PartSize = PartSize { getNumBytes :: Int } deriving (Show)

validPartSize :: Int -> Either String PartSize
validPartSize i = if i `elem` allowedPartSizes
              then Right (PartSize $ i * 1024 * 1024)  -- Convert to number of bytes
              else Left ("part size must be a power of two. Expected a member of " <> show allowedPartSizes <> ", got " <> show i)

eitherToMonadFail :: Monad m => Either String a -> m a
eitherToMonadFail = either fail pure

instance FromJSON PartSize where
  parseJSON v = do
    i <- parseJSON v
    eitherToMonadFail $ validPartSize i

instance ToJSON PartSize where
  toJSON (PartSize i) = toJSON $ i `div` (1024 * 1024)

instance ToText PartSize where
  toText (PartSize i) = toText i

instance FromText PartSize where
  parser = do
    i <- parser
    eitherToMonadFail $ validPartSize i
