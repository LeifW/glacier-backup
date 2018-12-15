module AllowedPartSizes (PartSize, getNumBytes)  where

import Control.Monad.Fail (MonadFail)
import Data.Aeson.Types (FromJSON(..), ToJSON(..))
import Network.AWS.Data.Text (ToText(..), FromText(..))

-- Powers of two from 1 MB to 4 GB.
allowedPartSizes :: [Int]
allowedPartSizes = map (2^) [0..12 :: Int]

newtype PartSize = PartSize Int deriving (Show)

-- Can't use Int64 for chunk size, because we're limited by the signature of
-- Conduit's vectorBuilder for the chunking, which takes an Int.
-- Theoretically that limits us to a chunk size of 2GB on 32-bit machines
-- which seems like a far-fetched scenario for a number of reasons (2GB upload chunk in a 4GB address space?)

-- Convert to number of bytes
getNumBytes :: PartSize -> Int
getNumBytes (PartSize i) = i * 1024 * 1024

validPartSize :: MonadFail m => Int -> m PartSize
validPartSize i = if i `elem` allowedPartSizes
              then pure $ PartSize i
              else fail $ "part size must be a power of two. Expected a member of " <> show allowedPartSizes <> ", got " <> show i

instance ToJSON PartSize where
  toJSON (PartSize i) = toJSON i

instance FromJSON PartSize where
  parseJSON v = parseJSON v >>= validPartSize

instance ToText PartSize where
  toText (PartSize i) = toText i

instance FromText PartSize where
  parser = parser >>= validPartSize
