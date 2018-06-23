{-# LANGUAGE DeriveGeneric, FlexibleInstances #-}
module ArchiveDescription where

import Data.Text --(intercalate, singleton)
import GHC.Generics (Generic)
import Control.Monad (void)
import Network.AWS.Data.Time  (ISO8601)
import Network.AWS.Data.Text (FromText(..), ToText(..), fromText)
import Data.Attoparsec.Text --(Parser, char)
import AmazonkaSupport ()
import Snapper (SnapshotRef)

data ArchiveDescription = ArchiveDescription {
  _current :: SnapshotRef,
  _timestamp :: ISO8601,
  _previous :: Maybe SnapshotRef
} deriving (Show, Generic)

csvSepChar :: Char
csvSepChar = ','

csvSep = singleton csvSepChar

instance ToText ArchiveDescription where
  toText (ArchiveDescription i ts prev) = intercalate csvSep [toText i, toText ts, toText prev]

-- The parsers don't seem to compose - they each expect end of input.
descriptionFromCsv :: Text -> Either String ArchiveDescription
descriptionFromCsv t = case splitOn csvSep t of
  [i, ts, prev] -> ArchiveDescription <$> fromText i <*> fromText ts <*> fromText  prev
