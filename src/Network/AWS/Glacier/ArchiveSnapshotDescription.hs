{-# LANGUAGE DeriveGeneric, DeriveDataTypeable, FlexibleInstances #-}
module ArchiveSnapshotDescription (ArchiveSnapshotDescription(..), descriptionFromCsv) where

import Data.Text (Text, intercalate, singleton, splitOn)
import Data.Data (Data)
import GHC.Generics (Generic)
import Network.AWS.Data.Time  (ISO8601)
import Network.AWS.Data.Text (ToText(toText), fromText)
import AmazonkaSupport ()
import Snapper (SnapshotRef)

data ArchiveSnapshotDescription = ArchiveSnapshotDescription {
  _current :: SnapshotRef,
  _previous :: Maybe SnapshotRef
} deriving (Show, Data, Generic)

csvSepChar :: Char
csvSepChar = ','

csvSep :: Text
csvSep = singleton csvSepChar

instance ToText ArchiveSnapshotDescription where
  toText (ArchiveSnapshotDescription i prev) = intercalate csvSep [toText i, toText prev]

-- The parsers don't seem to compose - they each expect end of input.
descriptionFromCsv :: Text -> Either String ArchiveSnapshotDescription
descriptionFromCsv t = case splitOn csvSep t of
  [i, ts, prev] -> ArchiveSnapshotDescription <$> fromText i <*> fromText  prev
  other -> Left $ "Expected two comma-seperated values; got: " ++ show other
