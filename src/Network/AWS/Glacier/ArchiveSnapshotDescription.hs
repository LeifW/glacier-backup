{-# LANGUAGE DeriveGeneric #-}
module ArchiveSnapshotDescription (ArchiveSnapshotDescription(..), descriptionFromCsv) where

import Data.Text (Text, intercalate, singleton, splitOn)
import GHC.Generics (Generic)
import Network.AWS.Data.Text (ToText(toText), fromText)
import Snapper (SnapshotRef)

data ArchiveSnapshotDescription = ArchiveSnapshotDescription {
  _current :: SnapshotRef,
  _previous :: Maybe SnapshotRef
} deriving (Show, Generic)

csvSepChar :: Char
csvSepChar = ','

csvSep :: Text
csvSep = singleton csvSepChar

instance ToText ArchiveSnapshotDescription where
  toText (ArchiveSnapshotDescription i prev) = intercalate csvSep [toText i, toText prev]

-- The parsers don't seem to compose - they each expect end of input.
descriptionFromCsv :: Text -> Either String ArchiveSnapshotDescription
descriptionFromCsv t = case splitOn csvSep t of
  [i, prev] -> ArchiveSnapshotDescription <$> fromText i <*> fromText  prev
  other -> Left $ "Expected two comma-seperated values; got: " ++ show other
