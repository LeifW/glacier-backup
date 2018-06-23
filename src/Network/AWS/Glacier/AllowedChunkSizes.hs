{-# LANGUAGE PackageImports, OverloadedStrings, FlexibleContexts #-}
module AllowedChunkSizes (ChunkSizeMB, validChunkSize, megabytesToBytes)  where

--import Data.Aeson (FromJSON(..))
import Data.Aeson.Types

-- Powers of two from 1 MB to 4 GB.
allowedChunkSizes :: [Int]
allowedChunkSizes = map (2 ^) [0..12]

newtype ChunkSizeMB = ChunkSizeMB Int deriving (Show)

validChunkSize :: Int -> Either String ChunkSizeMB
validChunkSize i = if i `elem` allowedChunkSizes
              then Right (ChunkSizeMB i)
              else Left ("chunk size must be a power of two. Expected a member of " <> show allowedChunkSizes <> ", got " <> show i)

instance FromJSON ChunkSizeMB where
  parseJSON v = do
    i <- parseJSON v
    either fail pure $ validChunkSize i

{-
chunkSizeToBytes :: MonadThrow m => Int -> m Int
chunkSizeToBytes chunkSizeMB = do
  unless (chunkSizeMB `elem` allowedChunkSizes) $ throwM InvalidChunkSizeRequested
  pure $ megabytesToBytes chunkSizeMB
-}

-- Can't use Int64 for chunk size, because we're limited by the signature of
-- Conduit's vectorBuilder for the chunking, which takes an Int.
-- Theoretically that limits us to a chunk size of 2GB on 32-bit machines
-- which seems like a far-fetched scenario for a number of reasons (2GB upload chunk in a 4GB address space?)
megabytesToBytes :: ChunkSizeMB -> Int
megabytesToBytes (ChunkSizeMB i) = i * 1024 * 1024
