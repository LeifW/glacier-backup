{-# LANGUAGE FlexibleInstances, PackageImports #-}
module TreeHash (treeHashList, treeHashByChunksOf, toHex) where
-- from https://stackoverflow.com/questions/25428065/rechunk-a-conduit-into-larger-chunks-using-combinators
import Data.ByteString (ByteString)
import qualified Data.ByteString            as BS
import Data.ByteArray (ByteArrayAccess)
import Data.ByteArray.Encoding (convertToBase, Base(Base16))

import "cryptonite" Crypto.Hash

import Data.List (unfoldr)
import Data.Tree.Binary.Leafy

boolToMaybe :: Bool -> a -> Maybe a
boolToMaybe True a = Just a
boolToMaybe False _ = Nothing

justWhen :: (a -> Bool) -> (a -> b) -> (a -> Maybe b)
justWhen p f a = boolToMaybe (p a) (f a)

-- Trusting the Data.ByteString comment: "As for all splitting functions in this library, this function does not copy the substrings, it just constructs new ByteStrings that are slices of the original."
-- I'd generalize this to ByteArrayAccess, but it looks like its corresponding splitAt does copies; doesn't allow for more efficient impl
byteStringChunksOf :: Int -> ByteString -> [ByteString]
byteStringChunksOf i = unfoldr (justWhen (not . BS.null) (BS.splitAt i))

--treeHash :: ByteString -> Digest SHA256
--treeHash = treeHash' (1024 * 1024) -- 1 MB chunks == 1024k where 1k = 1024 bytes

treeHashByChunksOf :: HashAlgorithm a => Int -> ByteString -> Digest a
treeHashByChunksOf chunkSize = treeHashList . map hash . byteStringChunksOf chunkSize

-- The bytestring -> bytestring version seems potentially too confusing for keeping track of what actually changed. E.g. bytes turn into a hash code. Turning bytes into bytes doesn't really clarify.
--treeHashList :: [ByteString] -> ByteString
--treeHashList :: (ByteArrayAccess ba, HashAlgorithm a) => [ba] -> Digest a
treeHashList :: HashAlgorithm a => [Digest a] -> Digest a
treeHashList =  foldTree id hashPair . fromList
--treeHashList =  foldTree hash hashPair . fromList

hashPair :: (ByteArrayAccess ba, HashAlgorithm a) => ba -> ba  -> Digest a
hashPair a b = hashFinalize $ hashUpdates hashInit [a, b]

toHex :: ByteArrayAccess a => a -> ByteString
toHex = convertToBase Base16
