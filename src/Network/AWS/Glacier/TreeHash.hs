{-# LANGUAGE PackageImports, BangPatterns #-}
module TreeHash (treeHashList, treeHashByChunksOf) where
import Data.ByteString (ByteString)
import qualified Data.ByteString            as BS
import Data.ByteArray (ByteArrayAccess)

import "cryptonite" Crypto.Hash (HashAlgorithm, Digest, hash, hashInit, hashUpdates, hashFinalize)

import Data.Tree.Binary.Leafy (foldTree, fromList)
import Data.List (unfoldr)
import Util (justWhen)

-- Trusting the Data.ByteString comment: "As for all splitting functions in this library, this function does not copy the substrings, it just constructs new ByteStrings that are slices of the original."
-- I'd generalize this to ByteArrayAccess, but it looks like its corresponding splitAt does copies; doesn't allow for more efficient impl
byteStringChunksOf :: Int -> ByteString -> [ByteString]
byteStringChunksOf !i = unfoldr (justWhen (not . BS.null) (BS.splitAt i))

treeHashByChunksOf :: HashAlgorithm a => Int -> ByteString -> Digest a
treeHashByChunksOf !chunkSize = treeHashList . map hash . byteStringChunksOf chunkSize

treeHashList :: HashAlgorithm a => [Digest a] -> Digest a
treeHashList = foldTree id hashPair . fromList

hashPair :: (ByteArrayAccess ba, HashAlgorithm a) => ba -> ba -> Digest a
hashPair !a !b = hashFinalize $ hashUpdates hashInit [a, b]
