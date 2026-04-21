{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  Native S3 client. Replaces shelling out to the `aws` CLI for
  s3:// listing and download.

  Auth modes:
    * Anonymous (noSign = True): no Authorization header. Used for public
      buckets such as overturemaps-us-west-2.
    * Signed (noSign = False): SigV4 from AWS_ACCESS_KEY_ID +
      AWS_SECRET_ACCESS_KEY env vars only. NO support for
      ~/.aws/credentials, IAM roles, SSO, or instance metadata.
      AWS_SESSION_TOKEN is honored if set (added as x-amz-security-token).
      Region defaults to us-east-1; override with AWS_REGION.

  Returns (ExitCode, Text, Text) so call sites mirror the curl/aws shell
  pattern used elsewhere (compare Tv.Ftp.Client).
-}
module Tv.S3.Client where

import Control.Exception (SomeException, try)
import qualified Crypto.Hash as H
import qualified Crypto.MAC.HMAC as HMAC
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as LBS
import Data.List (sortBy)
import Data.Maybe (fromMaybe)
import Data.Ord (comparing)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time.Clock (UTCTime, getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Network.HTTP.Client
  ( Request, RequestBody (RequestBodyBS), Response
  , httpLbs, method, parseRequest, requestBody, requestHeaders
  , responseBody, responseStatus )
import Data.CaseInsensitive (mk)
import Network.HTTP.Types.Header (HeaderName)
import Network.HTTP.Types.Status (statusCode)
import qualified Network.HTTP.Types.URI as URI
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))

import qualified Tv.Log as Log
import qualified Tv.Source.Core as Core

-- | List bucket prefix. Builds an XML→JSON conversion so the downstream
-- DuckDB read_json (in Tv.Source.S3.listSqlTmpl) keeps working.
-- Output schema: {"Contents": [{Key,Size,LastModified}], "CommonPrefixes": [{Prefix}]}
listS3 :: Bool -> Text -> Text -> IO (ExitCode, Text, Text)
listS3 noSign bucket prefix = do
  let qs = "list-type=2&prefix=" <> urlEncQS prefix <> "&delimiter=%2F"
      host_ = bucket <> ".s3.amazonaws.com"
      url = "https://" <> host_ <> "/?" <> qs
      -- Canonical query string for SigV4: keys sorted, values RFC3986-encoded.
      cQuery = "delimiter=%2F&list-type=2&prefix=" <> urlEncQS prefix
  Log.write "src" ("s3 list: " <> url)
  r <- runReq noSign "GET" host_ "/" cQuery "" url
  case r of
    Left err -> pure (ExitFailure 1, "", err)
    Right body ->
      let xml = TE.decodeUtf8 (LBS.toStrict body)
          json = xmlToJson xml
      in pure (ExitSuccess, json, "")

-- | Download an object key to a local file path.
downloadS3 :: Bool -> Text -> Text -> FilePath -> IO (ExitCode, Text, Text)
downloadS3 noSign bucket key dest = do
  let host_ = bucket <> ".s3.amazonaws.com"
      -- Path component: each segment percent-encoded but '/' preserved.
      cPath = "/" <> T.intercalate "/" (map urlEncPath (T.splitOn "/" key))
      url = "https://" <> host_ <> cPath
  Log.write "src" ("s3 get: " <> url)
  r <- runReq noSign "GET" host_ cPath "" "" url
  case r of
    Left err -> pure (ExitFailure 1, "", err)
    Right body -> do
      w <- try (LBS.writeFile dest body) :: IO (Either SomeException ())
      case w of
        Left e -> pure (ExitFailure 1, "", T.pack (show e))
        Right _ -> pure (ExitSuccess, "", "")

-- ## HTTP request execution

-- | Build the request, sign it (if required), and execute. Returns the
-- body bytes on 2xx, otherwise an error message in Left.
runReq
  :: Bool       -- noSign
  -> BS.ByteString  -- HTTP method (GET)
  -> Text       -- canonical host
  -> Text       -- canonical path (already encoded)
  -> Text       -- canonical query string (already encoded, sorted)
  -> Text       -- payload (empty for GET)
  -> Text       -- full URL
  -> IO (Either Text LBS.ByteString)
runReq noSign httpMethod host_ cPath cQuery payload url = do
  mReq <- try (parseRequest (T.unpack url)) :: IO (Either SomeException Request)
  case mReq of
    Left e -> pure (Left ("parse url failed: " <> T.pack (show e)))
    Right req0 -> do
      headers <- if noSign
                   then pure []
                   else signHeaders httpMethod host_ cPath cQuery payload
      let req = req0 { method = httpMethod
                     , requestHeaders = headers
                     , requestBody = RequestBodyBS (TE.encodeUtf8 payload)
                     }
      mgr <- Core.httpMgr
      r <- try (httpLbs req mgr)
             :: IO (Either SomeException (Response LBS.ByteString))
      case r of
        Left e -> pure (Left (T.pack (show e)))
        Right resp ->
          let sc = statusCode (responseStatus resp)
          in if sc >= 200 && sc < 300
               then pure (Right (responseBody resp))
               else pure $ Left
                 ( "http " <> T.pack (show sc) <> ": "
                <> TE.decodeUtf8 (LBS.toStrict (responseBody resp)) )

-- ## SigV4 signing
-- Reference: https://docs.aws.amazon.com/IAM/UserGuide/reference_aws-signing.html

data Creds = Creds
  { ckAccess :: BS.ByteString
  , ckSecret :: BS.ByteString
  , ckToken  :: Maybe BS.ByteString
  , ckRegion :: BS.ByteString
  }

readCreds :: IO (Either Text Creds)
readCreds = do
  ak <- lookupEnv "AWS_ACCESS_KEY_ID"
  sk <- lookupEnv "AWS_SECRET_ACCESS_KEY"
  tok <- lookupEnv "AWS_SESSION_TOKEN"
  reg <- lookupEnv "AWS_REGION"
  case (ak, sk) of
    (Just a, Just s) -> pure $ Right Creds
      { ckAccess = BS8.pack a
      , ckSecret = BS8.pack s
      , ckToken  = fmap BS8.pack tok
      , ckRegion = BS8.pack (fromMaybe "us-east-1" reg)
      }
    _ -> pure $ Left
      "S3 signed access requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY \
      \env vars (no profile/IAM/SSO support). Pass +n for anonymous access."

signHeaders
  :: BS.ByteString -> Text -> Text -> Text -> Text
  -> IO [(HeaderName, BS.ByteString)]
signHeaders httpMethod host_ cPath cQuery payload = do
  ec <- readCreds
  case ec of
    Left e -> ioError (userError (T.unpack e))
    Right c -> do
      now <- getCurrentTime
      pure (buildSigned c now httpMethod host_ cPath cQuery payload)

buildSigned
  :: Creds -> UTCTime -> BS.ByteString -> Text -> Text -> Text -> Text
  -> [(HeaderName, BS.ByteString)]
buildSigned c now httpMethod host_ cPath cQuery payload =
  let amzDate = BS8.pack (formatTime defaultTimeLocale "%Y%m%dT%H%M%SZ" now)
      dateStamp = BS8.pack (formatTime defaultTimeLocale "%Y%m%d" now)
      payloadHash = hexSha256 (TE.encodeUtf8 payload)
      hostBs = TE.encodeUtf8 host_
      tokenPair = case ckToken c of
        Nothing -> []
        Just t  -> [("x-amz-security-token", t)]
      -- Headers participating in the signature, sorted by lowercased name.
      signedPairs :: [(BS.ByteString, BS.ByteString)]
      signedPairs = sortBy (comparing fst)
        $ [ ("host", hostBs)
          , ("x-amz-content-sha256", payloadHash)
          , ("x-amz-date", amzDate)
          ]
        ++ tokenPair
      signedHeaderNames = BS.intercalate ";" (map fst signedPairs)
      canonicalHeaders =
        BS.concat [ k <> ":" <> v <> "\n" | (k, v) <- signedPairs ]
      canonicalReq = BS.intercalate "\n"
        [ httpMethod
        , TE.encodeUtf8 cPath
        , TE.encodeUtf8 cQuery
        , canonicalHeaders
        , signedHeaderNames
        , payloadHash
        ]
      crHash = hexSha256 canonicalReq
      scope = BS.intercalate "/"
        [dateStamp, ckRegion c, "s3", "aws4_request"]
      stringToSign = BS.intercalate "\n"
        ["AWS4-HMAC-SHA256", amzDate, scope, crHash]
      kDate    = hmacRaw ("AWS4" <> ckSecret c) dateStamp
      kRegion  = hmacRaw kDate (ckRegion c)
      kService = hmacRaw kRegion "s3"
      kSigning = hmacRaw kService "aws4_request"
      sig = B16.encode (hmacRaw kSigning stringToSign)
      authValue = BS.concat
        [ "AWS4-HMAC-SHA256 Credential=", ckAccess c, "/", scope
        , ", SignedHeaders=", signedHeaderNames
        , ", Signature=", sig
        ]
      -- Single source of truth for the request-header list: emit every
      -- signed header (including x-amz-security-token) exactly once,
      -- then append Authorization. Avoids duplicating the session
      -- token and keeps casing consistent with the canonical-headers
      -- block used to compute the signature.
  in [ (mk k, v) | (k, v) <- signedPairs ]
     ++ [ (mk "Authorization", authValue) ]

-- ## Crypto helpers

hexSha256 :: BS.ByteString -> BS.ByteString
hexSha256 b = B16.encode (BA.convert (H.hash b :: H.Digest H.SHA256))

hmacRaw :: BS.ByteString -> BS.ByteString -> BS.ByteString
hmacRaw k v = BA.convert (HMAC.hmac k v :: HMAC.HMAC H.SHA256)

-- ## URL encoding helpers
-- AWS SigV4 requires RFC 3986 unreserved chars unencoded; everything
-- else percent-encoded. http-types' urlEncode does this. The Bool
-- selects whether '/' should also be encoded.

urlEncQS :: Text -> Text
urlEncQS = TE.decodeUtf8 . URI.urlEncode True . TE.encodeUtf8

urlEncPath :: Text -> Text
urlEncPath = TE.decodeUtf8 . URI.urlEncode False . TE.encodeUtf8

-- ## Minimal XML→JSON for ListObjectsV2
-- The XML schema is fixed and tiny, so we do plain Text scans rather
-- than pull in xml-conduit. Output matches the aws CLI JSON shape:
--   {"Contents": [{"Key":..., "Size":..., "LastModified":...}, ...],
--    "CommonPrefixes": [{"Prefix":...}, ...]}

xmlToJson :: Text -> Text
xmlToJson xml =
  let contents = pluckEntries "Contents" xml
      prefixes = pluckEntries "CommonPrefixes" xml
      contentObjs = map mkContent contents
      prefixObjs  = map mkPrefix  prefixes
      arr xs = "[" <> T.intercalate "," xs <> "]"
  in "{\"Contents\":" <> arr contentObjs
       <> ",\"CommonPrefixes\":" <> arr prefixObjs <> "}"
  where
    mkContent body =
      let k = pluck1 "Key" body
          s = pluck1 "Size" body
          d = pluck1 "LastModified" body
      in "{\"Key\":" <> jsonStr k
         <> ",\"Size\":" <> (if T.null s then "0" else s)
         <> ",\"LastModified\":" <> jsonStr d <> "}"
    mkPrefix body =
      "{\"Prefix\":" <> jsonStr (pluck1 "Prefix" body) <> "}"

-- | All <tag>...</tag> bodies in document order.
pluckEntries :: Text -> Text -> [Text]
pluckEntries tag = go
  where
    open = "<" <> tag <> ">"
    close = "</" <> tag <> ">"
    go xml =
      case T.breakOn open xml of
        (_, rest) | T.null rest -> []
        (_, rest) ->
          let after = T.drop (T.length open) rest
              (body, tail0) = T.breakOn close after
          in if T.null tail0
               then []
               else body : go (T.drop (T.length close) tail0)

-- | First <tag>...</tag> body in the given snippet (empty if absent).
pluck1 :: Text -> Text -> Text
pluck1 tag xml =
  case pluckEntries tag xml of
    (x:_) -> xmlUnescape x
    []    -> ""

xmlUnescape :: Text -> Text
xmlUnescape =
    T.replace "&amp;" "&"
  . T.replace "&lt;" "<"
  . T.replace "&gt;" ">"
  . T.replace "&quot;" "\""
  . T.replace "&apos;" "'"

-- | JSON-escape a Text and wrap in quotes. S3 keys can contain backslash,
-- quote, control chars; we handle the cases that actually occur.
jsonStr :: Text -> Text
jsonStr t = "\"" <> T.concatMap esc t <> "\""
  where
    esc '"'  = "\\\""
    esc '\\' = "\\\\"
    esc '\n' = "\\n"
    esc '\r' = "\\r"
    esc '\t' = "\\t"
    esc c    = T.singleton c
