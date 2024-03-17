{-# LANGUAGE DeriveGeneric, OverloadedStrings, OverloadedRecordDot, DuplicateRecordFields #-}

module Main where

import Control.Applicative ( (<|>) )
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad.Trans.Class
import Control.Monad.Trans.Maybe
import Data.Aeson
import Data.ByteString ( ByteString )
import Data.ByteString.Builder ( byteString, toLazyByteString )
import Data.Cache.LRU ( LRU )
import Data.Functor.Identity
import Data.Hashable ( Hashable )
import Data.HashMap.Lazy (HashMap, (!?), (!))
import Data.List ( sortBy, intersperse )
import Data.Maybe ( fromMaybe, listToMaybe )
import Data.Text.Lazy (Text)
import Data.Time.Clock ( utctDay, utctDayTime )
import Data.Time.LocalTime ( timeToTimeOfDay, todHour, todMin, todSec )
import Data.Time.Calendar ( toGregorian )
import Data.Time.Clock.System
import GHC.Generics (Generic)
import Network.HTTP.Types.Status
import System.Directory ( createDirectoryIfMissing, listDirectory )
import Web.Scotty

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC
import qualified Data.Cache.LRU as LRU
import qualified Data.HashMap.Lazy as HM
import qualified Data.Sequence as Seq
import qualified Data.Text.Lazy as T

type IndexedByM d i m a = d i (m a)

type Year   = Text
type Month  = Text
type Day    = Text
type Hour   = Text
type Minute = Text
type Second = Text

type Millisecond = Text
type UniqueMsId  = Text

type IndexedByYearM   d m a =                       IndexedByM d Year   m a
type IndexedByMonthM  d m a = IndexedByYearM   d m (IndexedByM d Month  m a)
type IndexedByDayM    d m a = IndexedByMonthM  d m (IndexedByM d Day    m a)
type IndexedByHourM   d m a = IndexedByDayM    d m (IndexedByM d Hour   m a)
type IndexedByMinuteM d m a = IndexedByHourM   d m (IndexedByM d Minute m a)
type IndexedBySecondM d m a = IndexedByMinuteM d m (IndexedByM d Second m a)

type IndexedBySecondRM d m a = IndexedByM d Second m a
type IndexedByMinuteRM d m a = IndexedByM d Minute m (IndexedBySecondRM d m a)
type IndexedByHourRM   d m a = IndexedByM d Hour   m (IndexedByMinuteRM d m a)
type IndexedByDayRM    d m a = IndexedByM d Day    m (IndexedByHourRM   d m a)
type IndexedByMonthRM  d m a = IndexedByM d Month  m (IndexedByDayRM    d m a)
type IndexedByYearRM   d m a = IndexedByM d Year   m (IndexedByMonthRM  d m a)

type IndexedBySecond   a = IndexedBySecondM HashMap Identity a
type PersistedBySecond a = IndexedBySecondM HashMap IO       a

type ByYear = Year
type ByMonth = (Year, Month)
type ByDay = (Year, Month, Day)
type ByHour = (Year, Month, Day, Hour)
type ByMinute = (Year, Month, Day, Hour, Minute)
type BySecond = (Year, Month, Day, Hour, Minute, Second)

type IndexSecond = BySecond
type MessageLocator = (Millisecond, UniqueMsId)

data IChannel a = IChannel
    { name :: Text
    , hot  :: IndexedBySecond a
    , cold :: PersistedBySecond a
    , lru  :: LRU IndexSecond ()
    }

data IMessage a = IMessage
    { ms      :: Millisecond
    , uid     :: UniqueMsId
    , content :: a
    } deriving ( Show )
instance ToJSON (IMessage a) where
    toJSON m = String $ T.toStrict $ T.concat [m.ms, "/", m.uid]

type Message = IMessage ByteString
type Partition = [Message]
type Channel = IChannel Partition
type Channels = HashMap Text (MVar Channel)

data State = State
    { channels :: MVar Channels }

emptyChannel :: Text -> Channel
emptyChannel n = IChannel
    { name = n
    , hot  = HM.empty
    , cold = HM.empty
    , lru  = LRU.newLRU $ Just 100
    }

getPartitionM :: Monad m => IndexSecond -> IndexedBySecondM HashMap m Partition -> MaybeT m Partition
getPartitionM (y, m, d, h, mm, s) i
    =        hoistMaybe   (i !? y) >>= lift
        >>= (hoistMaybe . (!? m )) >>= lift
        >>= (hoistMaybe . (!? d )) >>= lift
        >>= (hoistMaybe . (!? h )) >>= lift
        >>= (hoistMaybe . (!? mm)) >>= lift
        >>= (hoistMaybe . (!? s )) >>= lift

getHotPartition :: IndexSecond -> IndexedBySecond Partition -> Maybe Partition
getHotPartition k i = runIdentity $ runMaybeT $ getPartitionM k i

getColdPartition :: IndexSecond -> PersistedBySecond Partition -> IO (Maybe Partition)
getColdPartition k i = runMaybeT $ getPartitionM k i

insertPartitionM :: Monad m => IndexedBySecondM HashMap m Partition -> IndexSecond -> Partition -> m (IndexedBySecondM HashMap m Partition)
insertPartitionM i (y, m, d, h, mm, s) p = do
    yP  <- HM.findWithDefault (return HM.empty) y  i
    yM  <- HM.findWithDefault (return HM.empty) m  yP
    yD  <- HM.findWithDefault (return HM.empty) d  yM
    yH  <- HM.findWithDefault (return HM.empty) h  yD
    yMM <- HM.findWithDefault (return HM.empty) mm yH
    return $ flip (HM.insert y) i $
        return $ flip (HM.insert m ) yP $
        return $ flip (HM.insert d ) yM $
        return $ flip (HM.insert h ) yD $
        return $ flip (HM.insert mm) yH $
        return $ flip (HM.insert s ) yMM (return p)

insertHotPartition :: IndexedBySecond Partition -> IndexSecond -> Partition -> IndexedBySecond Partition
insertHotPartition i k p = runIdentity $ insertPartitionM i k p

insertColdPartition :: PersistedBySecond Partition -> IndexSecond -> Partition -> IO (PersistedBySecond Partition)
insertColdPartition i k p = insertPartitionM i k p

prio :: Channel -> IndexSecond -> Channel
prio ch k = ch { lru = LRU.insert k () ch.lru }

warmup :: Channel -> IndexSecond -> Partition -> Channel
warmup ch k p = (prio ch k) { hot = insertHotPartition ch.hot k p }

getPartition :: Channel -> IndexSecond -> IO (Channel, Maybe Partition)
getPartition ch k = do
    let hotP = getHotPartition k ch.hot
    (ch2, mp) <- case hotP of
        Just p  -> return (prio ch k, Just p)
        Nothing -> do
            coldP <- getColdPartition k ch.cold
            case coldP of
                Just p  -> return (warmup ch k p, Just p)
                Nothing -> return (ch, Nothing)
    return (ch2, mp)

updatePartition :: Channel -> IndexSecond -> (Partition -> Partition) -> IO (Channel, Maybe Partition)
updatePartition ch k f = do
    let hotP = getHotPartition k ch.hot
    (ch2, mp) <- case hotP of
        Just p  -> return (warmup ch k $ f p, Just $ f p)
        Nothing -> do
            coldP <- getColdPartition k ch.cold
            case coldP of
                Just p  -> return (warmup ch k $ f p, Just $ f p)
                Nothing -> return (ch, Nothing)
    return (ch2, mp)

updateOrCreatePartition :: Channel -> IndexSecond -> (Partition -> Partition) -> IO (Channel, Maybe Partition)
updateOrCreatePartition ch k f = do
    let hotP = getHotPartition k ch.hot
    (ch2, mp) <- case hotP of
        Just p  -> return (warmup ch k $ f p, Just $ f p)
        Nothing -> do
            coldP <- getColdPartition k ch.cold
            case coldP of
                Just p  -> return (warmup ch k $ f p, Just $ f p)
                Nothing -> return (warmup ch k $ f [], Just $ f [])
    return (ch2, mp)

toIndexSecond :: SystemTime -> IndexSecond
toIndexSecond time =
    ( T.pack $ show y
    , T.pack $ show m
    , T.pack $ show d
    , T.pack $ show $ todHour tod
    , T.pack $ show $ todMin tod
    , T.pack $ show $ floor $ todSec tod )
    where
        utc = systemToUTCTime time
        (y, m, d) = toGregorian $ utctDay utc
        tod = timeToTimeOfDay $ utctDayTime utc

channelPost :: Channel -> ByteString -> IO (Channel, Maybe Partition)
channelPost ch payload = do
    time <- getSystemTime
    let millis = T.pack $ show $ floor ((fromIntegral $ systemNanoseconds time :: Float) / 1000 / 1000)
    let m = IMessage
            { ms      = millis
            , uid     = "asdf"
            , content = payload }
    let ixs = toIndexSecond time
    updateOrCreatePartition ch ixs $ \p -> (m:p)

locateMessage :: Partition -> MessageLocator -> Maybe Message
locateMessage p (ms', uid') = listToMaybe $ filter (\m -> m.ms == ms' && m.uid == uid') p

channelGetOne :: Channel -> IndexSecond -> MessageLocator -> IO (Channel, Maybe Message)
channelGetOne ch ixs ml = do
    (ch2, mp) <- getPartition ch ixs
    return (ch2, mp >>= flip locateMessage ml)

latestM :: Monad m => IndexedByM HashMap Text m a -> MaybeT m (Text, a)
latestM i = do
    k <- hoistMaybe $ listToMaybe $ sortBy (\x y -> compare y x) $ HM.keys i
    v <- lift $ i ! k
    return (k, v)

latestM_Y :: Monad m => IndexedByYearRM HashMap m Partition -> MaybeT m (BySecond, Partition)
latestM_Y i = do
    (y, v) <- latestM i
    latestM_M y v

latestM_M :: Monad m => ByYear -> IndexedByMonthRM HashMap m Partition -> MaybeT m (BySecond, Partition)
latestM_M y i = do
    (m, v) <- latestM i
    latestM_D (y, m) v

latestM_D :: Monad m => ByMonth -> IndexedByDayRM HashMap m Partition -> MaybeT m (BySecond, Partition)
latestM_D (y, m) i = do
    (d, v) <- latestM i
    latestM_H (y, m, d) v

latestM_H :: Monad m => ByDay -> IndexedByHourRM HashMap m Partition -> MaybeT m (BySecond, Partition)
latestM_H (y, m, d) i = do
    (h, v) <- latestM i
    latestM_Mm (y, m, d, h) v

latestM_Mm :: Monad m => ByHour -> IndexedByMinuteRM HashMap m Partition -> MaybeT m (BySecond, Partition)
latestM_Mm (y, m, d, h) i = do
    (mm, v) <- latestM i
    latestM_S (y, m, d, h, mm) v

latestM_S :: Monad m => ByMinute -> IndexedBySecondRM HashMap m Partition -> MaybeT m (BySecond, Partition)
latestM_S (y, m, d, h, mm) i = do
    (s, v) <- latestM i
    return ((y, m, d, h, mm, s), v)

latestHotMessage :: IndexedBySecond Partition -> Maybe (BySecond, Partition, Message)
latestHotMessage i = do
    (ixs, p) <- runIdentity $ runMaybeT $ latestM_Y i
    Just (ixs, p, head p)

latestColdMessage :: PersistedBySecond Partition -> IO (Maybe (BySecond, Partition, Message))
latestColdMessage i = do
    r <- runMaybeT $ latestM_Y i
    case r of
        Just (ixs, p) -> return $ Just (ixs, p, head p)
        Nothing       -> return Nothing

latestMessage :: Channel -> IO (Channel, (Maybe (BySecond, Message)))
latestMessage ch = do
    let hotLM = latestHotMessage ch.hot
    case hotLM of
        Just (k, _, m) -> return (prio ch k, Just (k, m))
        Nothing -> do
            coldLM <- latestColdMessage ch.cold
            case coldLM of
                Just (k, p, m) -> return (warmup ch k p, Just (k, m))
                Nothing -> return (ch, Nothing)

channelPostRoute :: State -> ActionM ()
channelPostRoute state = do
    n <- pathParam "name"
    payload <- body
    cs <- liftIO $ readMVar state.channels
    let ch' = cs ! n
    ch <- liftIO $ takeMVar ch'
    (ch2, mr) <- liftIO $ channelPost ch $ B.concat $ BL.toChunks $ payload
    liftIO $ putMVar ch' ch2
    case mr of
        Just _  -> status status200
        Nothing -> status status404

channelGetLatestRoute :: State -> ActionM ()
channelGetLatestRoute state = do
    n <- pathParam "name"
    cs <- liftIO $ readMVar state.channels
    let ch' = cs ! n
    ch <- liftIO $ takeMVar ch'
    (ch2, mr) <- liftIO $ latestMessage ch
    liftIO $ putMVar ch' ch2
    case mr of
        Just (_, m) -> do
            raw $ toLazyByteString $ byteString m.content
        Nothing       -> status status404

routes :: State -> ScottyM ()
routes state = do
    post   "/channel/:name"        $ channelPostRoute state
    get    "/channel/:name/latest" $ channelGetLatestRoute state
    get "/" $ do
        cs <- liftIO $ readMVar state.channels
        c <- liftIO $ readMVar (cs ! "one")
        json (HM.keys c.cold, c.hot)

serve :: State -> IO ()
serve state = scotty 3000 $ do
    routes state
    notFound $ status status404

initState :: IO State
initState = do
    c1 <- newMVar $ emptyChannel "one"
    csM <- newMVar (HM.singleton "one" c1)
    return $ State
        { channels = csM }

main :: IO ()
main = initState >>= serve
