import Control.Exception
import Control.Concurrent
import Data.Maybe
import Data.Function
import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Char as Char
import qualified System.Process as Cmd
import qualified Data.ByteString.Char8 as BS
import Data.Attoparsec.ByteString.Char8 hiding (takeWhile)
import Data.List.Split
import Data.Either
import Numeric
import System.Environment
import Text.Printf

import Debug.Trace

debug x = trace (show x) x

data Flow = Flow {ftime :: Double, src :: Int, dst :: Int, size :: Int, ftag :: String, faddr :: Int}
instance Show Flow where
    show flow = unwords $ map ($ flow) [(printf "%.9f") . ftime, show . src, show . dst, show . size, ftag, show . faddr]

data Record = Record {node :: Int, rtime :: Double, addr :: Int, rlength :: Int, rtag :: String}
instance Show Record where
    show record = unwords $ map ($ record) [show . node, show . rtime, show . addr, show . rlength, rtag]

data NodeInfo = NodeInfo {nid :: Int, memRange :: Int, diskRange :: Int, metaTime :: Double}

-- IO In Phase

main = do
    args <- getArgs
    --let args = 
    --        [
    --            "results_hs/wordcount/", 
    --            "0-mem-test"
    --        ]
    handleArgs args

readTraceFile :: String -> IO BS.ByteString
readTraceFile fileName =
    if "-disk-" `List.isInfixOf` fileName
    then 
        let readDiskInput diskTraceFileName = do
                tmp0 <- Cmd.readProcess "blkparse" [diskTraceFileName] ""
                tmp1 <- Cmd.readProcess "grep" ["java"] tmp0
                Cmd.readProcess "python" ["get_disk_io.py"] tmp1
        in  fmap BS.pack $ readDiskInput fileName
    else BS.readFile fileName

stripDirectory fileName = 
    let 
        remainder = List.dropWhile (/= '/') fileName
    in  if ([] == remainder) 
        then fileName 
        else (stripDirectory (tail remainder))

-- "Usage: ./parseTrace.hs <out file> <trace files...>"
handleArgs :: [String] -> IO()
handleArgs (outprefix:"-make":alsoCollapse:traceFiles) = do
    putStrLn $ "Making flows from original traces. Collapsing also: " ++ alsoCollapse
    let neededTraceFiles = filter (\fn -> (not ("disk" `List.isInfixOf` fn)) || (Char.ord (last fn) == 48)) traceFiles
    traces <- mapM readTraceFile neededTraceFiles --mapM isn't *that* kind of lazy
    let entries = map (\(a, b) -> readFlows a b) $ 
                    zip (map stripDirectory neededTraceFiles) (map BS.lines traces)
    --records is list of (NodeInfo, records)
    let nodeInfos = groupNodeInfo $ map fst entries 
    let records = 
            List.partition (\r -> "nic" `List.isPrefixOf` (rtag r)) $ 
            List.foldl' (++) [] $ 
            map snd entries

    let archs = ["rack-scale", "res-based"]
    let opts = 
            if (Char.toLower . head) alsoCollapse == 'y'
            then ["plain", "combined", "timeonly"]
            else ["plain"]

    let perms = foldr (++) [] $ map (\a -> (map (\b -> (a,b)) opts)) archs
    let fns = map (\(a,b) -> (outprefix) ++ a ++ "_" ++ b ++ "_flows.txt") perms
    
    writeFlows ((outprefix) ++ "nic_flows.txt") (makeNicFlows $ fst records) 
    writeResults $ zip fns $ map (\(arch, opt) -> combineFlows (makeFlows nodeInfos (snd records) arch) opt) perms

handleArgs (outprefix:"-collapse":opt:flowFile:_) = do
    putStrLn $ "Collapsing only in mode: " ++ opt ++ " for file: " ++ flowFile
    trace <- BS.readFile flowFile
    let flows = if (opt == "combined" || opt == "timeonly")
                then combineFlows 
                            (
                                map (\(_:ts:s:d:l:t:ad:_) -> 
                                    Flow {
                                        ftime = fastReadDouble ts, 
                                        src = fastReadInt s, 
                                        dst = fastReadInt d, 
                                        size = fastReadInt l, 
                                        ftag = BS.unpack t, 
                                        faddr = fastReadInt ad
                                    }) $ 
                                map BS.words $ 
                                BS.lines trace
                            )
                            opt
                else []
    let newFileName = 
            let sp = splitOn "/" flowFile 
            in  (List.intercalate "/" $ init sp) ++ "/" ++ 
                ((head (splitOn "_" (last sp))) ++ "_" ++ opt ++ "_flows.txt")
    writeFlows newFileName flows


-- Processing Phase

groupNodeInfo :: [NodeInfo] -> [NodeInfo]
groupNodeInfo nodeInfos = 
    let
        mergeNodeInfos (a:b:c:_) = 
            NodeInfo {
                nid = nid a,
                memRange = (memRange a) + (memRange b) + (memRange c),
                diskRange = (diskRange a) + (diskRange b) + (diskRange c),
                metaTime = (metaTime a) + (metaTime b) + (metaTime c)
            }
    in  map mergeNodeInfos $
            List.groupBy (\ a b -> nid a == nid b) $
            List.sortBy (compare `on` nid) $ 
            filter (\x -> (memRange x /= 0 || diskRange x /= 0 || metaTime x /= 0)) nodeInfos

fastReadInt bstr = fst $ fromMaybe (trace (show bstr) (0, BS.pack "")) $ BS.readInt bstr
fastReadDouble bstr = either (\err -> 0.0) id $ parseOnly double bstr

readFlows :: String -> [BS.ByteString] -> (NodeInfo, [Record])
readFlows fileName ls
    | "-mem-" `List.isInfixOf` fileName = 
        let
            readMemoryFlow :: BS.ByteString -> Record
            readMemoryFlow x = 
                let _:ts:addr:len:pgSize:_ = BS.words x
                    fts = fastReadDouble ts
                    rw = if fts < 0 then "memRead" else "memWrite"
                in  Record {
                        node = nodeId, 
                        rtime = (abs fts) / 1e6, 
                        addr = fastReadInt addr * 4096, 
                        rlength = fastReadInt len * fastReadInt pgSize, 
                        rtag = rw
                    }
            records = map readMemoryFlow ls
            addrs = map addr records
            range = List.maximum addrs - List.minimum addrs
            ninfo = NodeInfo {nid = nodeId, memRange = range, diskRange = 0, metaTime = 0}
        in  (ninfo, records)
    | "-disk-" `List.isInfixOf` fileName = 
        let
            readDiskFlow :: BS.ByteString -> Record
            readDiskFlow x = 
                let _:ts:_:addr:_:len:_:_:_:rw:_ = BS.words x
                in  Record {
                        node = nodeId, 
                        rtime = (fastReadDouble ts), 
                        addr = (fastReadInt addr) * 4096, 
                        rlength = (fastReadInt len) * 4096, 
                        rtag = (if Char.toLower (BS.head rw) == 'r' then "diskRead" else "diskWrite")
                    }
            records = map readDiskFlow ls
            addrs = map addr records
            range = List.maximum addrs - List.minimum addrs
            ninfo = NodeInfo {nid = nodeId, memRange = 0, diskRange = range, metaTime = 0}
        in  (ninfo, records)
    | "-nic-" `List.isInfixOf` fileName = 
        let
            readNicFlow :: BS.ByteString -> Record
            readNicFlow x = 
                let t:s:d:len:_ = BS.words x
                    getHostId = List.takeWhile (/= '.') 
                in  Record {
                        node = nodeId, 
                        rtime = (fastReadDouble t), 
                        addr = 0, 
                        rlength = (fastReadInt len), 
                        rtag = "nic " ++ (unwords $ map (getHostId . BS.unpack) [s,d]) 
                    }
        in (NodeInfo {nid = nodeId, memRange = 0, diskRange = 0, metaTime = 0}, map readNicFlow ls)
    | "-meta-" `List.isInfixOf` fileName = 
        (NodeInfo {nid = nodeId, memRange = 0, diskRange = 0, metaTime = (fastReadDouble (head ls)) / 1e6}, [])
    where 
        nodeId = read [head fileName]

-- list of records to list of flows

makeNicFlows :: [Record] -> [Flow]
makeNicFlows records = 
    let 
        groupedByNode = 
            map (\grp -> (grp, (node (head grp)))) $
            List.groupBy (\ a b -> node a == node b) $
            List.sortBy (compare `on` node) records 
        
        hostMap = 
            let findHostName :: [Record] -> String 
                findHostName recs = 
                    let sdset = List.foldl1' (Set.intersection) $ 
                            map (Set.fromList . (tail . words . rtag)) recs
                    in  if (Set.size sdset /= 1) then "" else (Set.findMin sdset)
            in  Map.fromList $ 
                filter ((/= "") . fst) $ 
                map (\(a,b) -> (findHostName a, b)) groupedByNode

        makeNicFlow :: Record -> Flow
        makeNicFlow r = 
            let
                sd = map (\x -> fromMaybe (-1) (Map.lookup x hostMap)) $ (tail . words . rtag) r
            in  Flow {ftime = (rtime r), src = (head sd), dst = (last sd), size = (rlength r), ftag = "nic", faddr = 0}
    
    in  adjustTime $ map makeNicFlow records

makeFlows :: [NodeInfo] -> [Record] -> String -> [Flow]
makeFlows nodeInfos records model =
    let
        numNodes = length nodeInfos
        isMem = ((List.isPrefixOf "mem") . rtag)
        isDisk = ((List.isPrefixOf "disk") . rtag)
        thisNodeInfo record = 
            fromMaybe (undefined) $
            List.find (\n -> nid n == node record) nodeInfos
        
        mapToNode :: Record -> Int
        mapToNode record 
            | model == "rack-scale" && (isMem record)  = truncate $ memNodeCase * (fromIntegral numNodes)
            | model == "rack-scale" && (isDisk record) = truncate $ diskNodeCase * (fromIntegral numNodes)
            | model == "res-based"  && (isMem record)  = numNodes + (truncate $ memNodeCase * (fromIntegral numNodes))
            | model == "res-based"  && (isDisk record) = 2 * numNodes + (truncate $ diskNodeCase * 3)
            where 
                memNodeCase = (fromIntegral $ addr record) / (fromIntegral $ memRange (thisNodeInfo record)) :: Double
                diskNodeCase = (fromIntegral $ addr record) / (fromIntegral $ diskRange (thisNodeInfo record)) :: Double
        
        recordToFlow record
            | (isMem record) && ("Read" `List.isSuffixOf` (rtag record)) = Flow {
                ftime = rtime record, 
                src = mapToNode record,
                dst = node record,
                size = rlength record,
                ftag = rtag record,
                faddr = addr record 
                }
            | (isMem record) && ("Write" `List.isSuffixOf` (rtag record)) = Flow {
                ftime = rtime record, 
                src = node record,
                dst = mapToNode record,
                size = rlength record,
                ftag = rtag record,
                faddr = addr record
                }
            | (isDisk record) && ("Read" `List.isSuffixOf` (rtag record)) = Flow {
                ftime = (rtime record) + (metaTime (thisNodeInfo record)), 
                src = mapToNode record,
                dst = node record,
                size = rlength record,
                ftag = rtag record,
                faddr = addr record
                }
            | (isDisk record) && ("Write" `List.isSuffixOf` (rtag record)) = Flow {
                ftime = (rtime record) + (metaTime (thisNodeInfo record)),
                src = node record,
                dst = mapToNode record,
                size = rlength record,
                ftag = rtag record,
                faddr = addr record
                }
       
    in  adjustTime $ map recordToFlow records

adjustTime :: [Flow] -> [Flow]
adjustTime flows = 
    let
        sortedFlows = List.sortBy (compare `on` ftime) flows
        startTime = (ftime . head) sortedFlows
    in  map (\f -> Flow {ftime = (ftime f - startTime), src = src f, dst = dst f, size = size f, ftag = ftag f, faddr = faddr f}) sortedFlows 

-- combine flows together

combineFlows :: [Flow] -> String -> [Flow]
combineFlows flows option = 
    let
        grp = List.foldl1' (\agg f -> Flow {
                ftime = (ftime agg), 
                src = (src agg), 
                dst = (dst agg), 
                size = (size agg) + (size f), 
                ftag = (ftag agg), 
                faddr = (faddr agg)
            }) 

        seqTime a b = (abs ((ftime b) - (ftime a))) <= 50e-6
        typeCheck a b = ftag a == ftag b
        
        comb :: (Flow -> Flow -> Bool) -> [Flow] -> [Flow] -> [Flow]
        comb fn [] [] = []
        comb fn agg [] = [grp agg]
        comb fn [] (f:fs) = comb fn [f] fs
        comb fn agg (f:fs) = 
            if   fn (last agg) f 
            then comb fn (agg ++ [f]) fs 
            else (grp agg) : comb fn [f] fs

        combine :: String -> [Flow] -> [Flow]
        combine "plain" flows = flows
        combine "combined" flows = 
            let 
                seqAddr old f = faddr old + size old == faddr f
                fn a b = (typeCheck a b && seqAddr a b && seqTime a b)
            in comb fn [] flows
            --in combine fn [] [] flows
        combine "timeonly" flows = 
            let
                fn a b = (typeCheck a b && seqTime a b)
            in  comb fn [] flows
        --combine "timeonly" flows = combine seqTime [] [] flows  
    
    in  List.sortBy (compare `on` ftime) $
        List.foldl' (++) [] $ 
        Map.elems $ 
        Map.map (combine option) $
        Map.fromListWith (++) $ 
        map (\f -> ((src f, dst f), [f])) flows


-- IO Out Phase

writeResults :: [(String, [Flow])] -> IO()
writeResults = mapM_ (\(f, fs) -> writeFlows f fs)

writeFlows :: String -> [Flow] -> IO ()
writeFlows fileName []    = putStrLn "No Flows to Write"
writeFlows fileName flows = do
                    putStrLn $ fileName ++ " " ++ (show (length flows))
                    writeFile fileName $ 
                                unlines $ 
                                map (\x -> fst x ++ " " ++ snd x) $
                                zip [show x | x <- [1,2..]] $ 
                                map show flows
                                --filter (\f -> (src f /= (-1) && dst f /= (-1))) flows

