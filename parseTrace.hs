import System.Environment
import Data.Maybe
import Data.Function
import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Char as Char
import qualified System.Process as Cmd

data Flow = Flow {ftime :: Float, src :: Int, dst :: Int, size :: Int, ftag :: String, faddr :: Int}
instance Show Flow where
    show flow = unwords $ map ($ flow) [show . ftime, show . src, show . dst, show . size, ftag]

data Record = Record {node :: Int, rtime :: Float, addr :: Int, rlength :: Int, rtag :: String}
instance Show Record where
    show record = unwords $ map ($ record) [show . node, show . rtime, show . addr, show . rlength, rtag]


-- IO In Phase

-- "Usage: ./parseTrace.hs <out file> <trace files...>"
main = do
    args <- getArgs
    --let args = ["tmp2", "traces/wordcount_with_nic/0-nic-ec2-54-161-230-217.compute-1.amazonaws.com"]
    let traceFiles = tail args
    let readTraceFile fileName =
            if "-disk-" `List.isInfixOf` fileName
            then 
                let readDiskFlows diskTraceFileName = do
                    tmp0 <- Cmd.readProcess "blkparse" [diskTraceFileName] ""
                    tmp1 <- Cmd.readProcess "grep" ["java"] tmp0
                    Cmd.readProcess "python" ["get_disk_io.py"] tmp1
                in  readDiskFlows fileName
            else readFile fileName
    traces <- mapM readTraceFile traceFiles
    let stripDirectory fileName = 
            if ([] == dropWhile (/= '/') fileName) 
            then fileName 
            else (stripDirectory (tail (List.dropWhile (/='/') fileName)))
        records = readFlows $ 
                    Map.fromList $ 
                    zip (map stripDirectory traceFiles) (map lines traces)
        nicFlows = makeNicFlows $ snd records
      in  writeFlows (head args) nicFlows

-- Processing Phase

readFlows :: Map.Map String [String] -> (Map.Map Int [Record], Map.Map Int [Record])
readFlows traceMap =
    let 
        getRecords :: (Int -> String -> Record) -> Map.Map String [String] -> Map.Map Int [Record]
        getRecords fn m =
            let nodeId a = read [head a]
            in  Map.fromList $ map (\(a,b) -> (nodeId a, map (fn $ nodeId a) b)) $ Map.toList m;

        filterTraces :: Map.Map String [String] -> String -> Map.Map String [String]
        filterTraces tm fil = Map.fromList $ filter (\x -> List.isInfixOf fil (fst x)) $ Map.toList tm

        traceMapFilter = filterTraces traceMap

        metaRecords = 
            let nodeId :: String -> Int
                nodeId a = read [head a]
            in  Map.fromList $ map (\(a,b) -> (nodeId a, [Record {node = nodeId a, rtime = (read . head) b, addr = 0, rlength = 0, rtag = "meta"}])) $ Map.toList (traceMapFilter "-meta-")

        readMemoryFlow :: Int -> String -> Record
        readMemoryFlow n x = 
            let rid:ts:addr:len:pgSize:_ = words x
                fts = read ts
                rw = if fts < 0 then "memRead" else "memWrite"
            in  Record {
                    node = n, 
                    rtime = (abs fts), 
                    addr = (read addr), 
                    rlength = (read len) * (read pgSize), 
                    rtag = rw
                }
        memRecords = getRecords readMemoryFlow $ traceMapFilter "-mem-";

        readNicFlow :: Int -> String -> Record
        readNicFlow n x = 
            let t:s:d:len:_ = words x
                getHostId = List.takeWhile (/= '.')
            in  Record {node = n, rtime = (read t), addr = 0, rlength = (read len), rtag = "nic " ++ (unwords $ map getHostId [s,d]) }
        nicRecords = getRecords readNicFlow $ traceMapFilter "-nic-";

        readDiskFlow :: Int -> String -> Record
        readDiskFlow n x = 
            let _:ts:_:addr:_:len:_:_:_:rw:_ = words x
            in  Record {
                    node = n, 
                    rtime = (read ts), 
                    addr = (read addr), 
                    rlength = (read len) * 4096, 
                    rtag = (if Char.toLower (head rw) == 'r' then "diskRead" else "diskWrite")
                }
        diskRecords = getRecords readDiskFlow $ traceMapFilter "-disk-";
    -- *Records are all Map.Map Int [Record].
    in  (Map.fromListWith (++) $ foldl (++) [] $ map Map.toList [metaRecords, memRecords, diskRecords], nicRecords)


makeNicFlows :: Map.Map Int [Record] -> [Flow]
makeNicFlows nicMap = 
    let 
        nicMapping = -- Map.Map String Int
            let findHostName :: [Record] -> String
                findHostName recs = 
                    let sdset = foldl1 (Set.intersection) $ map (Set.fromList . (tail . words . rtag)) recs
                    in  if (Set.size sdset /= 1) then "" else (Set.findMin sdset)
            in  Map.fromList $ map (\(one, two) -> (two, one)) $ Map.toList $ Map.map findHostName nicMap

        makeNicFlow :: Map.Map String Int -> Record -> Flow
        makeNicFlow nm r = 
            let
                sd = map (\x -> fromMaybe (-1) (Map.lookup x nm)) $ (tail . words . rtag) r
            in  Flow {ftime = (rtime r), src = (head sd), dst = (last sd), size = (rlength r), ftag = "nic", faddr = 0}
    in  map (makeNicFlow nicMapping) $ Map.foldl (++) [] nicMap

makeFlows :: Map.Map Int [Record] -> String -> String -> [Flow]
makeFlows records model  option = 
    let 
        numNodes = Map.size records
        isMem = ((List.isPrefixOf "mem") . rtag)
        isDisk = ((List.isPrefixOf "disk") . rtag)

        nodeToAddrRange = Map.map (\rs ->
                let memAddrs = map addr $ filter isMem rs
                    diskAddrs = map addr $ filter isDisk rs
                    range x = List.maximum x - List.minimum x
                in (range memAddrs, range diskAddrs)
            ) records

        mapToNode :: Record -> Int
        mapToNode record 
            | model == "rack-scale" && (isMem record)  = truncate $ memNodeCase * (fromIntegral numNodes)
            | model == "rack-scale" && (isDisk record) = truncate $ diskNodeCase * (fromIntegral numNodes)
            | model == "res-based"  && (isMem record)  = numNodes + (truncate $ memNodeCase * (fromIntegral numNodes))
            | model == "res-based"  && (isDisk record) = 2 * numNodes + (truncate $ diskNodeCase * 3)
            where 
                range = nodeToAddrRange Map.! (node record)
                memNodeCase = (fromIntegral $ addr record) / (fromIntegral $ fst range) :: Float
                diskNodeCase = (fromIntegral $ addr record) / (fromIntegral $ snd range) :: Float
        
        allRecords = filter (\r -> not ("meta" == (rtag r))) $ foldl (++) [] $ Map.elems records
        -- meta map : node id -> start time
        metaRecords = Map.fromList $
            map (\r -> (node r, rtime r)) $
            filter ((List.isPrefixOf "meta") . rtag) allRecords

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
                ftime = (rtime record) - (metaRecords Map.! (node record)), 
                src = mapToNode record,
                dst = node record,
                size = rlength record,
                ftag = rtag record,
                faddr = addr record
                }
            | (isDisk record) && ("Write" `List.isSuffixOf` (rtag record)) = Flow {
                ftime = rtime record - (metaRecords Map.! (node record)), 
                src = node record,
                dst = mapToNode record,
                size = rlength record,
                ftag = rtag record,
                faddr = addr record
                }

        -- combining

        grp = foldl1 (\agg f -> Flow {
                ftime = (ftime agg), 
                src = (src agg), 
                dst = (dst agg), 
                size = (size agg) + (size f), 
                ftag = (ftag agg), 
                faddr = (faddr agg)
            }) 
        seqTime a b = ftime b <= ((ftime a) + 10)

        combine :: (Flow -> Flow -> Bool) -> [Flow] -> [Flow] -> [Flow] -> [Flow]
        combine fn res agg [] = res ++ [grp agg]
        combine fn res agg (f:fs) = 
            let old = tail agg
            in  if fn (last agg) f 
                then combine fn res (agg ++ [f]) fs 
                else combine fn (res ++ [grp agg]) [f] fs
        
        combineFlows :: String -> [Flow] -> [Flow]
        combineFlows "plain" flows = flows
        combineFlows "combined" flows = 
            let 
                seqAddr old f = faddr old + size old == faddr f
                fn a b = (seqAddr a b && seqTime a b)
            in combine fn [] [] flows
        combineFlows "timeOnly" flows = combine seqTime [] [] flows
        
    in  List.sortBy (compare `on` ftime) $
        foldl (++) [] $ Map.elems $ Map.map (combineFlows option) $
        Map.fromListWith (++) $ map (\f -> ((src f, dst f), [f])) $ 
        map recordToFlow allRecords


-- IO Out Phase

writeFlows :: String -> [Flow] -> IO ()
writeFlows fileName []    = putStrLn "No Flows to Write"
writeFlows fileName flows = writeFile fileName $ 
                                unlines $ 
                                map (\x -> fst x ++ " " ++ snd x) $
                                zip [show x | x <- [1,2..]] $ 
                                map show $
                                filter (\f -> (src f /= (-1) && dst f /= (-1))) flows

