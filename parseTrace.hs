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
    --let io_traces = map readTraceFile neededTraceFiles :: [IO BS.ByteString]
    traces <- mapM readTraceFile neededTraceFiles 
    
    --nodeInfos <- sequence $ zipWith fmap (map readNodeInfo neededTraceFiles) io_traces 


    let archs = ["rack-scale", "res-based"]
    let opts = 
            if (Char.toLower . head) alsoCollapse == 'y'
            then ["plain", "combined", "timeonly"]
            else ["plain"]

--
--    let scrf = 
--            lineate $
--            groupNodeInfo $
--            map (\(a, b) -> readNodeInfo a b) $ 
--            zip (map stripDirectory neededTraceFiles) traces
--
--    let flowsPerArch = 
--            map (\arch -> List.foldl' (++) [] $ zipWith (scrf arch) (map stripDirectory neededTraceFiles) (traces)) archs :: [[Flow]]

    putStrLn "hi"
    {-
    let perms = foldr (++) [] $ map (\a -> (map (\b -> (a,b)) opts)) archs
    let fns = map (\(a,b) -> (outprefix) ++ a ++ "_" ++ b ++ "_flows.txt") perms
    
    writeFlows ((outprefix) ++ "nic_flows.txt") (makeNicFlows $ fst records) 
    writeResults $ zip fns $ map (\(arch, opt) -> combineFlows (makeFlows nodeInfos (snd records) arch) opt) perms
    -}
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

mami = List.foldl' (\(ma, mi) x ->
        if x > ma
        then (x, mi)
        else
            if x < mi
            then (ma, x)
        else (ma, mi)
    ) (0, 9999999999)

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

readNodeInfo :: String -> BS.ByteString -> NodeInfo
readNodeInfo fileName contents
    | "-mem-" `List.isInfixOf` fileName = 
        let
            memGetAddrLine ~(_:_:addr:_) = fastReadInt addr
        in NodeInfo {nid = nodeId, memRange = (getRange memGetAddrLine), diskRange = 0, metaTime = 0}
    | "-disk-" `List.isInfixOf` fileName = 
        let
            diskGetAddrLine  ~(_:_:_:addr:_) = fastReadInt addr
        in NodeInfo {nid = nodeId, memRange = 0, diskRange = (getRange diskGetAddrLine), metaTime = 0}
    | "-meta-" `List.isInfixOf` fileName = 
        NodeInfo {nid = nodeId, memRange = 0, diskRange = 0, metaTime = (fastReadDouble (head (BS.lines contents))) / 1e6}
    | otherwise = 
        NodeInfo {nid = nodeId, memRange = 0, diskRange = 0, metaTime = 0}
    where
        nodeId = read [head fileName]
        
        getRange getAddrs = 
            let addrsRange = mami $ map getAddrs $ map BS.words $ BS.lines contents
            in  fst addrsRange - snd addrsRange

lineate :: [NodeInfo] -> String -> String -> BS.ByteString -> [Flow]
lineate infos model fileName contents = 
    map (combinedReadFlows infos model fileName) (map BS.words $ BS.lines contents)

combinedReadFlows :: [NodeInfo] -> String -> String -> [BS.ByteString] -> Flow
combinedReadFlows infos model fileName line 
    | isMem  = readMemoryFlow line 
    | isDisk = readDiskFlow line
    | otherwise = undefined
    where
        isMem = "-mem-" `List.isInfixOf` fileName
        isDisk = "-disk-" `List.isInfixOf` fileName
        nodeId = read [head fileName]
        numNodes = length infos
        thisNodeInfo = 
            fromMaybe (undefined) $
            List.find (\n -> nid n == nodeId) infos 

        readDiskFlow x = 
            let _:ts:_:addr:_:len:_:_:_:rws:_ = x
                rw = Char.toLower (BS.head rws) == 'r'
                addrNum = (fastReadInt addr) * 4096
                s = if rw then mapToNode nodeId addrNum else nodeId
                d = if rw then nodeId else mapToNode nodeId addrNum
            in  Flow {
                ftime = (fastReadDouble ts) + (metaTime thisNodeInfo),
                src = s,
                dst = d,
                size = (fastReadInt len) * 4096,
                ftag = (if rw then "diskRead" else "diskWrite"),
                faddr = addrNum
            }
        
        readMemoryFlow x = 
            let _:ts:addr:len:pgSize:_ = x
                fts = fastReadDouble ts
                addrNum = fastReadInt addr * 4096
                s = if fts < 0 then mapToNode nodeId addrNum else nodeId
                d = if fts < 0 then nodeId else mapToNode nodeId addrNum
            in  Flow {
                    ftime = (abs fts) / 1e6,
                    src = s,
                    dst = d,
                    size = fastReadInt len * fastReadInt pgSize,
                    ftag = if fts < 0 then "memRead" else "memWrite",
                    faddr = addrNum
                }

        mapToNode :: Int -> Int -> Int
        mapToNode node addr 
            | model == "rack-scale" && isMem  = truncate $ memNodeCase * (fromIntegral numNodes)
            | model == "rack-scale" && isDisk = truncate $ diskNodeCase * (fromIntegral numNodes)
            | model == "res-based"  && isMem  = numNodes + (truncate $ memNodeCase * (fromIntegral numNodes))
            | model == "res-based"  && isDisk = 2 * numNodes + (truncate $ diskNodeCase * 3)
            where 
                memNodeCase = (fromIntegral addr) / (fromIntegral $ memRange thisNodeInfo) :: Double
                diskNodeCase = (fromIntegral addr) / (fromIntegral $ diskRange thisNodeInfo) :: Double


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

-- combine flows together

adjustTime :: [Flow] -> [Flow]
adjustTime flows = 
    let
        sortedFlows = List.sortBy (compare `on` ftime) flows
        startTime = (ftime . head) sortedFlows
    in  map (\f -> Flow {ftime = (ftime f - startTime), src = src f, dst = dst f, size = size f, ftag = ftag f, faddr = faddr f}) sortedFlows 

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

