import System.Environment
import Numeric

fastRead :: String -> Float 
fastRead s = case readFloat s of [(n, "")] -> n

main = do
    args <- getArgs
    putStrLn $ handleArgs args

handleArgs :: [String] -> String
handleArgs [] = "Usage: ./calculate_oracle <cut through ('ct'/'nct')> <header size> <packet size> <last hop bw> <propagation delay per link> <flow size pkts>"
handleArgs (ct:rest) = 
    let 
        (hdr_size:pkt_size:bw:pd:fsize:_) = map fastRead rest
    in show $ oracle ct hdr_size pkt_size bw pd fsize

oracle :: String -> Float -> Float -> Float -> Float -> Float -> (Float, Float)
oracle "ct" hdr_size pkt_size bw pd fsize = 
    let
        core_bw = bw * 4
        
        pkt_td = 8 * pkt_size / bw
        
        hdr_td      = 8 * hdr_size / bw
        hdr_td_core = 8 * hdr_size / core_bw

        hops2 = (2 * (2 * pd + 2 * hdr_td                  ) + fsize * pkt_td) * 1e6
        hops4 = (2 * (4 * pd + 2 * hdr_td + 2 * hdr_td_core) + fsize * pkt_td) * 1e6
    in (hops2, hops4)

oracle "nct" hdr_size pkt_size bw pd fsize = 
    let
        core_bw = bw * 4
        
        pkt_td      = 8 * pkt_size / bw
        pkt_td_core = 8 * pkt_size / core_bw
        
        hdr_td      = 8 * hdr_size / bw
        hdr_td_core = 8 * hdr_size / core_bw

        hops2 = (2 * pd + (fsize + 1) * pkt_td + 2 * pd + 2 * hdr_td) * 1e6
        hops4 = (4 * pd + 2 * pkt_td_core + (fsize + 1) * pkt_td + 4 * pd + 2 * hdr_td + 2 * hdr_td_core) * 1e6
    in (hops2, hops4)

