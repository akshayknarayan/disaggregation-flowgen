/* ProcessTrace.scala */
/* Akshay Narayan 30 Oct 2015 */
package org.apache.spark.examples

import org.apache.log4j.Logger

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.io.Source

import java.io.File

class Flow(var id: Long, var time: Double, var src: Int, var dst: Int, var length: Long, var cat: String, var node: Int, var addr: Int) extends Serializable
class NicFlow(var id: Long, var start: Double, var end: Double, var src: Int, var dst: Int, var length: Long) extends Serializable

object LogHolder extends Serializable {
    @transient lazy val log = Logger.getLogger(getClass.getName)
}

object ProcessTrace {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Process Trace")
        val sc = new SparkContext(conf)
        val numPartitions = args.head.toInt
        val traces = args.tail.map(s => if (s.last == '/') s.dropRight(1) else s)

        LogHolder.log.warn(s"input traces: ${traces.toList.reduce(_ + " " + _)}")
        traces.map(processTrace(_, sc, numPartitions)).map(_ match {
            case (traceName: String, nf: RDD[NicFlow], fs: RDD[Flow]) => {
                LogHolder.log.warn(s"writing chart data: ${traceName}")
                fig6abcd(traceName, nf, fs)
                fig6ef(traceName, nf, fs)
            }
        })

        sc.stop()
    }

    def write(fn: String, out:String) = {
        val pw = new java.io.PrintWriter(new File(fn))
        try pw.write(out) finally pw.close()
    }

    def fig6abcd(traceName: String, nf: RDD[NicFlow], fs: RDD[Flow]) {
        // number of flows
        val nfCount = nf.count
        write(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6c-pdis", (s"${nfCount}\n"))
        val fsCount = fs.count
        write(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6c-dis", (s"${fsCount}\n"))
        
        // flow size distribution (CDF)
        // a for fs (dis), b for nf (predis)
        val nfSizes = nf.map(_.length)
        nfSizes.sortBy(a => a).zipWithIndex.map(_ match {
            case (size: Long, idx: Long) => s"${size} ${idx.toDouble / nfCount}"
        }).saveAsTextFile(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6b-pdis")

        val fsSizes = fs.map(_.length)
        fsSizes.sortBy(a => a).zipWithIndex.map(_ match {
            case (size: Long, idx: Long) => s"${size} ${idx.toDouble / fsCount}"
        }).saveAsTextFile(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6a-dis")

        // traffic volume
        val nfVolume = nfSizes.reduce(_ + _)
        write(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6d-pdis", (s"${nfVolume}\n"))
        val fsVolume = fsSizes.reduce(_ + _)
        write(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6d-dis", (s"${fsVolume}\n"))
    }
    def fig6ef(traceName: String, nf: RDD[NicFlow], fs: RDD[Flow]) {
        // temporal distribution
        // e for fs (dis), f for nf (predis)

        val slotDuration = 0.001 // 100 ms
        nf.sortBy(_.start).map(f => ((f.start / slotDuration).toLong, f.length)).reduceByKey(_ + _).map(_ match {
            case (time: Long, volume: Long) => s"${time} ${volume * 8}"
        }).saveAsTextFile(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6f-pdis")

        fs.sortBy(_.time).map(f => ((f.time / slotDuration).toLong, f.length)).reduceByKey(_ + _).map(_ match {
            case (time: Long, volume: Long) => s"${time} ${volume * 8}"
        }).saveAsTextFile(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/fig6e-dis")
    }

    def processTrace(tracedir: String, sc: SparkContext, numPartitions: Int): (String, RDD[NicFlow], RDD[Flow]) = {
        val traceFiles = new File(tracedir).listFiles.toList
        val filename = """(.*/)+(.*?)""".r
        val filename(_, traceName) = tracedir
        LogHolder.log.warn(s"tracedir: ${tracedir} trace: ${traceName}")
        var localMem = 0.0
        var addrMap = null: Map[String, Int]
        traceFiles.map(_.toString).filter(_.endsWith(".txt")).map(path => {
            val filename(_, file) = path 
            file match {
                case "traceinfo.txt" => {
                    val rmem = """(.*)(RmemGb: )(\d\d\.\d\d)(.*)""".r
                    val rmem(_, _, rm, _) = Source.fromFile(path).getLines.drop(1).next()
                    localMem = 1.0 - rm.toDouble / 29.45 
                } 
                case "addr_mapping.txt" => 
                    addrMap = Source.fromFile(path).getLines.map(line => {
                        val sp = line.split(" ")
                        (sp(2) -> sp(0).toInt)
                    }).toMap
            }
        }) 
        LogHolder.log.warn(localMem)
        LogHolder.log.warn(addrMap)
        val dataFiles = traceFiles.map(_.toString).filter(!_.endsWith(".txt"))
        LogHolder.log.warn(dataFiles.reduce(_ + " " + _))
        var nicFlows = processNicFlows(sc.textFile(dataFiles.filter(
            path => {
                val filename(dir, file) = path
                file == "nic"
            }
        ).head), addrMap)

        nicFlows.persist()

        LogHolder.log.warn(s"nic flows: ${nicFlows.count}")

        nicFlows.map(
            nf => {
                s"${nf.id} ${"%.6f" format nf.start} ${nf.src} ${nf.dst} ${nf.length}"
            }
        ).coalesce(1).saveAsTextFile(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/nic")

        LogHolder.log.warn("Wrote nic trace: " + s"./results/${traceName}/nic")

        val tr = dataFiles.filter(
            path => {
                val filename(dir, file) = path
                file != "nic"
            }
        ).head
        val filename(dir, _) = tr 
        
        val all = sc.textFile(tr, numPartitions)
        //all.persist

        LogHolder.log.warn("read " + all.count + "flows")

        val mem = processMemFlows(all.filter(_.contains("mem")))

        LogHolder.log.warn("finished memory flows starting disk")
        
        val disk = processDiskFlows(all.filter(_.contains("disk")), nicFlows.repartition(numPartitions), sc)
        
        LogHolder.log.warn("finished disk flows")
        
        val combined = mem.union(disk).sortBy(_.time)

        LogHolder.log.warn(s"Total number of flows ${combined.count}")

        val startTime = combined.take(1)(0).time
        val allFlows = combined.map(
            f => {
                new Flow(
                    id = f.id,
                    time = f.time - startTime,
                    src = f.src,
                    dst = f.dst,
                    length = f.length,
                    cat = f.cat,
                    node = f.node, 
                    addr = f.addr
                )
            }
        )

        allFlows.persist()

        allFlows.zipWithIndex().map(
            tup => {
                val id = tup._2
                val f = tup._1
                s"$id ${"%.6f" format f.time} ${f.src} ${f.dst} ${f.length} ${f.cat} ${f.node}-${f.addr}"
            }
        ).coalesce(1).saveAsTextFile(s"/scratch/akshay/disaggregation-flowgen/flowgen-spark/results/${traceName}/flows")

        return (traceName, nicFlows, allFlows)
    }

    def processMemFlows(memFile: RDD[String]):RDD[Flow] = {
        // find the range of memory addresses so we can hash to nodes in the next stage
        val memRange = memFile.flatMap(
            line => {
                line.split(" ").toList match {
                    case node :: "mem" :: _ :: _ :: start_addr :: runlength :: Nil => 
                        List((node.toInt -> start_addr.toInt), (node.toInt -> (start_addr.toInt + runlength.toInt)))
                    case Nil => throw new Exception("failed match in mem")
                }
            }
        ).aggregateByKey(
            (-1, Int.MaxValue)
        )(
            (u, addr) => {
                (Math.max(addr, u._1), Math.min(addr, u._2))
            },
            (u1, u2) => (Math.max(u1._1, u2._1), Math.min(u1._2, u2._2))
        ).collect.toMap

        // 
        val memFlows = memFile.flatMap(
            line => {
                line.split(" ").toList match {
                    case n :: "mem" :: batchid :: time :: start_addr :: runlength :: Nil => {
                        val node = n.toInt
                        val start = start_addr.toInt.abs
                        (start to (start + runlength.toInt - 1.toInt)).map(
                            addr => {
                                val h = (addr / memRange(node)._1.toDouble).toInt * memRange.keys.iterator.length + memRange.keys.iterator.length
                                if (start_addr(0) == '-') new Flow(
                                        id = batchid.toInt,
                                        time = time.toDouble / 1e6,
                                        src = node,
                                        dst = h,
                                        length = 4096l, // mem flow length always 4KB at this stage
                                        cat = "memWrite",
                                        node = node,
                                        addr = addr
                                    )
                                else new Flow(
                                        id = batchid.toInt,
                                        time = time.toDouble / 1e6,
                                        src = h,
                                        dst = node,
                                        length = 4096l, // mem flow length always 4KB at this stage
                                        cat = "memRead",
                                        node = node,
                                        addr = addr
                                    )
                            }
                        )
                    }
                    case _ => throw new Exception("mem trace format")
                }
            }
        ).keyBy(f => (f.id, f.src, f.dst)).reduceByKey( //aggregate the batches together
            (f1: Flow, f2: Flow) => 
                new Flow(
                    id = 0, 
                    time = Math.min(f1.time, f2.time), 
                    src = f1.src, 
                    dst = f1.dst, 
                    length = f1.length + f2.length, 
                    cat = f1.cat, 
                    node = f1.node, 
                    addr = Math.min(f1.addr, f2.addr)
                )
        ).map(_._2)
        LogHolder.log.warn("finished memory flows")

        //collapse flows
        return collapse(memFlows)
    }

    def collapse(flows: RDD[Flow]): RDD[Flow] = {
        LogHolder.log.warn("collapsing")

        //collapse flows that are within 50us at the same src and dst
        flows.keyBy(
            flow => {
                (Math.floor(flow.time / 50e-6).toLong, flow.src, flow.dst)
            }
        ).reduceByKey(
            (f1: Flow, f2: Flow) => 
                new Flow(
                    id = 0, 
                    time = Math.min(f1.time, f2.time), 
                    src = f1.src, 
                    dst = f1.dst, 
                    length = f1.length + f2.length, 
                    cat = f1.cat, 
                    node = f1.node, 
                    addr = Math.min(f1.addr, f2.addr)
                )
        ).map(_._2).sortBy(_.time)
    }

    def processNicFlows(nicFile: RDD[String], nicHostMapping: Map[String, Int]): RDD[NicFlow] = {
        var nfs = nicFile.map(
            line => {
                // start, end, src, dst, length, _
                line.split(" ").toList match {
                    case start :: end :: src :: dst :: length :: _ :: Nil => {
                        val hostname = "^(.*\\.ec2\\.internal).*$".r
                        val hostname(s) = src
                        val hostname(d) = dst 
                        new NicFlow(
                            id = 0, 
                            start = start.toDouble, 
                            end = end.toDouble, 
                            src = nicHostMapping.getOrElse(s, -1), 
                            dst = nicHostMapping.getOrElse(d, -1), 
                            length = length.toLong
                        )
                    }
                    case _ => throw new Exception("nic flow format") 
                }
            }
        ).filter(f => f.src != -1 && f.dst != -1).sortBy(_.start)
        val firstStart = nfs.take(1)(0).start
        nfs.zipWithIndex.map(
            tup => {
                val nf = tup._1: NicFlow
                val id = tup._2: Long
                new NicFlow(
                    id     = id, 
                    start  = nf.start - firstStart,
                    end    = nf.end - firstStart,
                    src    = nf.src,
                    dst    = nf.dst,
                    length = nf.length
                )
            }
        )
    }

    def processDiskFlows(diskFile: RDD[String], nicFlows: RDD[NicFlow], sc: SparkContext): RDD[Flow] = {
        if (nicFlows == null) {
            throw new Exception("nicFlows is null")
        }

        // get address range for hashing to node
        val diskRange = diskFile.map(
            line => {
                val sp = line.split(" ").toList
                sp match {
                    case x :: xs => (x.toInt -> xs)
                    case Nil => throw new Exception("disk null line") 
                }
            }
        ).aggregateByKey(
            (-1, Int.MaxValue)
        )(
            (u, v) => {
                val addr = v(2).toInt
                (Math.max(addr, u._1), Math.min(addr, u._2))
            },
            (u1, u2) => (Math.max(u1._1, u2._1), Math.min(u1._2, u2._2))
        ).collect.toMap

        // read input into RDD[Flow]
        val diskFlows = diskFile.map(
            line => 
                line.split(" ").toList match {
                    case node :: "disk" :: timestamp :: address :: length :: _ :: rw :: Nil => {
                        val addr = address.toInt
                        val h = (addr / diskRange(node.toInt)._1.toDouble).toInt * 3 + diskRange.keys.iterator.length * 2
                        val ts = timestamp.toDouble
                        rw match {
                            //write: node -> h
                            case _ if rw.take(1) == "R" => 
                                new Flow(
                                    id = 0, 
                                    time = ts, 
                                    src = node.toInt, 
                                    dst = h, 
                                    length = length.toLong * 4096, 
                                    cat = "diskRead", 
                                    node = node.toInt, 
                                    addr = addr
                                )
                            //read: h -> node
                            case _ if rw.take(1) == "W" => 
                                new Flow(
                                    id = 0, 
                                    time = ts, 
                                    src = h,
                                    dst = node.toInt, 
                                    length = length.toLong * 4096, 
                                    cat = "diskRead", 
                                    node = node.toInt, 
                                    addr = addr
                                )
                        }
                    }
                    case _ => throw new Exception("disk flows format") 
                }
        )

        val writeFlows = diskFlows.filter(_.cat == "diskWrite")
        val readFlows = diskFlows.filter(_.cat == "diskRead")
        readFlows.persist

        // do source attribution from nic flows
        val readFlows_bv = sc.broadcast(readFlows.collect.toList)
        val matched = nicFlows.flatMap(
            (nf: NicFlow) => {
                val matching = readFlows_bv.value.filter(
                    (df: Flow) => 
                        df.src == nf.dst && 
                        df.time >= nf.start && 
                        df.time <= nf.end && 
                        df.length <= nf.length
                )
                if (!matching.isEmpty) {
                    matching.map(df => (df -> nf))
                }
                else None
            }
        ).filter(_ != None): RDD[(Flow, NicFlow)]
        val uniq = matched.union(readFlows.map(x => (x -> null))).reduceByKey(
            (nf1: NicFlow, nf2: NicFlow) => if (nf1 != null) nf1 else nf2
        )

        val readFlowsNoMap = uniq.filter(_._2 == null).map(_._1)
        LogHolder.log.warn("num disk reads: " + readFlows.count)
        LogHolder.log.warn("unmatched: " + readFlowsNoMap.count)
        val readFlowsMapped = uniq.filter(_._2 != null).map(t => (t._2 -> (t._1, t._2.length))).aggregateByKey(
            // tuple of selected disk flows, sum of sizes of selected disk flows, total nic flow size
            (List(): List[Flow], 0l, 0l)
        )(
            // u: tuple of (Flows selected for this key (NicFlow), sum of selected flows, nic flow size)
            // v: tuple of (Next flow, nic flow size)
            (u: (List[Flow], Long, Long), v: (Flow, Long)) => {
                if (u._2 <= v._2) (u._1 :+ v._1, u._2 + v._1.length, v._2)
                else (u._1, u._2, v._2)
            },
            (u1: (List[Flow], Long, Long), u2: (List[Flow], Long, Long)) => {
                if (u1._3 != u2._3) throw new Exception("non-matching total nic flow size")
                else if (u1._2 + u2._2 <= u1._3) (u1._1 ++ u2._1, u1._2 + u2._2, u1._3)
                else if (u1._2 == u1._3) u1
                else {
                    var target = u1._3 - u1._2
                    val toAdd = u2._1.takeWhile(
                        f => {
                            target -= f.length
                            target > 0
                        }
                    )
                    (u1._1 ++ toAdd, u1._3, u1._3)
                }
            }
        ).flatMap(
            t => (for (f <- t._2._1) yield (f -> t._1))
        ).map(
            t => {
                t._1.dst = t._2.src
                t._1
            }
        )

        return collapse(writeFlows.union(readFlowsNoMap).union(readFlowsMapped).sortBy(_.time))
    }
}
