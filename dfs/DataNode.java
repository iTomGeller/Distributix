package Distributix.dfs;

import Distributix.ipc.*;
import Distributix.conf.*;
import Distributix.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import javax.imageio.IIOException;


public class DataNode implements FSConstants, Runnable {
  public static final Logger LOG = LogFormatter.getLogger("Distributix.dfs.DataNode");

  public static InetSocketAddress createSocketAddr(String s) throws IOException {
    String target = s;
    int colonIndex = target.indexOf(":");
    if (colonIndex < 0) {
      throw RuntimeException("Not a host:port pair: " + s);
    }
    String host = target.substring(0, colonIndex);
    int port = Integer.parseInt(target.substring(colonIndex + 1));

    return new InetSocketAddress(host, port);
  }

  private static Vector subThreadList = null;//为什么用static？
  DatanodeProtocol namenode;
  FSDataset data;
  String localName;
  boolean shouldRun = true;
  Vector receivedBlockList = new Vector();
  int xmitsInProgress = 0;
  Daemon dataXceiveServer = null;
  long blockReportInterval;
  private long datanodeStartupPeriod;
  private Configuration fConf;

  public DataNode(Configuration conf, String datadir) throws IOException {
    this(InetAddress.getLocalHost().getHostName(),
         new File(datadir),
         createSocketAddr(conf.get("fs.default.name", "local")), conf);
  }

  public DataNode(String machineName, File datadir, InetSocketAddress nameNodeAddr, Configuration conf) throws IOException{
    this.namenode = (DatanodeProtocol) RPC.getProxy(DatanodeProtocol.class, nameNodeAddr, conf);
    this.data = new FSDataset(datadir, conf);

    ServerSocket ss = null;
    int tmpPort = conf.getInt("dfs.datanode.port", 50010);
    while (ss == null) {
      try {
          ss = new ServerSocket(tmpPort);
          LOG.info("Opened server at " + tmpPort);
      } catch (IOException ie) {
          LOG.info("Could not open server at " + tmpPort + ", trying new port");
          tmpPort++;
      }
    }
    this.localName = machineName + ":" + tmpPort;
    this.dataXceiveServer = new Daemon(new DataXceiveServer(ss));
    this.dataXceiveServer.start();

    long blockReportIntervalBasis =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.blockReportInterval =
      blockReportIntervalBasis - new Random().nextInt((int)(blockReportIntervalBasis/10));
    this.datanodeStartupPeriod =
      conf.getLong("dfs.datanode.startupMsec", DATANODE_STARTUP_PERIOD);
  }

  public String getNamenode() {
  //return namenode.toString();
	return "<namenode>";
  }

  void shutdown() {
    this.shouldRun = false;
    ((DataXceiveServer) this.dataXceiveServer.getRunnable()).kill();
    try {
        this.dataXceiveServer.join();
    } catch (InterruptedException ie) {
    }
  }

  public void offerService() throws IOException {
    long wakeups = 0;
    long lastHeartbeat = 0, lastBlockReport = 0;
    long sendStart = System.currentTimeMillis();
    int heartbeatsSent = 0;
    LOG.INFO("using BLOCKREPORT_INTERVAL of " + blockReportInterval + "msec");

    while (shouldRun) {
      long now = System.currentTimeMillis();
      //只对receivedBlockList加锁
      synchronized (receivedBlockList) {
        //heartbeat --- blockreport --- sendstart
        if (now - lastHeartbeat > HEARTBEAT_INTERVAL) {
          namenode.sendHeartbeat(localName, data.getCapacity(), data.getRemaining());
          //LOG.info("Just sent heartbeat, with name " + localName);
          lastHeartbeat = now;
		    }
      }

      //定时获取要删除的block
      if (now - lastBlockReport > blockReportInterval) {
        Block[] toDelete = namenode.blockReport(localName, data.getBlockReport());
        data.invalidate(toDelete);
        lastBlockReport = now;
        continue;
		  }

      //datanode收到block之后通知namenode，需要把receivedBlockList拷贝到blockarray然后清空
      if (receivedBlockList.size() > 0) {
        Block[] blockArray = (Block[]) receivedBlockList.toArray(new Block[receivedBlockList.size()]);
        receivedBlockList.removeAllElements();
        namenode.blockReceived(localName, blockArray);
      }

      if (now - sendStart > datanodeStartupPeriod) {
        //周期性轮询是否要transfer block
		    BlockCommand cmd = namenode.getBlockwork(localName, xmitsInProgress);
		    if (cmd != null && cmd.transferBlocks()) {

          Block blocks[] = cmd.getBlocks();
          DatanodeInfo xferTargets[][] = cmd.getTargets();
          
          for (int i = 0; i < blocks.length; i++) {
            if (!data.isValidBlock(blocks[i])) {
              String errStr = "Can't send invalid block " + blocks[i];
              LOG.info(errStr);
              namenode.errorReport(localName, errStr);
              break;
            } else {
              if (xferTargets[i].length > 0) {
                LOG.info("Starting thread to transfer block " + blocks[i] + " to " + xferTargets[i]);
                new Daemon(new DataTransfer(xferTargets[i], blocks[i])).start();
            }
          }
        }
      } else if (cmd != null && cmd.invalidateBlocks()) {
        data.invalidate(cmd.getBlocks());
      }
      }

      long waitTime = HEARTBEAT_INTERVAL - (now - lastHeartbeat);
      if (waitTime > 0 && receivedBlockList.size() == 0) {
        try {
          receivedBlockList.wait(waitTime);
        } catch (InterruptedException ie) {
        }
      }
    }
}

  class DataXceiveServer implements Runnable {
    boolean shouldListen = true;
    ServerSocket ss;
    //socket的开关都可能会引起IOException
    public DataXceiveServer(ServerSocket ss) {
        this.ss = ss;
    }

    public void run() {
      try {
        while (shouldListen) {
          Socket s = ss.accept();
          new Daemon(new Dataxceiver(s)).start();;
        }
        ss.close();
      } catch (IOException ie) {
          LOG.info("Exiting DataXceiveServer due to " + ie.toString());
      }
    }

    public void kill() {
      this.shouldListen = false;
      try {
        this.ss.close();
      } catch (IOException iex) {
      }
    }
  }

  class DataXceiver implements Runnable {
    Socket s;
    public DataXceiver(Socket s) {
      this.s = s;
    }

    public void run() {
      try {
        DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        try {
            byte op = (byte) in.read();
            if (op == OP_WRITE_BLOCK) {
                //
                // Read in the header
                //
                DataOutputStream reply = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                try {
                    boolean shouldReportBlock = in.readBoolean();
                    Block b = new Block();
                    b.readFields(in);
                    int numTargets = in.readInt();
                    if (numTargets <= 0) {
                        throw new IOException("Mislabelled incoming datastream.");
                    }
                    DatanodeInfo targets[] = new DatanodeInfo[numTargets];
                    for (int i = 0; i < targets.length; i++) {
                        DatanodeInfo tmp = new DatanodeInfo();
                        tmp.readFields(in);
                        targets[i] = tmp;
                    }
                    byte encodingType = (byte) in.read();
                    long len = in.readLong();

                    //
                    // Make sure curTarget is equal to this machine
                    //
                    DatanodeInfo curTarget = targets[0];

                    //
                    // Track all the places we've successfully written the block
                    //
                    Vector mirrors = new Vector();

                    //
                    // Open local disk out
                    //
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(data.writeToBlock(b)));
                    InetSocketAddress mirrorTarget = null;
                    try {
                        //
                        // Open network conn to backup machine, if 
                        // appropriate
                        //
                        DataInputStream in2 = null;
                        DataOutputStream out2 = null;
                        if (targets.length > 1) {
                            // Connect to backup machine
                            mirrorTarget = createSocketAddr(targets[1].getName().toString());
                            try {
                                Socket s2 = new Socket();
                                s2.connect(mirrorTarget, READ_TIMEOUT);
                                s2.setSoTimeout(READ_TIMEOUT);
                                out2 = new DataOutputStream(new BufferedOutputStream(s2.getOutputStream()));
                                in2 = new DataInputStream(new BufferedInputStream(s2.getInputStream()));

                                // Write connection header
                                out2.write(OP_WRITE_BLOCK);
                                out2.writeBoolean(shouldReportBlock);
                                b.write(out2);
                                out2.writeInt(targets.length - 1);
                                for (int i = 1; i < targets.length; i++) {
                                    targets[i].write(out2);
                                }
                                out2.write(encodingType);
                                out2.writeLong(len);
                            } catch (IOException ie) {
                                if (out2 != null) {
                                    try {
                                        out2.close();
                                        in2.close();
                                    } catch (IOException out2close) {
                                    } finally {
                                        out2 = null;
                                        in2 = null;
                                    }
                                }
                            }
                        }

                        //
                        // Process incoming data, copy to disk and
                        // maybe to network.
                        //
                        try {
                            boolean anotherChunk = len != 0;
                            byte buf[] = new byte[BUFFER_SIZE];

                            while (anotherChunk) {
                                while (len > 0) {
                                    int bytesRead = in.read(buf, 0, (int)Math.min(buf.length, len));
                                    if (bytesRead < 0) {
                                      throw new EOFException("EOF reading from "+s.toString());
                                    }
                                    if (bytesRead > 0) {
                                        try {
                                            out.write(buf, 0, bytesRead);
                                        } catch (IOException iex) {
                                            shutdown();
                                            throw iex;
                                        }
                                        if (out2 != null) {
                                            try {
                                                out2.write(buf, 0, bytesRead);
                                            } catch (IOException out2e) {
                                                //
                                                // If stream-copy fails, continue 
                                                // writing to disk.  We shouldn't 
                                                // interrupt client write.
                                                //
                                                try {
                                                    out2.close();
                                                    in2.close();
                                                } catch (IOException out2close) {
                                                } finally {
                                                    out2 = null;
                                                    in2 = null;
                                                }
                                            }
                                        }
                                        len -= bytesRead;
                                    }
                                }

                                if (encodingType == RUNLENGTH_ENCODING) {
                                    anotherChunk = false;
                                } else if (encodingType == CHUNKED_ENCODING) {
                                    len = in.readLong();
                                    if (out2 != null) {
                                        out2.writeLong(len);
                                    }
                                    if (len == 0) {
                                        anotherChunk = false;
                                    }
                                }
                            }

                            if (out2 == null) {
                                LOG.info("Received block " + b + " from " + s.getInetAddress());
                            } else {
                                out2.flush();
                                long complete = in2.readLong();
                                if (complete != WRITE_COMPLETE) {
                                    LOG.info("Conflicting value for WRITE_COMPLETE: " + complete);
                                }
                                LocatedBlock newLB = new LocatedBlock();
                                newLB.readFields(in2);
                                DatanodeInfo mirrorsSoFar[] = newLB.getLocations();
                                for (int k = 0; k < mirrorsSoFar.length; k++) {
                                    mirrors.add(mirrorsSoFar[k]);
                                }
                                LOG.info("Received block " + b + " from " + s.getInetAddress() + " and mirrored to " + mirrorTarget);
                            }
                        } finally {
                            if (out2 != null) {
                                out2.close();
                                in2.close();
                            }
                        }
                    } finally {
                        try {
                            out.close();
                        } catch (IOException iex) {
                            shutdown();
                            throw iex;
                        }
                    }
                    data.finalizeBlock(b);

                    // 
                    // Tell the namenode that we've received this block 
                    // in full, if we've been asked to.  This is done
                    // during NameNode-directed block transfers, but not
                    // client writes.
                    //
                    if (shouldReportBlock) {
                        synchronized (receivedBlockList) {
                            receivedBlockList.add(b);
                            receivedBlockList.notifyAll();
                        }
                    }

                    //
                    // Tell client job is done, and reply with
                    // the new LocatedBlock.
                    //
                    reply.writeLong(WRITE_COMPLETE);
                    mirrors.add(curTarget);
                    LocatedBlock newLB = new LocatedBlock(b, (DatanodeInfo[]) mirrors.toArray(new DatanodeInfo[mirrors.size()]));
                    newLB.write(reply);
                } finally {
                    reply.close();
                }
            } else if (op == OP_READ_BLOCK || op == OP_READSKIP_BLOCK) {
                //
                // Read in the header
                //
                Block b = new Block();
                b.readFields(in);

                long toSkip = 0;
                if (op == OP_READSKIP_BLOCK) {
                    toSkip = in.readLong();
                }

                //
                // Open reply stream
                //
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                try {
                    //
                    // Write filelen of -1 if error
                    //
                    if (! data.isValidBlock(b)) {
                        out.writeLong(-1);
                    } else {
                        //
                        // Get blockdata from disk
                        //
                        long len = data.getLength(b);
                        DataInputStream in2 = new DataInputStream(data.getBlockData(b));
                        out.writeLong(len);

                        if (op == OP_READSKIP_BLOCK) {
                            if (toSkip > len) {
                                toSkip = len;
                            }
                            long amtSkipped = 0;
                            try {
                                amtSkipped = in2.skip(toSkip);
                            } catch (IOException iex) {
                                shutdown();
                                throw iex;
                            }
                            out.writeLong(amtSkipped);
                        }

                        byte buf[] = new byte[BUFFER_SIZE];
                        try {
                            int bytesRead = 0;
                            try {
                                bytesRead = in2.read(buf);
                            } catch (IOException iex) {
                                shutdown();
                                throw iex;
                            }
                            while (bytesRead >= 0) {
                                out.write(buf, 0, bytesRead);
                                len -= bytesRead;
                                try {
                                    bytesRead = in2.read(buf);
                                } catch (IOException iex) {
                                    shutdown();
                                    throw iex;
                                }
                            }
                        } catch (SocketException se) {
                            // This might be because the reader
                            // closed the stream early
                        } finally {
                            try {
                                in2.close();
                            } catch (IOException iex) {
                                shutdown();
                                throw iex;
                            }
                        }
                    }
                    LOG.info("Served block " + b + " to " + s.getInetAddress());
                } finally {
                    out.close();
                }
            } else {
                while (op >= 0) {
                    System.out.println("Faulty op: " + op);
                    op = (byte) in.read();
                }
                throw new IOException("Unknown opcode for incoming data stream");
            }
        } finally {
            in.close();
        }
    } catch (IOException ie) {
      LOG.log(Level.WARNING, "DataXCeiver", ie);
    } finally {
        try {
            s.close();
        } catch (IOException ie2) {
        }
    }
}
}

  class DataTransfer implements Runnable {
          InetSocketAddress curTarget;
          DatanodeInfo targets[];
          Block b;
          byte buf[];

          /**
           * Connect to the first item in the target list.  Pass along the 
           * entire target list, the block, and the data.
           */
          public DataTransfer(DatanodeInfo targets[], Block b) throws IOException {
              this.curTarget = createSocketAddr(targets[0].getName().toString());
              this.targets = targets;
              this.b = b;
              this.buf = new byte[BUFFER_SIZE];
          }

          /**
           * Do the deed, write the bytes
           */
          public void run() {
        xmitsInProgress++;
              try {
                  Socket s = new Socket();
                  s.connect(curTarget, READ_TIMEOUT);
                  s.setSoTimeout(READ_TIMEOUT);
                  DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                  try {
                      long filelen = data.getLength(b);
                      DataInputStream in = new DataInputStream(new BufferedInputStream(data.getBlockData(b)));
                      try {
                          //
                          // Header info
                          //
                          out.write(OP_WRITE_BLOCK);
                          out.writeBoolean(true);
                          b.write(out);
                          out.writeInt(targets.length);
                          for (int i = 0; i < targets.length; i++) {
                              targets[i].write(out);
                          }
                          out.write(RUNLENGTH_ENCODING);
                          out.writeLong(filelen);

                          //
                          // Write the data
                          //
                          while (filelen > 0) {
                              int bytesRead = in.read(buf, 0, (int) Math.min(filelen, buf.length));
                              out.write(buf, 0, bytesRead);
                              filelen -= bytesRead;
                          }
                      } finally {
                          in.close();
                      }
                  } finally {
                      out.close();
                  }
                  LOG.info("Transmitted block " + b + " to " + curTarget);
              } catch (IOException ie) {
                LOG.log(Level.WARNING, "Failed to transfer "+b+" to "+curTarget, ie);
              } finally {
      xmitsInProgress--;
        }
          }
      }

      public void run() {
        LOG.info("Starting DataNode in: "+data.data);
        while (shouldRun) {
            try {
                offerService();
            } catch (Exception ex) {
                LOG.info("Exception: " + ex);
              if (shouldRun) {
                LOG.info("Lost connection to namenode.  Retrying...");
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
              }
            }
        }
      LOG.info("Finishing DataNode in: "+data.data);
    }


     public static void run(Configuration conf) throws IOException {
        String[] dataDirs = conf.getStrings("dfs.data.dir");
        subThreadList = new Vector(dataDirs.length);
        for (int i = 0; i < dataDirs.length; i++) {
          DataNode dn = makeInstanceForDir(dataDirs[i], conf);
          if (dn != null) {
            Thread t = new Thread(dn, "DataNode: "+dataDirs[i]);
            t.setDaemon(true); // needed for JUnit testing
            t.start();
            subThreadList.add(t);
          }
        }
    }

  /** Start datanode daemons.
   * Start a datanode daemon for each comma separated data directory
   * specified in property dfs.data.dir and wait for them to finish.
   * If this thread is specifically interrupted, it will stop waiting.
   */
  private static void runAndWait(Configuration conf) throws IOException {
    run(conf);

    //  Wait for sub threads to exit
    for (Iterator iterator = subThreadList.iterator(); iterator.hasNext();) {
      Thread threadDataNode = (Thread) iterator.next();
      try {
        threadDataNode.join();
      } catch (InterruptedException e) {
        if (Thread.currentThread().isInterrupted()) {
          // did someone knock?
          return;
        }
      }
    }
  }

  static DataNode makeInstanceForDir(String dataDir, Configuration conf) throws IOException {
    DataNode dn = null;
    File data = new File(dataDir);
    data.mkdirs();
    if (!data.isDirectory()) {
      LOG.warning("Can't start DataNode in non-directory: "+dataDir);
      return null;
    } else {
      dn = new DataNode(conf, dataDir);
    }
    return dn;
  }

  public String toString() {
    return "DataNode{" +
        "data=" + data +
        ", localName='" + localName + "'" +
        ", xmitsInProgress=" + xmitsInProgress +
        "}";
  }

    /**
     */
    public static void main(String args[]) throws IOException {
        LogFormatter.setShowThreadIDs(true);
        runAndWait(new Configuration());
    }


}