package Distributix.dfs;

import java.io.*;

interface ClientProtocol {
  public LocatedBlock[] open(String src) throws IOException;

  public LocatedBlock create(String src, String clientName, String clientMachine, boolean overwrite) throws IOException;

  public void reportWrittenBlock(LocatedBlock b) throws IOException;

  public void abandonBlock(Block b, String src) throws IOException;

  public LocatedBlock addBlock(String src, String clientMachine) throws IOException;

  public void abandonFileInProgress(String src) throws IOException;

  public boolean complete(String src, String clientName) throws IOException;

  public boolean rename(String src, String dst) throws IOException;

  public boolean delete(String src) throws IOException;

  public boolean exists(String src) throws IOException;

  public boolean isDir(String src) throws IOException;

  public boolean mkdirs(String src) throws IOException;

  public DFSFileInfo[] getListing(String src) throws IOException;

  public String[][] getHints(String src, long start, long len) throws IOException;

  public boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException;

  public boolean releaseLock(String src, String clientName) throws IOException;

  public void renewLease(String clientName) throws IOException;

  public long[] getStats() throws IOException;

  public DatanodeInfo[] getDatanodeReport() throws IOException;
  
}
