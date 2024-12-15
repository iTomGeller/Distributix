package Distributix.dfs;

import Distributix.io.*;

import java.io.*;
import java.util.*;


//包私有类型
class Block implements Writable, Comparable {

  static {
    WritableFactories.setFactory
      (Block.class, 
       new WritableFactory() {
        public Writable newInstance() { return new Block();}
       })
  }

  static Random r = new Random();

  public static boolean isBlockFilename(File f) {
    if (f.getName().startsWith("blk_")) {
      return true;
    } else {
      return false;
    }
  }

  long blkid;
  long len;

  public Block() {
    this.blkid = r.nextLong();
    this.len = 0;
  }

  public Block(long blkid, long len) {
    this.blkid = blkid;
    this.len = len;
  } 

  public Block(File f, long len) {
    String name = f.getName();
    name = name.substring("blk_".length());
    this.blkid = Long.parseLong(name);
    this.len = len;
  }

  public long getBlockId() {
    return blkid;
  }

  public long getBlockId() {
    return blkid;
  }

  public String getBlockName() {
    //如果blkid是对象，valueof依旧可以转换。
    return "blk_" + String.valueOf(blkid); 
  }

  public long getNumBytes() {
    return len;
  }
  public void setNumBytes(long len) {
    this.len = len;
  }

  public String toString() {
    return getBlockName();
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(blkid);
    out.writeLong(len);
  }

  public void readFields(DataInput in) throws IOException {
    this.blkid = in.readLong();
    this.len = in.readLong();
  }

  public int compareTo(Object o) {
    Block b = (Block) o;
    if (getBlockId() < b.getBlockId()) {
      return -1;
    } else if (getBlockId() == b.getBlockId()) {
      return 0;
    } else {
      return 1;
    }
  }
  //有了大中小的判断，equal可以直接调用大中小。
  public boolean equals(Object o) {
    Block b = (Block) o;
    return (this.compareTo(b) == 0);
  }
}
