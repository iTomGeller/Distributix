package Distributix.dfs;

import Distributix.io.*;

import java.io.*;
import java.util.*;

class DatanodeInfo implements Writable, Comparable {
  static {                                      // register a ctor
      WritableFactories.setFactory
        (DatanodeInfo.class,
         new WritableFactory() {
           public Writable newInstance() { return new DatanodeInfo(); }
         });
    }

    private UTF8 name;
    private long capacityBytes, remainingBytes, lastUpdate;
    private volatile TreeSet blocks;


    public DatanodeInfo() {
        this(new UTF8(), 0, 0);
    }

   /**
    * @param name hostname:portNumber as UTF8 object.
    */
    public DatanodeInfo(UTF8 name) {
        this.name = name;
        this.blocks = new TreeSet();
        updateHeartbeat(0, 0);        
    }

   /**
    * @param name hostname:portNumber as UTF8 object.
    */
    public DatanodeInfo(UTF8 name, long capacity, long remaining) {
        this.name = name;
        this.blocks = new TreeSet();
        updateHeartbeat(capacity, remaining);
    }

   /**
    */
    public void updateBlocks(Block newBlocks[]) {
        blocks.clear();
        for (int i = 0; i < newBlocks.length; i++) {
            blocks.add(newBlocks[i]);
        }
    }

   /**
    */
    public void addBlock(Block b) {
        blocks.add(b);
    }

    /**
     */
    public void updateHeartbeat(long capacity, long remaining) {
        this.capacityBytes = capacity;
        this.remainingBytes = remaining;
        this.lastUpdate = System.currentTimeMillis();
    }

    /**
     * @return hostname:portNumber as UTF8 object.
     */
    public UTF8 getName() {
        return name;
    }

    /**
     * @return hostname and no :portNumber as UTF8 object.
     */
    public UTF8 getHost() {
        String nameStr = name.toString();
        int colon = nameStr.indexOf(":");
        if (colon < 0) {
            return name;
        } else {
            return new UTF8(nameStr.substring(0, colon));
        }
    }
    public String toString() {
        return name.toString();
    }
    public Block[] getBlocks() {
        return (Block[]) blocks.toArray(new Block[blocks.size()]);
    }
    public Iterator getBlockIterator() {
        return blocks.iterator();
    }
    public long getCapacity() {
        return capacityBytes;
    }
    public long getRemaining() {
        return remainingBytes;
    }
    public long lastUpdate() {
        return lastUpdate;
    }


    public int compareTo(Object o) {
        DatanodeInfo d = (DatanodeInfo) o;
        return name.compareTo(d.getName());
    }

    /////////////////////////////////////////////////
    // Writable
    /////////////////////////////////////////////////
    /**
     */
    public void write(DataOutput out) throws IOException {
        name.write(out);
        out.writeLong(capacityBytes);
        out.writeLong(remainingBytes);
        out.writeLong(lastUpdate);

        /**
        out.writeInt(blocks.length);
        for (int i = 0; i < blocks.length; i++) {
            blocks[i].write(out);
        }
        **/
    }

    /**
     */
    public void readFields(DataInput in) throws IOException {
        this.name = new UTF8();
        this.name.readFields(in);
        this.capacityBytes = in.readLong();
        this.remainingBytes = in.readLong();
        this.lastUpdate = in.readLong();
    }
}