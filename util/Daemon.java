package Distributix.util;

public class Daemon extends Thread {
  {
  setDaemon(true);
  }
  
  Runnable runnable = null;

  public Daemon() {
    super();//调用父类thread的constructor
  }

  public Daemon(Runnable runnable) {
    super(runnable);
    this.runnable = runnable;
    this.setName(((Object)runnable).toString());//to——string存在于object里面
  }
  
  public Runnable getRunnable() {
    return runnable;
  }
}