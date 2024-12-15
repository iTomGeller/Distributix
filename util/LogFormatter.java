package Distributix.util;

import java.util.logging.*;
import java.io.*;
import java.text.*;
import java.util.Date;


/** Prints just the date and the log message. */

public class LogFormatter extends Formatter {
  private static final String FORMAT = "yyMMdd HHmmss";
  private static final String NEWLINE = System.getProperty("line.separator");

  //不是static 每个logFormatter都不一样
  private final Date date = new Date();
  private final SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);

  private static boolean loggedSevere = false;

  private static boolean showTime = true;
  private static boolean showThreadIDs = false;

  static {
    Handler[] handlers = LogFormatter.getLogger("").getHandlers();
    //给每个logger安装logformatter
    for (int i = 0; i < handlers.length; i++) {
      handlers[i].setFormatter(new LogFormatter());
      handlers[i].setLevel(Level.FINEST);
    }
  }

  public static Logger getLogger(String name) {
    return Logger.getLogger(name);
  }

  public static void showTime(boolean showtime) {
    LogFormatter.showTime = showtime;
  }

  public static void setShowThreadIDs(boolean ShowThreadIDs) {
    LogFormatter.showThreadIDs = ShowThreadIDs;
  }

  //不声明成static因为每一个logger都配一个formatter
  public synchronized String format(LogRecord record) {
    StringBuffer buffer = new StringBuffer();

    if (showTime) {
      date.setTime(record.getMillis());
      formatter.format(date, buffer, new FieldPosition(0));
    }

    if (showThreadIDs) {
      buffer.append(" ");
      buffer.append(record.getThreadID());
    }

    if (record.getLevel() == Level.SEVERE) {
      buffer.append(" SEVERE");
      loggedSevere = true;
    }

    buffer.append(" ");
    buffer.append(formatMessage(record));

    buffer.append(NEWLINE);

    if (record.getThrown() != null) {
      try {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        pw.close();
        buffer.append(sw.toString());
      } catch (Exception ex) {
      }
    }
    return buffer.toString();
  }

  public static boolean hasLoggedSevere() {
    return loggedSevere;
  }

  public static PrintStream getLogStream(final Logger logger,
                                         final Level level) {
    return new PrintStream(new ByteArrayOutputStream() {
      private int scan = 0;

      private boolean hasNewline() {
        for (; scan < count; scan++) {
          if (buf[scan] == '\n') {
            return true;
          }
        }
        return false;
      }

      public void flush() throws IOException {
        if (!hasNewline()) 
          return;
        logger.log(level, toString().trim());
        reset();
        scan = 0;
      }
    }, true);                                        
  }

}
