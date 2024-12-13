package Distributix.util;

import java.util.jar.*;

public class PrintJarMainClass {
  public static void main(String[] args) {
    try {
      JarFile jar_file = new JarFile(args[0]);
      if (jar_file != null) {
        Manifest manifest = jar_file.getManifest();
        if (manifest != null) {
          String value = manifest.getMainAttributes().getValue("Main-Class");
          if (value != null) {
            System.out.println(value.replaceAll("/", "."));
            return;
          }
        }
      }
    } catch (Throwable e) {
      //ignore it
    }
    System.out.println("UNKNOWN");
    System.exit(-1);
  }
}
