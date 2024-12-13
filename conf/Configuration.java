package Distributix.conf;

import java.util.*;
import java.net.URL;
import java.io.*;
import java.util.logging.Logger;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import Distributix.util.LogFormatter;

public class Configuration {
  private static final Logger LOG = 
    LogFormatter.getLogger("Distributix.conf.Configuration");
  
  private ArrayList defaultResources = new ArrayList<>();
  private ArrayList finalResources = new ArrayList<>();

  private Properties properties;
  private ClassLoader classLoader = 
    Thread.currentThread().getContextClassLoader();

  public Configuration() {
    defaultResources.add("");
    finalResources.add("");
  }

  public Configuration(Configuration other) {
    this.defaultResources = (ArrayList)other.defaultResources.clone();//clone返回object，需要转换
    this.finalResources = (ArrayList)other.finalResources.clone();
    if (other.properties != null) {
      this.properties = (Properties)other.properties.clone();
    }
  }

  public void addDefaultResource(String name) {
    addResource(defaultResources, name);
  }

  public void addDefaultResource(File file) {
    addResource(defaultResources, file);
  }

  public void addFinalResource(String name) {
    addResource(finalResources, name);
  }

  public void addFinalResource(File file) {
    addResource(finalResources, file);
  }

  //使用泛型object，使得兼容file，string类型。
  private synchronized void addResource(ArrayList resources, Object resource) {
    resources.add(resource);
    properties = null;
  }

  public Object getObject(String name) { return getProps().get(name);} 

  public void setObject(String name, Object value) {
    getProps().put(name, value);
  }

  public Object get(String name, Object defaultValue) {
    Object res = getObject(name);
    if (res != null) return res;
    else return defaultValue;
  }

  public String get(String name) { return getProps().getProperty(name);}

  public void set(String name, Object value) {
    getProps().setProperty(name, value.toString());
  }

  public String get(String name, String defaultValue) {
    return getProps().getProperty(name, defaultValue);
  }

  public int getInt(String name, int defaultValue) {
    String valueString = get(name);
    if (valueString == null) 
      return defaultValue;
    try {
      return Integer.parseInt(valueString);
    } catch(NumberFormatException e) {
      return defaultValue;
    }
  }

  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }

  public long getLong(String name, long defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  public float getFloat(String name, float defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = get(name);
    //1.字符串判断用equal 避免直接== 
    //2.同时把常量放前面，避免空指针
    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else return defaultValue;
  }

  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  public String[] getStrings(String name) {
    String valueString = get(name);
    if (valueString == null) 
      return null;
    StringTokenizer tokenizer = new StringTokenizer(valueString, ", \t\n\r\f");
    List values = new ArrayList();//用接口List可以转为toArray
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return (String[])values.toArray(new String[values.size()]);
  }

  public Class getClass(String name, Class defaultValue) {
    String valueString = get(name);
    if (valueString == null) 
      return defaultValue;
    try {
      return Class.forName(name)
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Class getClass(String propertyName, Class defaultValue, Class xface) {
    try {
      Class theClass = getClass(propertyName, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass + " not " + xface.getName());
      return theClass;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setClass(String propertyName, Class theClass, Class xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass+" not "+xface.getName());
    set(propertyName, theClass.getName());
  }

  public File getFile(String dirsProp, String path) throws IOException {
    String[] dirs = getStrings(dirsProp);
    int hashcode = path.hashCode();
    for (int i = 0; i < dirs.length; i++) {
      //确保结果为非负值
      int index = (hashcode + i & Integer.MAX_VALUE) % dirs.length;
      File file = new File(dirs[index], path).getAbsoluteFile();
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new IOException("No valid local directories in property: "+dirsProp);
  }

  //得到资源的路径
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }

  //有了inputstream就可以读数据
  public InputStream getConfResourceAsInputStreamm(String name) {
    try {
      URL url = getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }
      
      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  public Reader getConfResourceAsReader(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream());
    } catch (Exception e) {
      return null;
    }
  }

  private synchronized Properties getProps() {
    if (properties == null) {
      Properties newProps = new Properties();
      loadResources(newProps, defaultResources, false, false);
      loadResources(newProps, finalResources, true, true);
      properties = newProps;
    }
    return properties;
  }

  private void loadResources(Properties props,
                             ArrayList resources,
                             boolean reverse, boolean quiet) {
    ListIterator i = resources.listIterator(reverse ? resources.size() : 0);
    while (reverse ? i.hasPrevious() : i.hasNext()) {
      loadResource(props, reverse ? i.previous() : i.next(), quiet);
    }
  }

  private void loadResource(Properties properties, Object name, boolean quiet) {
    try {
      DocumentBuilder builder = 
        DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = null;

      if (name instanceof String) {
        URL url = getResource((String)name);
        if (url != null) {
          LOG.info("parsing " + url);
          doc = builder.parse(url.toString());
        } else if (name instanceof File) {
          File file = (File)name;
          if (file.exists()) {
            LOG.info("parsing " + file);
            doc = builder.parse(file);
          }
        }
      }

      if (doc == null) {
        if (quiet) 
          return;
        throw new RuntimeException(name + " not found");
      }

      Element root = doc.getDocumentElement();
      if (!"configuration".equals(root.getTagName())) 
        LOG.severe("bad conf file: top-level element not <configuration>");
      //nodelist专为xml文档设计，不可动态拓展。
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) 
          continue;
        Element prop = (Element)propNode;
        if (!"property".equals(prop.getTagName())) 
          LOG.warning("bad conf file: element not <property>");
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element)fieldNode;
          if ("name".equals(field.getTagName()))
            attr = ((Text)field.getFirstChild()).getData();
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = ((Text)field.getFirstChild()).getData();
        }
        if (attr != null && value != null)
          properties.setProperty(attr, value); 
      }

    } catch (Exception e) {
      LOG.severe("error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
  }

  public void write(OutputStream out) throws IOException {
    Properties properties = getProps();
    try {
      Document doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      Element conf = doc.createElement("configuration");
      doc.appendChild(conf);
      conf.appendChild(doc.createTextNode("\n"));
      for (Enumeration e = properties.keys(); e.hasMoreElements();) {
        String name = (String)e.nextElement();
        Object object = properties.get(name);
        String value = null;
        if(object instanceof String) {
          value = (String) object;
        }else {
          continue;
        }
        Element propNode = doc.createElement("property");
        conf.appendChild(propNode);
      
        Element nameNode = doc.createElement("name");
        nameNode.appendChild(doc.createTextNode(name));
        propNode.appendChild(nameNode);
      
        Element valueNode = doc.createElement("value");
        valueNode.appendChild(doc.createTextNode(value));
        propNode.appendChild(valueNode);

        conf.appendChild(doc.createTextNode("\n"));
      }
    
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Configuration: ");
    sb.append("defaults: ");
    toString(defaultResources, sb);
    sb.append("final: ");
    toString(finalResources, sb);
    return sb.toString();
  }

  private void toString(ArrayList resources, StringBuffer sb) {
    ListIterator i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(" , ");
      }
      Object obj = i.next();
      if (obj instanceof File) {
        //会自动调用File.toString()
        sb.append((File)obj);
      } else {
        sb.append((String)obj);
      }
    }
  }

    /** For debugging.  List non-default properties to the terminal and exit. */
    public static void main(String[] args) throws Exception {
      new Configuration().write(System.out);
    }
}
