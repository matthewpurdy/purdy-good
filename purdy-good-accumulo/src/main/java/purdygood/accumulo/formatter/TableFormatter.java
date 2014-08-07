package purdygood.accumulo.formatter;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.hadoop.io.Text;

public class TableFormatter implements Formatter {
  protected Iterator<Entry<Key,Value>> si;
  protected boolean doTimestamps;
  
  protected int rowPad = 20;
  protected int cfPad  = 20;
  protected int cqPad  = 20;
  protected int cvPad  = 20;
  protected int tsPad  = 20;
  protected int valPad = 20;
  
  protected boolean padChange = true;
  
  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    checkState(si, false);
    si = scanner.iterator();
    doTimestamps = printTimestamps;
  }
  
  public boolean hasNext() {
    checkState(si, true);
    return si.hasNext();
  }
  
  public String next() {
    checkState(si, true);
    return formatEntry(si.next(), doTimestamps);
  }
  
  public void remove() {
    checkState(si, true);
    si.remove();
  }
  
  protected void checkState(Iterator<Entry<Key,Value>> si, boolean expectInitialized) {
    if(expectInitialized && si == null)
      throw new IllegalStateException("Not initialized");
    if(!expectInitialized && si != null)
      throw new IllegalStateException("Already initialized");
  }
  
  public String formatEntry(Entry<Key, Value> entry) {
    return formatEntry(entry.getKey(), entry.getValue(), false);
  }
  
  public String formatEntry(Entry<Key, Value> entry, boolean showTimestamps) {
    return formatEntry(entry.getKey(), entry.getValue(), showTimestamps);
  }
  
  public String formatEntry(Key key, Value value) {
    return formatEntry(key, value, false);
  }
  
  public String formatEntry(Key key, Value value, boolean showTimestamps) {
    StringBuilder ret = new StringBuilder();
    
    Key currentKey = key;
    String row = currentKey.getRow().toString();
    String cf  = currentKey.getColumnFamily().toString();
    String cq  = currentKey.getColumnQualifier().toString();
    String cv  = currentKey.getColumnVisibility().toString();
    String ts  = Long.toString(currentKey.getTimestamp());
    String val = value.toString();
    
    if(rowPad < row.length()) {
      rowPad = row.length() + 2;
      padChange = true;
    }
    if(cfPad < cf.length()) {
      cfPad = cf.length() + 2;
      padChange = true;
    }
    if(cqPad < cq.length()) {
      cqPad = cq.length() + 2;
      padChange = true;
    }
    if(cvPad < cv.length()) {
      cvPad = cv.length() + 2;
      padChange = true;
    }
    if(tsPad < ts.length()) {
      tsPad = ts.length() + 2;
      padChange = true;
    }
    if(valPad < val.length()) {
        valPad = val.length() + 2;
        padChange = true;
    }
    
    if(padChange) {
      appendHeader(ret, showTimestamps);
      padChange = false;
    }
    // append row
    appendString(ret, "|").append(pad(row, rowPad)).append("|");
    
    // append column family
    appendString(ret, pad(cf, cfPad)).append("|");
    
    // append column qualifier
    appendString(ret, pad(cq, cqPad)).append("|");
    
    // append visibility expression
    appendString(ret, pad(cv, cqPad)).append("|");
    
    // append timestamp
    if(showTimestamps) {
    	appendString(ret, pad(ts, tsPad)).append("|");
    }
    
    // append value
    if(value != null && value.getSize() > 0) {
      appendString(ret, pad(convertValueToString(value), valPad));
      appendString(ret, "|");
    }
    else {
    	appendString(ret, createPad(" ", valPad)).append("|");
    }
    
    return ret.toString();
  }
  
  protected String createPad(String character, int pad) {
    StringBuilder ret = new StringBuilder(pad);
    for(int i = 0; i < pad; i++) {
      ret.append(character);
    }
    return ret.toString();
  }
  
  protected String pad(String value, int pad) {
    StringBuilder ret = new StringBuilder(64);
    
    ret.append(" ").append(value);
    for(int i = value.length() + 2; i <= pad; i++) {
      ret.append(" ");
    }
    
    return ret.toString();
  }
  
  protected StringBuilder appendHeader(StringBuilder sb, boolean showTimestamps) {
    sb.append("+");sb.append(createPad("-", rowPad));
    sb.append("+");sb.append(createPad("-", cfPad));
    sb.append("+");sb.append(createPad("-", cqPad));
    sb.append("+");sb.append(createPad("-", cvPad));
    
    if(showTimestamps) {
      sb.append("+");sb.append(createPad("-", tsPad));
    }
    
    sb.append("+");sb.append(createPad("-", valPad));sb.append("+\n");
    
    sb.append("|");sb.append(pad("Row Id",           rowPad));
    sb.append("|");sb.append(pad("Column Family",    cfPad));
    sb.append("|");sb.append(pad("Column Qualifier", cqPad));
    sb.append("|");sb.append(pad("Column Visiblity", cvPad));
    
    if(showTimestamps) {
      sb.append("|");sb.append(pad("Timestamp", tsPad));
    }
    
    sb.append("|");sb.append(pad("Value", valPad));sb.append("|\n");
    
    sb.append("+");sb.append(createPad("-", rowPad));
    sb.append("+");sb.append(createPad("-", cfPad));
    sb.append("+");sb.append(createPad("-", cqPad));
    sb.append("+");sb.append(createPad("-", cvPad));

    if(showTimestamps) {
      sb.append("+");sb.append(createPad("-", tsPad));
    }
    
    sb.append("+");sb.append(createPad("-", valPad));sb.append("+\n");
    
    return sb;
  }
  
  protected StringBuilder appendText(StringBuilder sb, Text t) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength());
  }
  
  protected StringBuilder appendString(StringBuilder sb, String s) {
    return appendBytes(sb, s.getBytes(), 0, s.length());
  }
  
  protected StringBuilder appendValue(StringBuilder sb, Value value) {
    return appendBytes(sb, value.get(), 0, value.get().length);
  }
  
  protected StringBuilder appendBytes(StringBuilder sb, byte ba[], int offset, int len) {
    for(int i = 0; i < len; i++) {
      int c = 0xff & ba[offset + i];
      if (c == '\\')
        sb.append("\\\\");
      else if (c >= 32 && c <= 126)
        sb.append((char) c);
      else
        sb.append("\\x").append(String.format("%02X", c));
    }
    return sb;
  }
  
  protected String convertValueToString(Value value) {
    return convertBytesToString(value.get(), 0, value.get().length);
  }
  
  protected String convertBytesToString(byte ba[], int offset, int len) {
    StringBuilder ret = new StringBuilder(len);
    for(int i = 0; i < len; i++) {
      int c = 0xff & ba[offset + i];
      if (c == '\\')
        ret.append("\\\\");
      else if (c >= 32 && c <= 126)
        ret.append((char) c);
      else
        ret.append("\\x").append(String.format("%02X", c));
    }
    return ret.toString();
  }
  
  public Iterator<Entry<Key,Value>> getScannerIterator() {
    return si;
  }
}

