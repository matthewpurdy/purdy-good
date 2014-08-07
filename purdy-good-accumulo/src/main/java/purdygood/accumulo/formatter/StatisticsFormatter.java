package purdygood.accumulo.formatter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class StatisticsFormatter extends TableFormatter {
  protected Map<String, Long> ROWs                       = new HashMap<String, Long>();
  protected Map<String, Long> ROW_COLFAMs                = new HashMap<String, Long>();
  protected Map<String, Long> ROW_COLFAM_COLQUALs        = new HashMap<String, Long>();
  protected Map<String, Long> ROW_COLFAM_COLQUAL_COLVISs = new HashMap<String, Long>();
  protected Map<String, Long> COLFAMs                    = new HashMap<String, Long>();
  protected Map<String, Long> COLFAM_COLQUALs            = new HashMap<String, Long>();
  protected Map<String, Long> COLFAM_COLQUAL_COLVISs     = new HashMap<String, Long>();
  protected Map<String, Long> COLQUALs                   = new HashMap<String, Long>();
  protected Map<String, Long> COLQUAL_COLVISs            = new HashMap<String, Long>();
  protected Map<String, Long> COLVISs                    = new HashMap<String, Long>();
  
  @Override
  public String next() {
    Iterator<Entry<Key,Value>> si = super.getScannerIterator();
    checkState(si, true);
    while(si.hasNext()) {
      aggregateStats(si.next().getKey());
    }
    return outputStats();
  }
  
  protected void aggregateStats(Key key) {
    String row = key.getRow().toString();
    String cf  = key.getColumnFamily().toString();
    String cq  = key.getColumnQualifier().toString();
    String cv  = key.getColumnVisibility().toString();
    
    incrementCount(keyToCsv(row),             ROWs);
    incrementCount(keyToCsv(row, cf),         ROW_COLFAMs);
    incrementCount(keyToCsv(row, cf, cq),     ROW_COLFAM_COLQUALs);
    incrementCount(keyToCsv(row, cf, cq, cv), ROW_COLFAM_COLQUAL_COLVISs);
    incrementCount(keyToCsv("", cf),          COLFAMs);
    incrementCount(keyToCsv("", cf, cq),      COLFAM_COLQUALs);
    incrementCount(keyToCsv("", cf, cq),      COLFAM_COLQUAL_COLVISs);
    incrementCount(keyToCsv("", "", cq),      COLQUALs);
    incrementCount(keyToCsv("", "", cq, cv),  COLQUAL_COLVISs);
    incrementCount(keyToCsv("", "", "", cv),  COLVISs);
    
  }
  
  protected String outputStats() {
    StringBuilder ret = new StringBuilder(1024);
    
    for(Entry<String, Long> entry: ROWs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: ROW_COLFAMs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: ROW_COLFAM_COLQUALs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: ROW_COLFAM_COLQUAL_COLVISs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: COLFAMs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: COLFAM_COLQUALs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: COLFAM_COLQUAL_COLVISs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: COLQUALs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: COLQUAL_COLVISs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    for(Entry<String, Long> entry: COLVISs.entrySet()) {
      ret.append(formatEntry(entry.getKey(), entry.getValue())).append("\n");
    }
    
    return ret.toString();
  }
  
  protected String formatEntry(String key, Long value) {
    String[] keyPair = key.split(",");
    
    Key aKey     = new Key(new Text(extractValue(keyPair[0])), new Text(extractValue(keyPair[1])), 
                       new Text(extractValue(keyPair[2])), new Text(extractValue(keyPair[3])));
    Value aValue = new Value(value.toString().getBytes());
    
      return super.formatEntry(aKey, aValue);
    
    //return "keyPart.size => " + keyPart.length + "| key => " + key + " | value => " + value;
    
  }
  
  protected String extractValue(String nameValue) {
    String[] nameVal = nameValue.split("=");
    if(nameVal.length == 1) {
      return "";
    }
    else {
      return nameVal[1];
    }
  }
  
  protected void incrementCount(String key, Map<String, Long> map) {
    Long value = map.get(key);
    if(value == null) {
      map.put(key, 1L);
    }
    else {
      map.put(key, value + 1);
    }
  }
  
  protected String keyToCsv(String row) {
    return keyToCsv(row, "", "", "");
  }
  
  protected String keyToCsv(String row, String cf) {
    return keyToCsv(row, cf, "", "");
  }
  
  protected String keyToCsv(String row, String cf, String cq) {
    return keyToCsv(row, cf, cq, "");
  }
  
  protected String keyToCsv(String row, String cf, String cq, String cv) {
    StringBuilder ret = new StringBuilder(64);
    
    ret.append("row=").append(row).append(",cf=").append(cf).append(",cq=").append(cq).append(",cv=").append(cv);
    
    return ret.toString();
  }
}
