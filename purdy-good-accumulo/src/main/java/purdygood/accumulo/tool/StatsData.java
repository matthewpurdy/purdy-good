package purdygood.accumulo.tool;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class StatsData {
  protected Map<Key, Value> ROWs                       = new TreeMap<Key, Value>();
  protected Map<Key, Value> ROW_COLFAMs                = new TreeMap<Key, Value>();
  protected Map<Key, Value> ROW_COLFAM_COLQUALs        = new TreeMap<Key, Value>();
  protected Map<Key, Value> ROW_COLFAM_COLQUAL_COLVISs = new TreeMap<Key, Value>();
  protected Map<Key, Value> COLFAMs                    = new TreeMap<Key, Value>();
  protected Map<Key, Value> COLFAM_COLQUALs            = new TreeMap<Key, Value>();
  protected Map<Key, Value> COLFAM_COLQUAL_COLVISs     = new TreeMap<Key, Value>();
  protected Map<Key, Value> COLQUALs                   = new TreeMap<Key, Value>();;
  protected Map<Key, Value> COLQUAL_COLVISs            = new TreeMap<Key, Value>();
  protected Map<Key, Value> COLVISs                    = new TreeMap<Key, Value>();
  
  protected void aggregateStats(Key key) {
    String row = key.getRow().toString();
    String cf  = key.getColumnFamily().toString();
    String cq  = key.getColumnQualifier().toString();
    String cv  = key.getColumnVisibility().toString();
    
    incrementCount(createKey(row, "", "", ""), ROWs);
    incrementCount(createKey(row, cf, "", ""), ROW_COLFAMs);
    incrementCount(createKey(row, cf, cq, ""), ROW_COLFAM_COLQUALs);
    incrementCount(createKey(row, cf, cq, cv), ROW_COLFAM_COLQUAL_COLVISs);
    incrementCount(createKey("", cf, "", ""),  COLFAMs);
    incrementCount(createKey("", cf, cq, ""),  COLFAM_COLQUALs);
    incrementCount(createKey("", cf, cq, ""),  COLFAM_COLQUAL_COLVISs);
    incrementCount(createKey("", "", cq, ""),  COLQUALs);
    incrementCount(createKey("", "", cq, cv),  COLQUAL_COLVISs);
    incrementCount(createKey("", "", "", cv),  COLVISs);
    
  }
  
  protected void incrementCount(Key key, Map<Key, Value> map) {
    Value value = map.get(key);
    if(value == null) {
      map.put(key, new Value("1".getBytes()));
    }
    else {
      String val = "" + (Long.parseLong(value.toString()) + 1);
      map.put(key, new Value(val.getBytes()));
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
  
  protected Key createKey(String row, String cf, String cq, String cv) {
    return new Key(row, cf, cq, cv);
  }
  
}
