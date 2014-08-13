package purdygood.accumulo.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

public class ScanTool {
  
  protected static final String prop_CONFIGURATION_FILE       = "c";
  protected static final String prop_ACCUMULO_INSTANCE        = "i";
  protected static final String prop_ACCUMULO_ZOOKEEPERS      = "z";
  protected static final String prop_ACCUMULO_USERNAME        = "u";
  protected static final String prop_ACCUMULO_PASSWORD        = "p";
  protected static final String prop_CONFIGURATION_FILE_LONG  = "configuration_file";
  protected static final String prop_ACCUMULO_INSTANCE_LONG   = "accumulo_instance";
  protected static final String prop_ACCUMULO_ZOOKEEPERS_LONG = "accumulo_zookeepers";
  protected static final String prop_ACCUMULO_USERNAME_LONG   = "accumulo_username";
  protected static final String prop_ACCUMULO_PASSWORD_LONG   = "accumulo_password";
  protected static final String command_COMMAND               = "command";
  protected static final String command_BYE                   = "bye";
  protected static final String command_COUNT                 = "count";
  protected static final String command_EXIT                  = "exit";
  protected static final String command_HELP                  = "help";
  protected static final String command_HISTORY               = "history";
  protected static final String command_QUIT                  = "quit";
  protected static final String command_SCAN                  = "scan";
  protected static final String command_TABLES                = "tables";
  protected static final String command_FIELD_ROW             = "row";
  protected static final String command_FIELD_CF              = "cf";
  protected static final String command_FIELD_CQ              = "cq";
  protected static final String command_FIELD_LINE            = "line";
  protected static final String command_FIELD_TABLE           = "table";
  protected static final String command_FIELD_THRESHOLD       = "threshold";
  //protected static final String command_FIELD_OUTPUT          = "output";
  
  protected static Logger log                   = Logger.getLogger(ScanTool.class);
  protected static Connector connector          = null;
  protected static Authorizations userAuths     = null;
  protected static TableOperations tableOps     = null;
  protected static Formatter formatter          = null;
  protected static Map<Integer, String> history = null;
  
  protected static Map<String, String> parseArgs(String[] args) throws FileNotFoundException, ParseException, IOException {
    Options options         = buildOptions();
    Parser parser           = new PosixParser();
    CommandLine commandLine = parser.parse(options, args);
    return parseProperties(commandLine.getOptions());
  }
  
  public static Map<String, String> parseProperties(Option[] options) throws FileNotFoundException, IOException {
    Map<String, String> ret = new TreeMap<String, String>();
    
    String configPath = null;
    for(Option option: options) {
      String opt = option.getOpt();
      if(opt.equals(prop_CONFIGURATION_FILE)) {
        configPath = option.getValue();
      }
      else {
        ret.put(opt, option.getValue());
      }
    }
    log.info("getting properties from configuration file => " + configPath);
    Properties properties = new Properties();
    if(configPath != null) {
      properties.load(new FileInputStream(configPath));
      for(Entry<Object, Object> entry: properties.entrySet()) {
        ret.put((String)entry.getKey(), (String)entry.getValue());
      }
    }
    
    log.info("############################");
    for(Entry<String, String> entry: ret.entrySet()) {
      log.info("ScanTool Properties: key => " + entry.getKey() + ", value => " + entry.getValue());
    }
    log.info("############################");
    
    return ret;
  }
  
  public static Options buildOptions() {
    Options ret = new Options();
    ret.addOption(prop_CONFIGURATION_FILE,  prop_CONFIGURATION_FILE_LONG,  true, "properties file holding all the args");
    ret.addOption(prop_ACCUMULO_INSTANCE,   prop_ACCUMULO_INSTANCE_LONG,   true, "accumulo instance name");
    ret.addOption(prop_ACCUMULO_ZOOKEEPERS, prop_ACCUMULO_ZOOKEEPERS_LONG, true, "accumulo zookeepers");
    ret.addOption(prop_ACCUMULO_USERNAME,   prop_ACCUMULO_USERNAME_LONG,   true, "accumulo username");
    ret.addOption(prop_ACCUMULO_PASSWORD,   prop_ACCUMULO_PASSWORD_LONG,   true, "accumulo password");
    
    return ret;
  }
  
  protected static void initialize(Map<String, String> properties) throws AccumuloException, AccumuloSecurityException {
    System.out.println("instance   => " + properties.get(prop_ACCUMULO_INSTANCE_LONG));
    System.out.println("zookeepers => " + properties.get(prop_ACCUMULO_ZOOKEEPERS_LONG));
    System.out.println("username   => " + properties.get(prop_ACCUMULO_USERNAME_LONG));
    System.out.println("password   => " + properties.get(prop_ACCUMULO_PASSWORD_LONG));
    Instance instance = new ZooKeeperInstance(properties.get(prop_ACCUMULO_INSTANCE_LONG), properties.get(prop_ACCUMULO_ZOOKEEPERS_LONG));
    connector         = instance.getConnector(properties.get(prop_ACCUMULO_USERNAME_LONG), properties.get(prop_ACCUMULO_PASSWORD_LONG).getBytes());
    userAuths         = connector.securityOperations().getUserAuthorizations(properties.get(prop_ACCUMULO_USERNAME_LONG));
    tableOps          = connector.tableOperations();
    formatter         = new Formatter();
    history           = new TreeMap<Integer, String>();
  }
  
  protected static String prompt() {
    return "\nscantool> ";
  }
  
  protected static void console() throws IOException, TableNotFoundException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    
    int lineCount = 0;
    Map<String, Map<String, String>> parseLine = null;
    String line = null;
    for(;;) {
      writer.write(prompt());writer.flush();
      
      line = reader.readLine();
      history.put(++lineCount, line.trim());
      parseLine = parseLine(line);
      if(isExitCommand(getCommand(parseLine))) {
        break;
      }
      else {
        writer.write(doWork(line));writer.flush();
      }
      
    }
  }
  
  protected static boolean isExitCommand(String command) {
    boolean ret = false;
    if(command.equalsIgnoreCase(command_BYE) || command.equalsIgnoreCase(command_EXIT) || command.equalsIgnoreCase(command_QUIT)) {
      ret = true;
    }
    return ret;
  }
  
  protected static String getCommand(Map<String, Map<String, String>> parseLine) {
    return (String)parseLine.keySet().toArray()[0];
  }
  
  protected static Map<String, Map<String, String>> parseLine(String line) {
    Map<String, Map<String, String>> ret = new TreeMap<String, Map<String, String>>();
    
    line                       = line.trim();
    int commandEndDex          = line.indexOf(" ");
    String command             = "";
    Map<String, String> fields = new TreeMap<String, String>();
    
    if(commandEndDex <= 0) {
      command = line;
      fields.put(command_COMMAND, command);
      ret.put(command, fields);
    }
    else if(commandEndDex > 0 ) {
      command = line.substring(0, commandEndDex);
      fields.put(command_COMMAND, command);
      try {
        //commandValue field1 => field1Value | field2 => field2Value
        String[] parts = line.substring(commandEndDex).split("\\|");
        for(String part: parts) {
          String[] nameValue = part.split("=>");
          fields.put(nameValue[0].trim(), nameValue[1].trim());
        }
        ret.put(command, fields);
      }
      catch(Exception e) {
        command = "error";
        fields = new TreeMap<String, String>();
        fields.put("line", line);
        fields.put("exception", e.toString());
        fields.put("message", e.getMessage());
        ret.put(command, fields);
      }
    }
    
    return ret;
  }
  
  protected static String doWork(String line) {
    StringBuilder ret = new StringBuilder(128);
    Map<String, Map<String, String>> parseLine = parseLine(line);
    for(String command: parseLine.keySet()) {
      ret.append(doWork(command, parseLine.get(command)));
    }
    return ret.toString();
  }
  
  protected static String doWork(String command, Map<String, String> parseFields) {
    StringBuilder ret = new StringBuilder(128);
    
    try {
    if(command.equalsIgnoreCase(command_HELP)) {
      ret.append(doWorkHelp(parseFields));
    }
    else if(command.equalsIgnoreCase(command_SCAN)) {
      ret.append(doWorkScan(parseFields));
    }
    else if(command.equalsIgnoreCase(command_COUNT)) {
      ret.append(doWorkCount(parseFields));
    }
    else if(command.equalsIgnoreCase(command_HISTORY)) {
      ret.append(doWorkHistory(parseFields));
    }
    else if(command.equalsIgnoreCase(command_TABLES)) {
      ret.append(doWorkTable(parseFields));
    }
    else {
      ret.append("command => |").append(command).append("|");
      for(Entry<String, String> entry: parseFields.entrySet()) {
        ret.append(", ").append(entry.getKey());
        ret.append(" => |").append(entry.getValue()).append("|");
      }
    }
    }
    catch(TableNotFoundException e) {
      ret.append("bad table name => ").append(e.getMessage());
    }
    
    return ret.toString();
  }
  
  protected static String doWorkHelp(Map<String, String> parseFields) {
    return help();
  }
  
  protected static String doWorkTable(Map<String, String> parseFields) throws TableNotFoundException {
    StringBuilder ret = new StringBuilder(1024);
    Map<String, String > tables = tableOps.tableIdMap();
    for(String table: tables.keySet()) {
      ret.append(table).append("\n");
    }
    return ret.toString();
  }
  
  protected static String doWorkHistory(Map<String, String> parseFields) throws TableNotFoundException {
    StringBuilder ret = new StringBuilder(1024);
    if(parseFields.size() == 1) {
      for(Entry<Integer, String> line: history.entrySet()) {
        ret.append(line.getKey()).append(" => ").append(line.getValue()).append("\n");
      }
    }
    else {
      String lineDex = parseFields.get(command_FIELD_LINE);
      if(lineDex != null) {
        int dex = Integer.parseInt(lineDex);
        if(dex < 0) {
          dex = history.size() + dex;
        }
        if(dex == 0 || dex > (history.size() - 1)) {
          dex = 1;
        }
        history.put(history.size(), history.get(dex));
        ret.append(doWork(history.get(dex)));
      }
    }
    
    return ret.toString();
  }
  
  protected static String doWorkScan(Map<String, String> parseFields) throws TableNotFoundException {
    StringBuilder ret = new StringBuilder(1024);
    
    String table = parseFields.get(command_FIELD_TABLE);
    String row   = parseFields.get(command_FIELD_ROW);
    String cf    = parseFields.get(command_FIELD_CF);
    String cq    = parseFields.get(command_FIELD_CQ);
    
    Scanner scanner             = connector.createScanner(table, userAuths);
    IteratorSetting iterSetting = new IteratorSetting(15, "filter", RegExFilter.class);
    configureScannerWithRegexFilter(scanner, iterSetting, row, cf, cq);
    
    formatter.reset();
    for(Entry<Key, Value> entry: scanner) {
      ret.append(formatter.formatEntry(entry)).append("\n");
    }
    scanner.clearScanIterators();
    scanner.clearColumns();
    
    return ret.toString();
  }
  
  protected static String doWorkCount(Map<String, String> parseFields) throws TableNotFoundException {
    StringBuilder ret = new StringBuilder(1024);
    
    String table      = parseFields.get(command_FIELD_TABLE);
    String row        = parseFields.get(command_FIELD_ROW);
    String cf         = parseFields.get(command_FIELD_CF);
    String cq         = parseFields.get(command_FIELD_CQ);
    String sThreshold = parseFields.get(command_FIELD_THRESHOLD);
    long threshold    = 0;
    
    try { threshold = Long.parseLong(sThreshold); } catch(Exception e) {}
    
    StatsData statsData         = new StatsData();
    Scanner scanner             = connector.createScanner(table, userAuths);
    IteratorSetting iterSetting = new IteratorSetting(15, "filter", RegExFilter.class);
    configureScannerWithRegexFilter(scanner, iterSetting, row, cf, cq);
    
    for(Entry<Key, Value> entry: scanner) {
      statsData.aggregateStats(entry.getKey());
    }
    
    formatter.reset();
    appendResults(statsData.ROWs, ret, threshold);
    appendResults(statsData.ROW_COLFAMs, ret, threshold);
    appendResults(statsData.ROW_COLFAM_COLQUALs, ret, threshold);
    appendResults(statsData.ROW_COLFAM_COLQUAL_COLVISs, ret, threshold);
    appendResults(statsData.COLFAMs, ret, threshold);
    appendResults(statsData.COLFAM_COLQUALs, ret, threshold);
    appendResults(statsData.COLFAM_COLQUAL_COLVISs, ret, threshold);
    appendResults(statsData.COLQUALs, ret, threshold);
    appendResults(statsData.COLQUAL_COLVISs, ret, threshold);
    appendResults(statsData.COLVISs, ret, threshold);
    appendResults(statsData.ROWs, ret, threshold);
    
    scanner.clearScanIterators();
    scanner.clearColumns();
    
    return ret.toString();
  }
  
  protected static void configureScannerWithRegexFilter(Scanner scanner, IteratorSetting iterSetting, String row, String cf, String cq) {
    boolean regex = false;
    if(row == null && cf == null && cq != null) {
      regex = true;
      RegExFilter.setRegexs(iterSetting, null, null, cq, null, false);
    }
    else if(row == null && cf != null && cq == null) {
      regex = true;
      RegExFilter.setRegexs(iterSetting, null, cf, null, null, false);
    }
    else if(row == null && cf != null && cq != null) {
      regex = true;
      RegExFilter.setRegexs(iterSetting, null, cf, cq, null, false);
    }
    else if(row != null && cf == null && cq == null) {
      regex = true;
      RegExFilter.setRegexs(iterSetting, row, null, null, null, false);
    }
    else if(row != null && cf == null && cq != null) {
      regex = true;
      RegExFilter.setRegexs(iterSetting, row, null, cq, null, false);
    }
    else if(row != null && cf != null && cq == null) {
      regex = true;
      RegExFilter.setRegexs(iterSetting, row, cf, null, null, false);
    }
    else if(row != null && cf != null && cq != null) {
      regex = true;
      RegExFilter.setRegexs(iterSetting, row, cf, cq, null, false);
    }
    
    if(regex) {
      scanner.addScanIterator(iterSetting);
    }
  }
  
  protected static void appendResults(Map<Key, Value> map, StringBuilder sb, long threshold) {
    for(Entry<Key, Value> entry: map.entrySet()) {
      if(Long.parseLong(entry.getValue().toString()) >= threshold) {
        sb.append(formatter.formatEntry(entry)).append("\n");
      }
    }
  }
  
  protected static String help() {
    StringBuilder ret = new StringBuilder(1024);
    
    ret.append("##################################################################\n");
    ret.append("ScanTool commands:\n");
    ret.append("bye     =>  exits the program\n");
    ret.append("exit    =>  exits the program\n");
    ret.append("help    =>  prints the help\n");
    ret.append("history =>  list history (no arguments) or runs history by line number: field options\n");
    ret.append("            line      => line number of command to run (negtive number is list in reverse)\n");
    ret.append("quit    =>  exits the program\n");
    ret.append("scan    =>  runs a scan against an accumulo table: field options:\n");
    ret.append("            table     => name of the accumulo table to run scan against\n");
    ret.append("            row       => a comma seperated list of row regex filters\n");
    ret.append("            cf        => a comma seperated list of column family regex filters\n");
    ret.append("            cq        => a comma seperated list of column qualifer regex filters\n");
    ret.append("count   =>  runs a scan against an accumulo table: field options:\n");
    ret.append("            table     => name of the accumulo table to run scan against\n");
    ret.append("            row       => a comma seperated list of row regex filters\n");
    ret.append("            cf        => a comma seperated list of column family regex filters\n");
    ret.append("            cq        => a comma seperated list of column qualifer regex filters\n");
    ret.append("            threshold => the minimum count of a key before it is returned to the console\n");
    ret.append("tables  =>  queries accumulo for a list of tables accumulo tables\n");
    ret.append("\n");
    ret.append("#####\n");
    ret.append("\n");
    ret.append("each command must be entered on one line in the following format:\n");
    ret.append("command => commandValue | field1 => field1Value | field2 => field2Value\n");
    ret.append("\n");
    ret.append("##################################################################\n");
    
    return ret.toString();
  }
  
  public static final void main(String[] args) throws FileNotFoundException, ParseException, IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Map<String, String> properties = parseArgs(args);
    for(Entry<String, String> prop: properties.entrySet()) {
      System.out.println("key => " + prop.getKey() + ", value => " + prop.getValue());
    }
    initialize(properties);
    console();
    
  }
}
