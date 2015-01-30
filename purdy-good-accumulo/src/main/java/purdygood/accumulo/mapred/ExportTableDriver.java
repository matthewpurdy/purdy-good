package purdygood.accumulo.mapred;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExportTableDriver extends Configured implements Tool {
	private Options options;
	private Option instanceNameOpt;
	private Option zookeepersOpt;
	private Option inputTableNameOpt;
	private Option outputPathOpt;
	private Option passwordOpt;
	private Option usernameOpt;
	
	@Override
	public int run(String[] args) throws Exception {
		instanceNameOpt    = new Option("n", "instanceName",    true, "accumulo instance");
		zookeepersOpt      = new Option("z", "zookeepers",      true, "comma separtated list of zookeepers");
		usernameOpt        = new Option("u", "username",        true, "username");
		passwordOpt        = new Option("p", "password",        true, "password");
		inputTableNameOpt  = new Option("i", "inputTableName",  true, "accumulo input table");
		outputPathOpt      = new Option("o", "outputPath",      true, "hdfs output path");
		
		options = new Options();
		
		options.addOption(instanceNameOpt);
		options.addOption(zookeepersOpt);
		options.addOption(usernameOpt);
		options.addOption(passwordOpt);
		options.addOption(inputTableNameOpt);
		options.addOption(outputPathOpt);
		
		Parser p = new BasicParser();
	    
	    CommandLine commandLine = p.parse(options, args);
	    
	    String instanceName = commandLine.getOptionValue(instanceNameOpt.getOpt());
		String zookeepers   = commandLine.getOptionValue(zookeepersOpt.getOpt());
	    String username     = commandLine.getOptionValue(usernameOpt.getOpt());
	    String password     = commandLine.getOptionValue(passwordOpt.getOpt());
	    String tableName    = commandLine.getOptionValue(inputTableNameOpt.getOpt());
		String outputPath   = commandLine.getOptionValue(outputPathOpt.getOpt());
	    
	    if(instanceName == null || zookeepers == null || tableName == null || outputPath == null  || username == null || password == null) {
	    	throw new Exception(usage());
	    }
	    
	    Authorizations authorizations = new Authorizations("public");
		//1.4.4 => Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
		Job job = new Job(getConf());
		job.setJobName(this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
	    job.setJarByClass(this.getClass());
	    job.setInputFormatClass(AccumuloInputFormat.class);
		
		//1.4.4 => AccumuloInputFormat.setZooKeeperInstance(job, instanceName, zookeepers);
		AccumuloInputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers));
		
		//1.4.4 => AccumuloInputFormat.setInputInfo(job, username, password.getBytes(), tableName, authorizations);
		AccumuloInputFormat.setConnectorInfo(job, username, new PasswordToken(password.getBytes()));
		AccumuloInputFormat.setScanAuthorizations(job, authorizations);
		AccumuloInputFormat.setInputTableName(job, tableName);
		
		job.setMapperClass(ExportTableMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		
		TextOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		return exitCode;
	}
	
	public static String usage() {
		String usage1 = "purdygood.accumulo.mapreduce.ExportTableDriver -n <instance_name> -z <zookeepers> -u <username> -p <password> -i <input_table_name> -o <output_path>";
		String usage2 = "purdygood.accumulo.mapreduce.ExportTableDriver --instanceName <instance_name> --zookeepers <zookeepers> --username <username> --password <password> --inputTableName <input_table_name> --outputPath <output_path>";
		
		return "\n##################################\nusage using short options => " + usage1 + "\nusage using long options  => " + usage2 + "\n##################################\n";
		
	}//end static method usage
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExportTableDriver(), args);
		System.exit(res);
	}
}
