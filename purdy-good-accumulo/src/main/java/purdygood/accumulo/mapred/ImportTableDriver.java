package purdygood.accumulo.mapred;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImportTableDriver extends Configured implements Tool {
	private Options options;
	private Option instanceNameOpt;
	private Option zookeepersOpt;
	private Option inputPathOpt;
	private Option outputTableNameOpt;
	private Option passwordOpt;
	private Option usernameOpt;
	
	@Override
	public int run(String[] args) throws Exception {
		instanceNameOpt    = new Option("n", "instanceName",    true, "accumulo instance");
		zookeepersOpt      = new Option("z", "zookeepers",      true, "comma separtated list of zookeepers");
		usernameOpt        = new Option("u", "username",        true, "username");
		passwordOpt        = new Option("p", "password",        true, "password");
		inputPathOpt       = new Option("i", "inputPath",       true, "hdfs input path");
		outputTableNameOpt = new Option("o", "outputTableName", true, "accumulo output table");
		
		options = new Options();
		
		options.addOption(instanceNameOpt);
		options.addOption(zookeepersOpt);
		options.addOption(usernameOpt);
		options.addOption(passwordOpt);
		options.addOption(inputPathOpt);
		options.addOption(outputTableNameOpt);
		
		Parser p = new BasicParser();
		
		CommandLine commandLine = p.parse(options, args);
		
		String instanceName = commandLine.getOptionValue(instanceNameOpt.getOpt());
		String zookeepers   = commandLine.getOptionValue(zookeepersOpt.getOpt());
		String username     = commandLine.getOptionValue(usernameOpt.getOpt());
		String password     = commandLine.getOptionValue(passwordOpt.getOpt());
		String inputPath    = commandLine.getOptionValue(inputPathOpt.getOpt());
		String tableName    = commandLine.getOptionValue(outputTableNameOpt.getOpt());
		
		if(instanceName == null || zookeepers == null || inputPath == null || tableName == null  || username == null || password == null) {
			throw new Exception(usage());
		}
		
		//1.4.4 => Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
		Job job = new Job(getConf());
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(inputPath));
		job.setMapperClass(ImportTableMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		//1.4.4 => AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username, password.getBytes(), true, tableName);
		AccumuloOutputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers));
		
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		return exitCode;
	}
	
	public static String usage() {
		String usage1 = "purdygood.accumulo.mapreduce.ImportTableDriver -n <instance_name> -z <zookeepers> -u <username> -p <password> -i <input_path> -o <output_table_name>";
		String usage2 = "purdygood.accumulo.mapreduce.ImportTableDriver --instanceName <instance_name> --zookeepers <zookeepers> --username <username> --password <password> --inputPath <input_path> --outputTableName <output_table_name>";
		
		return "\n##################################\nusage using short options => " + usage1 + "\nusage using long options  => " + usage2 + "\n##################################\n";
		
	}//end static method usage
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ImportTableDriver(), args);
		System.exit(res);
	}
}
