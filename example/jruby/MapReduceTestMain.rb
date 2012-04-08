require 'java'
include Java

# should be compiled like:
# jrubyc --javac -c ./:./lib/hadoop-mapreduce-client-core-0.23.0-cdh4b1.jar:./lib/hadoop-common-0.23.0-cdh4b1.jar:../java/src:./lib/hadoop-jruby-adapter.jar MapReduceTestMain.rb

# assume jars are located in lib directory
require "lib/hadoop-common-0.23.0-cdh4b1.jar"
require "lib/hadoop-mapreduce-client-core-0.23.0-cdh4b1.jar"

java_package 'com.akuroda.hadoop.jrubyexample'

# need to create subclasses of MapperAdapter and ReducerAdapter in Java
java_import 'com.akuroda.hadoop.jrubyexample.CountMapperAdapter'
java_import 'com.akuroda.hadoop.jrubyexample.CountReducerAdapter'

java_import 'org.apache.hadoop.fs.Path'
java_import 'org.apache.hadoop.io.IntWritable'
java_import 'org.apache.hadoop.io.Text'
java_import 'org.apache.hadoop.mapreduce.Job'
java_import 'org.apache.hadoop.mapreduce.lib.input.FileInputFormat'
java_import 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat'
java_import 'org.apache.hadoop.mapreduce.lib.output.FileOutputFormat'
java_import 'org.apache.hadoop.mapreduce.lib.output.TextOutputFormat'


class MapReduceTestMain

  java_signature 'void main(String[])'
  def self.main(args)
    puts "MapReduceTest"
    job = Job.instance
   
    # set MapperAdapter's subclass
    job.mapperClass = CountMapperAdapter.java_class
    puts job.mapperClass
    job.mapOutputKeyClass = Text.java_class
    puts job.mapOutputKeyClass
    job.mapOutputValueClass = IntWritable.java_class
    puts job.mapOutputValueClass

    # set ReducerAdapter's subclass
    job.reducerClass = CountReducerAdapter.java_class
    puts job.getReducerClass
    job.outputKeyClass = Text.java_class
    puts job.getOutputKeyClass
    job.outputValueClass = IntWritable.java_class
    puts job.getOutputValueClass

    FileInputFormat.addInputPath job, Path.new(args[0])
    FileOutputFormat.setOutputPath job, Path.new(args[1])

    job.waitForCompletion true
  end
end
