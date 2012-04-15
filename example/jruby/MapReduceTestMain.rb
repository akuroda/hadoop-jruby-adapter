require 'java'
include Java

# should be compiled like:
# jrubyc --javac -c ./:./lib/hadoop-mapreduce-client-core-0.23.0-cdh4b1.jar:./lib/hadoop-common-0.23.0-cdh4b1.jar:../java/src:./lib/hadoop-jruby-adapter.jar MapReduceTestMain.rb

# assume jars are located in lib directory
require "lib/hadoop-common-0.23.0-cdh4b1.jar"
require "lib/hadoop-mapreduce-client-core-0.23.0-cdh4b1.jar"
require "lib/hadoop-jruby-adapter.jar"

java_package 'com.akuroda.hadoop.jrubyexample'

java_import 'com.akuroda.hadoop.MapperAdapter'
java_import 'com.akuroda.hadoop.ReducerAdapter'

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
   
    # set actual mapper via hadoop-jruby-adapter-conf.xml
    job.mapperClass = MapperAdapter.java_class
    puts job.mapperClass
    job.mapOutputKeyClass = Text.java_class
    puts job.mapOutputKeyClass
    job.mapOutputValueClass = IntWritable.java_class
    puts job.mapOutputValueClass

    # set actual reducer via hadoop-jruby-adapter-conf.xml
    job.reducerClass = ReducerAdapter.java_class
    puts job.reducerClass
    job.outputKeyClass = Text.java_class
    puts job.outputKeyClass
    job.outputValueClass = IntWritable.java_class
    puts job.outputValueClass

    FileInputFormat.addInputPath job, Path.new(args[0])
    FileOutputFormat.setOutputPath job, Path.new(args[1])

    job.waitForCompletion true
  end
end
