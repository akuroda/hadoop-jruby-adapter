require 'java'
include Java

# should be compiled like:
# $JRUBYC --javac -c ./:./lib/hadoop-mapreduce-client-core-0.23.0-cdh4b1.jar:./lib/hadoop-common-0.23.0-cdh4b1.jar:../java/src:./lib/hadoop-jruby-adapter.jar MapReduceTest.rb

# assume jars are located in lib directory
require "lib/hadoop-common-0.23.0-cdh4b1.jar"
require "lib/hadoop-mapreduce-client-core-0.23.0-cdh4b1.jar"
require "lib/hadoop-jruby-adapter.jar"

java_package 'com.akuroda.hadoop.jrubyexample'
java_import 'org.apache.hadoop.io.IntWritable'
java_import 'org.apache.hadoop.io.Writable'
java_import 'org.apache.hadoop.io.Text'
java_import 'org.apache.hadoop.mapreduce.Mapper'
java_import 'org.apache.hadoop.mapreduce.Reducer'

class CountMapper
  def initialize
    @one = IntWritable.new(1)
    @word = Text.new
  end

  java_signature 'void map(Writable, Writable, Mapper.Context)'
  def map(key, value, context)
    #puts "mapper: #{value}"
    value.to_s.split.each do |i|
      @word.set(i)
      context.write(@word, @one)
    end
  end
end

class CountReducer
  java_signature 'void reduce(Writable, Iterable, Reducer.Context)'
  def reduce(key, values, context)
    #puts "reducer: #{key}"
    sum = 0
    values.each do |i|
      sum += i.get
    end
    context.write key, IntWritable.new(sum)
  end
end
