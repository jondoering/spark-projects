package com.jonathandoering.wordcount

import org.apache.spark._

/** Create a RDD of lines from a text file, and keep count of
  *  how often each word appears.
  */
object Wordcount {

  def main(args: Array[String]) {
    // Set up a SparkContext named WordCount that runs locally using
    // all available cores.
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Create a RDD of lines of text in our book
    val input = sc.textFile("src/main/resources/text.txt")
    // Use flatMap to convert this into an rdd of each word in each line
    val words = input.flatMap(line => line.split(' '))
    // Convert these words to lowercase
    val lowerCaseWords = words.map(word => word.toLowerCase())
    // Count up the occurence of each unique word
    val wordCounts = lowerCaseWords.countByValue()

    //Sort in Descending order
    val sortedWordCount = wordCounts.map(_.swap).toSeq.sortWith(_._1 > _._1).map(_.swap)

    // Print the first 20 results
    val sample = sortedWordCount.take(20)

    for ((word, count) <- sample) {
      println(word + " " + count)
    }

    sc.stop()
  }
}