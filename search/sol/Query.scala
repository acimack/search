package search.sol

import java.io._

import search.src.{FileIO, PorterStemmer, StopWords}

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Represents a query REPL built off of a specified index
 *
 * @param titleIndex    - the filename of the title index
 * @param documentIndex - the filename of the document index
 * @param wordIndex     - the filename of the word index
 * @param usePageRank   - true if page rank is to be incorporated into scoring
 */
class Query(titleIndex: String, documentIndex: String, wordIndex: String,
            usePageRank: Boolean) {

  // Maps the document ids to the title for each document
  private val idsToTitle = new mutable.HashMap[Int, String]

  // Maps the document ids to the euclidean normalization for each document
  // is the most frequently occurring term in doc
  private val idsToMaxFreqs = new mutable.HashMap[Int, Double]

  // Maps the document ids to the page rank for each document
  private val idsToPageRank = new mutable.HashMap[Int, Double]

  // Maps each word to its inverse document frequency
  private val wordsToInvFreq = new mutable.HashMap[String, Double]

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies = new
      mutable.HashMap[String, mutable.HashMap[Int, Double]]

  //-------------DELETE?------------------------------------------
  // tf = going to have to divide word freq in HM #5 by max freq in HM #2
  // idf = HM #4
  // tfidf (relevance score) = tf * idf

  // pagerank = HM #3

  // overall score = tfidf * pagerank (iff pagerank considered)

  // within query, we need to make a hashmap going from doc ids to TOTAL scores
  // we then need to make an array of ints, the index of the resulting docs,
  // in order of highest to lowest scores
  //---------------------------------------------------------------
  /**
   * Handles a single query and prints out results
   *
   * @param userQuery - the query text, a String
   */
  private def query(userQuery: String): Unit = {

    // Code to tokenize, stop, and stem query
    val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")
    val matchesIterator = regex.findAllMatchIn(userQuery)
    val tokenizedQuery =
      matchesIterator.toList.map { aMatch => aMatch.matched.toLowerCase }

    // Stopping and stemming tokenized words from the query
    val stoppedAndStemmed = new mutable.MutableList[String]()
    for (word <- tokenizedQuery) {
      if (!StopWords.isStopWord(word)) {
        stoppedAndStemmed += PorterStemmer.stem(word)
      }
    }

    //an array of words in the query
    val splitQuery = stoppedAndStemmed.toArray

    //an empty map which will map id's to scores
    val idToScoreMap = scala.collection.mutable.HashMap.empty[Int, Double]

    // need to be totaling scores as we go along,
    // making sure to add like terms if doc id = same

    //for every word in the query
    for (word <- splitQuery) {

      // filling in wordToInvFreq
      val totalDocs = idsToTitle.size
      var docsContaining = idsToTitle.size
      if (wordsToDocumentFrequencies.contains(word)) {
        docsContaining = wordsToDocumentFrequencies(word).size
      }
      //calculate the idf of the corpus with respect to the word/term/query
      val idf = Math.log(totalDocs / docsContaining)
      wordsToInvFreq.+=((word, idf))


      // defining accumulator, map of id's to scores
      val partialIDToScore =
        scala.collection.mutable.HashMap.empty[Int, Double]

      /**
       * If the word appears within the corpus then compute the
       * relevance score of each page the word appears in. If using
       * PageRank, then multiply the score by the id's page rank
       *
       * If not, then do nothing
       */
      wordsToDocumentFrequencies.get(word) match {
        case Some(hashmap) =>
          for ((id, frequency) <- hashmap) {

            // Calculating relevance score (tfidf)
            val maxDocFreq = idsToMaxFreqs(id)
            val tf = frequency / maxDocFreq
            var relScore = tf * wordsToInvFreq(word)

            // If PageRank is being used, include in calculations
            if (this.usePageRank) {
              relScore = relScore * idsToPageRank(id)
            }
            partialIDToScore += (id -> relScore)
          }
        case None => // then do nothing, move onto next word
      }

      /**
       * inserting final results into the partialID map, totaling scores
       * for pages already existing within the map, adding a new mapping
       * otherwise
       */
      for ((id, score) <- partialIDToScore) {
        idToScoreMap.get(id) match {
          case Some(n) =>
            idToScoreMap(id) = idToScoreMap(id) + score
          case None =>
            idToScoreMap += (id -> score)
        }
      }
    }

    /**
     * Order and print the results by creating a Priority Queue of
     * (Int, Double) tuples representing the id, score pairs of
     * idToScoreMap. Populate the results Priority Queue with the pairs of
     * idToScoreMap, order results, call printResults.
     */
    val orderByFreq = Ordering.by[(Int, Double), Double](_._2)
    val results = new mutable.PriorityQueue[(Int, Double)]()(orderByFreq)
    for ((id, score) <- idToScoreMap) {
      if (score != 0.0) {
        results += ((id, score))
      }
    }
    printResults(results)
  }


  /**
   * Format and print up to 10 results from the results list
   *
   * @param results - an array of all results
   */
  private def printResults(results: mutable.PriorityQueue[(Int, Double)]) {
    var anyResults = false
    println("Top results:")
    for (i <- 1 to Math.min(10, results.size)) {
      anyResults = true
      val pair = results.dequeue
      println("\t" + (i) + ": " + idsToTitle(pair._1).toString)
    }
    if (!anyResults) {
      println("\t" + "No search results could be returned for this query")
    }
  }

  def readFiles(): Unit = {
    FileIO.readTitles(titleIndex, idsToTitle)
    FileIO.readDocuments(documentIndex, idsToMaxFreqs, idsToPageRank)
    FileIO.readWords(wordIndex, wordsToDocumentFrequencies)
  }

  /**
   * Starts the read and print loop for queries
   */
  def run() {
    val inputReader = new BufferedReader(new InputStreamReader(System.in))

    // Print the first query prompt and read the first line of input
    print("search> ")
    var userQuery = inputReader.readLine()

    // Loop until there are no more input lines (EOF is reached)
    while (userQuery != null) {
      // If ":quit" is reached, exit the loop
      if (userQuery == ":quit") {
        inputReader.close()
        return
      }

      // Handle the query for the single line of input
      query(userQuery)

      // Print next query prompt and read next line of input
      print("search> ")
      userQuery = inputReader.readLine()
    }

    inputReader.close()
  }
}

// Query object
object Query {
  /**
   * Creates an query object using the arguments
   * @param args Array of Strings representing the pagerank value
   *             and the names of the titles file, the docs file, and
   *             the words file.
   * @throws FileNotFoundException if a file name does not match that of a
   *                               legitmate file
   * @throws IOException if the input arguments are not formatted correctly
   */
  def main(args: Array[String]) {
    try {
      // Run queries with page rank
      var pageRank = false
      var titleIndex = 0
      var docIndex = 1
      var wordIndex = 2
      if (args.size == 4 && args(0) == "--pagerank") {
        pageRank = true;
        titleIndex = 1
        docIndex = 2
        wordIndex = 3
      } else if (args.size != 3) {
        println("Incorrect arguments. Please use [--pagerank] <titleIndex> "
          + "<documentIndex> <wordIndex>")
        System.exit(1)
      }
      val query: Query = new Query(args(titleIndex),
        args(docIndex), args(wordIndex), pageRank)
      query.readFiles()
      query.run()
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}
