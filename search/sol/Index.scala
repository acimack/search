package search.sol

import java.io.{FileNotFoundException, IOException}

import search.src.{FileIO, PorterStemmer, StopWords}

import scala.collection.mutable
import scala.math.{pow, sqrt}
import scala.util.matching.Regex
import scala.xml.{Node, NodeSeq}

/**
 *  Indexer class
 * @param wikiFile a String representing the file name of the corpus
 * @param titlesFile a String representing the name of the file we will
 *                   write our title data in
 * @param docsFile a String representing the name of the file we will
 *                 write our ranking (docs) data in
 * @param wordsFile a String representing the name of the file we will
 *                  write our words data in
 */
class Index(wikiFile: String, titlesFile: String, docsFile: String,
            wordsFile: String) {

  // Maps the document ids to the title for each document
  private val idsToTitle = new mutable.HashMap[Int, String]


  // Maps the document ids to the euclidean normalization for each document
  // is the most frequently occurring term in doc
  private val idsToMaxFreqs = new mutable.HashMap[Int, Double]


  // Maps the document ids to the page rank for each document
  private val idsToPageRank = new mutable.HashMap[Int, Double]

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies = new
      mutable.HashMap[String, mutable.HashMap[Int, Double]]

  /**
   * The primary indexing method. Iterates through each page of the XML
   * document, extracting a list of the page's links, the page's title,
   * the page's id, and the body of the page, which is tokenized, stemmed,
   * and stripped of stop words. Index then maps the id and list of links into
   * a hashmap, idToDocLinks, which is used in PageRank. The respective
   * frequencies of the words are calculated and mapped into
   * wordsToDocumentFrequencies, and the highest frequency of this map is found
   * and mapped with the id of the page to idsToMaxFreqs, which is used to
   * calculate IDF.
   */
  def index() = {

    // Setting up main node and regex
    val mainNode: Node = xml.XML.loadFile(wikiFile)
    val pageSeq: NodeSeq = mainNode \ "page"
    val wordRegex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")
    val linkRegex = new Regex("""\[\[[^\[]+?\]\]""")

    // PageRank stuff ---------------------------------------------------------

    // Hashmap going from id to set of titles (links)
    val idToDocLinks = new mutable.HashMap[Int, mutable.Set[String]]()

    // Hashmap going from title to document ID
    val titlesToID = new mutable.HashMap[String, Int]

    // ------------------------------------------------------------------------

    // Recurring through all documents
    for (page <- pageSeq) {

      // Getting text, id, and title from document
      val title = (page \ "title").text.split("""\n""")(1)
      val idRegex = new Regex("""[\d]+""")
      val id: Int = idRegex.findFirstIn((page \ "id").text).getOrElse("this " +
        "will never happen").toInt
      val content: String =
        (page \ "title").text ++ " " ++ (page \ "text").text

      // Filling up idsToTitle hashmap
      idsToTitle.+=((id, title))

      // Filling up titlesToID hashmap (for PageRank)
      titlesToID.+=((title, id))

      // Gathering links and reformatting (removing brackets)
      val linksIterator = linkRegex.findAllMatchIn(content)
      val linkListHaHa = linksIterator.toList.map { aMatch => aMatch.matched }
      val textExtractor = new Regex("""[^\[\]]+""")
      val linkTextList = linkListHaHa.map { link =>
        textExtractor.findFirstIn(link).getOrElse("oops") }

      // Getting data from links (i.e. data before pipe and after pipe)
      val forPageRank = mutable.Set[String]()
      val toBeAddedToContents = new mutable.MutableList[String]()
      for (linkText <- linkTextList) {
        // Adding to forPageRank and toBeAddedToContents lists
        val pipeRegex = new Regex("""\|""")
        pipeRegex.findFirstIn(linkText) match {
          case Some(_) =>
            val splitLink = linkText.split("""\|""", 2)

            // learned from testing:
            // - if "|" char is at beginning, first item in array will be the
            // empty string
            // - if "|" is at the end, second item in array will be null
            if (splitLink.size == 1) {
              forPageRank += splitLink(0)
            } else {
              forPageRank += splitLink(0)
              toBeAddedToContents += splitLink(1)
            }
          case None =>
            forPageRank += linkText
            toBeAddedToContents += linkText
        }
      }

      // Adding set of links to idToDocLinks
      idToDocLinks += ((id, forPageRank))

      // Tokenizing document text
      val wordsIterator = wordRegex.findAllMatchIn(content)
      val tokenized =
        wordsIterator.toList.map { aMatch => aMatch.matched.toLowerCase }

      // Stopping and stemming tokenized words
      val stoppedAndStemmed = new mutable.MutableList[String]()
      for (word <- tokenized) {
        if (!StopWords.isStopWord(word)) {
          stoppedAndStemmed += PorterStemmer.stem(word)
        }
      }

      // Using the stopped and stemmed words to compute word frequencies
      // for the document
      val frequencyBuilder = new mutable.HashMap[String, Int]()
      for (word <- stoppedAndStemmed) {
        frequencyBuilder.get(word) match {
          case Some(num) => frequencyBuilder(word) = num + 1
          case None => frequencyBuilder += ((word, 1))
        }
      }

      // Filling up idsToMaxFreqs hashmap
      val highestFreq = frequencyBuilder.valuesIterator.max
      idsToMaxFreqs.+=((id, highestFreq))

      // Filling up wordToDocumentFrequencies hashmap
      // (using frequencyBuilder data)
      for ((word, freq) <- frequencyBuilder) {
        wordsToDocumentFrequencies.get(word) match {
          case Some(map) => wordsToDocumentFrequencies(word).+=((id, freq))
          case None =>
            wordsToDocumentFrequencies.+=((word,
              new mutable.HashMap[Int, Double]()))
            wordsToDocumentFrequencies(word).+=((id, freq))
        }
      }
    }

    // PageRank stuff ---------------------------------------------------------
    val epsilon = 1.0
    val n = pageSeq.size.toDouble

    // Creating empty hashmap to fill with weights
    val weights = new mutable.HashMap[Int, mutable.HashMap[Int, Double]]

    /** PageRank Calculations:
     *
     * The map of ID's and the list of pages they link to, (idToDocLinks) is
     * iterated through.
     * - First, we determine which of the page's links are valid by
     *  determining if the link matches any of the pages titles in the corpus
     *  and if the link is not in fact a link to the page itself.
     *  - Then, for each page in the corpus, represented as an (id, title) pair
     *  in idsToTitles, we determine if the if the original page (id) links to
     *  the new page(idStar). Depending on the result, we then calculate
     *  WSubJK, the weight given to page (idStar) by the original page (id).
     * - We then map the id of the new iterated page (idStar) to the weight
     *  (WSubJK) in innerWeights.
     * - We complete this second iteration for each page in the corpus. After
     *  the iteration is completed, we map the id of the initial page (id) to
     *  innerWeights.
     */
    for ((id, set) <- idToDocLinks) {
      // Creating list of valid links (i.e. in the corpus and not to itself)
      // and defining variables
      val validLinks = set.filter(link => titlesToID.contains(link)
        && !(idsToTitle(id).equals(link)))
      val nSubK = validLinks.size.toDouble
      val innerWeights = new mutable.HashMap[Int, Double]()
      for ((idStar, title) <- idsToTitle) {
        // Boolean; is there a link to the current document?
        val isIn = validLinks.contains(title)
        // Initializing wSubJK
        var wSubJK = 0.0
        if (isIn) {
          wSubJK = (epsilon / n) + ((1.0 - epsilon) * (1.0 / nSubK))
        } else {
          wSubJK = epsilon / n
        }
        innerWeights += ((idStar, wSubJK))
      }
      weights += ((id, innerWeights))
    }

    // Creating and initializing r and rPrime arrays
    var r = new Array[Double](pageSeq.size)
    var rPrime = new Array[Double](pageSeq.size)
    for (i <- 0 to n.toInt - 1) {
      r(i) = 0
      rPrime(i) = 1.0 / n
    }

    // Calculating PageRanks
    /**
     * r and rPrime represent the array of page rankings where the index of
     * this array corresponds to the page's id in the corpus. rPrime is the
     * current ranking, which gets updated based on a copy of that ranking in
     * each iteration. The ranking has stabilized when the distance between r
     * and rPrime is (!!!!!!LESS THAN OR GREATER THAN?????!!!!!) .001.
     */
    while (euclideanDistance(r, rPrime) > 0.001) {
      r = rPrime.clone
      for (j <- 0 to n.toInt - 1) {
        rPrime(j) = 0
        for (k <- 0 to n.toInt - 1) {
          rPrime(j) = rPrime(j) + (r(k) * weights(k)(j))
        }
      }
    }

    /**
     * For every page in the corpus:
     * Populates the map idsToPageRank with the id and its respective
     * PageRank, which is the value at the indexCount index of rPrime.
     */
    var indexCount = 0
    for ((id, map) <- weights) {
      idsToPageRank += ((indexCount, rPrime(indexCount)))
      indexCount += 1
    }
    // ------------------------------------------------------------------------

    // Writing to files to be used in querier
    writeFiles()
  }

  /**
   * Calculates the distance between r and rPrime by totaling the distance
   * between each corresponding index of r and rPrime
   * @param r Array[Double] representing the pages ranks
   * @param rPrime Array[Double] representing the pages current ranks
   * @return the distance between the two arrays
   */
  def euclideanDistance(r: Array[Double], rPrime: Array[Double]): Double = {
    var totalDistance = 0.0
    for (i <- 0 until r.length - 1) {
      totalDistance += pow((rPrime(i) - r(i)), 2)
    }
    sqrt(totalDistance)
  }


  //writes the maps in their corresponding documents
  def writeFiles(): Unit = {
    FileIO.printTitleFile(titlesFile, idsToTitle)
    FileIO.printDocumentFile(docsFile, idsToMaxFreqs, idsToPageRank)
    FileIO.printWordsFile(wordsFile, wordsToDocumentFrequencies)
  }
}

// Index object
object Index {
  /**
   * Creates an index object using the arguments
   * @param args an Array of Strings representing the parameters of the Index
   *             class
   * @throws FileNotFoundException if the first argument is not the name of
   *                               a valid file
   * @throws IOException if the arguments of main do not fit within the
   *                     parameters of index
   */
  def main(args: Array[String]): Unit = {
    try {
      val fileToIndex = args(0)
      val titles = args(1)
      val docs = args(2)
      val words = args(3)
      val index = new Index(fileToIndex, titles, docs, words)
      index.index()
      println("Indexing complete!")
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}
