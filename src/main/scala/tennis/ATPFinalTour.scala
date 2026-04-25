package tennis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, coalesce, lit, sum, desc}
import org.apache.spark.graphx._

object ATPFinalTour {

  // Vertex property
  case class Player(name: String, country: String)

  // Edge property
  case class Match(matchType: String, points: Int, head2HeadCount: Int)

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    // --------------------------------------------------------------------
    // 1. Create the graph using GraphX (RDDs) – use Seq, not Array
    // --------------------------------------------------------------------
    val verticesRDD = spark.sparkContext.parallelize(Seq(
      (1L, Player("Novak Djokovic", "SRB")),
      (3L, Player("Roger Federer", "SUI")),
      (5L, Player("Tomas Berdych", "CZE")),
      (7L, Player("Kei Nishikori", "JPN")),
      (11L, Player("Andy Murray", "GBR")),
      (15L, Player("Stan Wawrinka", "SUI")),
      (17L, Player("Rafael Nadal", "ESP")),
      (19L, Player("David Ferrer", "ESP"))
    ))

    val edgesRDD = spark.sparkContext.parallelize(Seq(
      // Group G1
      Edge(1L, 5L, Match("G1", 1, 1)),
      Edge(1L, 7L, Match("G1", 1, 1)),
      Edge(3L, 1L, Match("G1", 1, 1)),
      Edge(3L, 5L, Match("G1", 1, 1)),
      Edge(3L, 7L, Match("G1", 1, 1)),
      Edge(7L, 5L, Match("G1", 1, 1)),
      // Group G2
      Edge(11L, 19L, Match("G2", 1, 1)),
      Edge(15L, 11L, Match("G2", 1, 1)),
      Edge(15L, 19L, Match("G2", 1, 1)),
      Edge(17L, 11L, Match("G2", 1, 1)),
      Edge(17L, 15L, Match("G2", 1, 1)),
      Edge(17L, 19L, Match("G2", 1, 1)),
      // Semifinals
      Edge(3L, 15L, Match("S", 5, 1)),
      Edge(1L, 17L, Match("S", 5, 1)),
      // Final
      Edge(1L, 3L, Match("F", 11, 1))
    ))

    val graph = Graph(verticesRDD, edgesRDD)

    // --------------------------------------------------------------------
    // 2. Convert edges and vertices to DataFrames for easier querying
    //    (The graph itself is still GraphX – this is just helper conversion)
    // --------------------------------------------------------------------
    val edgesDF = graph.edges.map(e => (e.srcId, e.dstId, e.attr.matchType, e.attr.points, e.attr.head2HeadCount))
      .toDF("src", "dst", "matchType", "points", "head2HeadCount")
    val verticesDF = graph.vertices.map { case (id, p) => (id, p.name, p.country) }
      .toDF("id", "name", "country")

    // 1. All match details
    println("All matches:")
    edgesDF.show(false)

    // 2. Match descriptions
    println("\nMatch descriptions (winner beat loser in round):")
    edgesDF.as("e")
      .join(verticesDF.as("w"), col("e.src") === col("w.id"))
      .join(verticesDF.as("l"), col("e.dst") === col("l.id"))
      .select(col("w.name").as("winner"), col("l.name").as("loser"), col("e.matchType"))
      .collect()
      .foreach(row => println(s"${row.getString(0)} beat ${row.getString(1)} in ${row.getString(2)}"))

    // 3. Group 1 winners total points
    println("\nGroup 1 winners with total points:")
    edgesDF.filter(col("matchType") === "G1")
      .groupBy("src")
      .agg(sum("points").as("total_points"))
      .join(verticesDF, col("src") === col("id"))
      .select(col("name"), col("total_points"))
      .orderBy(col("total_points").desc)
      .show(false)

    // 4. Group 2 winners total points
    println("\nGroup 2 winners with total points:")
    edgesDF.filter(col("matchType") === "G2")
      .groupBy("src")
      .agg(sum("points").as("total_points"))
      .join(verticesDF, col("src") === col("id"))
      .select(col("name"), col("total_points"))
      .orderBy(col("total_points").desc)
      .show(false)

    // 5. Semifinal winners and points
    println("\nSemifinal winners and match points:")
    edgesDF.filter(col("matchType") === "S")
      .join(verticesDF, col("src") === col("id"))
      .select(col("name"), col("points"))
      .show(false)

    // 6. Final winner and points
    println("\nFinal winner and match points:")
    edgesDF.filter(col("matchType") === "F")
      .join(verticesDF, col("src") === col("id"))
      .select(col("name"), col("points"))
      .show(false)

    // 7. Total points per player (including zero for those who didn't win any)
    println("\nTotal points earned by each player (throughout tournament):")
    val totalPoints = edgesDF.groupBy("src")
      .agg(sum("points").as("total_won"))
      .join(verticesDF, col("src") === col("id"), "right")
      .select(col("name"), coalesce(col("total_won"), lit(0)).as("total_points"))
      .orderBy(col("total_points").desc)
    totalPoints.show(false)

    // 8. Tournament winner based on most total points
    println("\nTournament winner (most total points):")
    totalPoints.limit(1).show(false)

    // 9. Pairs that played more than once
    println("\nPairs of players who faced each other more than once:")
    val multiPlay = edgesDF.groupBy("src", "dst").count().filter(col("count") > 1)
    if (multiPlay.count() == 0) println("None") else multiPlay.show(false)

    // 10-12. Wins / losses statistics (using GraphX RDDs)
    val winnersSet = graph.edges.map(_.srcId).distinct().collect().toSet
    val losersSet = graph.edges.map(_.dstId).distinct().collect().toSet
    val allPlayersSet = graph.vertices.map(_._1).collect().toSet
    println(s"\nPlayers who won at least one match: ${winnersSet.mkString(", ")}")
    println(s"Players who lost at least one match: ${losersSet.mkString(", ")}")
    println(s"Players who both won and lost: ${winnersSet.intersect(losersSet).mkString(", ")}")
    println(s"Players with zero wins: ${(allPlayersSet -- winnersSet).mkString(", ")}")
    println(s"Players with zero losses: ${(allPlayersSet -- losersSet).mkString(", ")}")

    // 13. PageRank on reversed graph (pure GraphX)
    val reversedEdges = graph.edges.map(e => Edge(e.dstId, e.srcId, e.attr))
    val reversedGraph = Graph(graph.vertices, reversedEdges)
    val pr = reversedGraph.pageRank(0.001).vertices
    val top3 = pr.join(graph.vertices).map { case (id, (rank, player)) => (player.name, rank) }
      .sortBy(-_._2).take(3)
    println("\nTop 3 most important players (PageRank on reversed graph):")
    top3.foreach { case (name, rank) => println(s"$name -> $rank") }
    // Display as table for consistency
    spark.sparkContext.parallelize(top3).toDF("name", "pagerank").show(false)
  }
}