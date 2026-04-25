package atp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object ATPWorldTourAnalysis {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")

    // ------------------------------------------------------------
    // 1. Load player data – transform birthdate
    // ------------------------------------------------------------
    val players = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/player_overviews.csv")
      .withColumn("birthdate", to_date(col("birthdate"), "yyyy.MM.dd"))
      .withColumnRenamed("player_id", "id")
      .withColumn("name", concat(col("first_name"), lit(" "), col("last_name")))
      .select("id", "name", "birthdate", "birthplace", "handedness")

    // ------------------------------------------------------------
    // 2. Load match scores – rename winner/loser to src/dst
    // ------------------------------------------------------------
    val matchesRaw = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/match_scores_2020-2022.csv")

    val matchesDF = matchesRaw
      .withColumnRenamed("winner_id", "src")
      .withColumnRenamed("loser_id", "dst")
      .withColumnRenamed("tourn_round", "round")
      .withColumnRenamed("tourn_round_order", "round_order")
      .withColumnRenamed("match_score", "score")

    // ------------------------------------------------------------
    // 3. Load tournaments – note: no full start_date column (only year)
    // ------------------------------------------------------------
    val tournaments = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/tournaments_2020-2022.csv")
      .withColumnRenamed("tourn_name", "tournament")
      .withColumnRenamed("tourn_start_year", "year")
      // The CSV does not contain a full date column; we create a placeholder.
      .withColumn("start_date", lit(null))
      .select("tourn_id", "tournament", "year", "tourn_ground", "start_date")

    // ------------------------------------------------------------
    // 4. Join matches with tournaments
    // ------------------------------------------------------------
    val matches = matchesDF.join(tournaments, Seq("tourn_id"), "left")
      .select($"src", $"dst", $"tournament", $"year", $"round", $"round_order", $"score", $"tourn_ground", $"start_date")

    // ------------------------------------------------------------
    // 5. Full graph
    // ------------------------------------------------------------
    val fullGraph = GraphFrame(players, matches)
    println(s"Full graph: ${fullGraph.vertices.count()} vertices, ${fullGraph.edges.count()} edges")

    // ------------------------------------------------------------
    // 6. Function to display tournament results
    // ------------------------------------------------------------
    def displayTournamentResults(tournamentName: String, year: Int): Unit = {
      val subEdges = matches.filter($"tournament" === tournamentName && $"year" === year)
      if (subEdges.count() == 0) {
        println(s"No matches found for $tournamentName $year")
        return
      }
      val subGraph = GraphFrame(players, subEdges)
      subGraph.edges
        .join(players.as("w"), $"src" === $"w.id")
        .join(players.as("l"), $"dst" === $"l.id")
        .select($"year", $"round", $"round_order", $"w.name".as("Winner"), $"l.name".as("Loser"), $"score")
        .orderBy($"round_order")
        .show(100, false)
    }

    println("Distinct tournament names in 2020:")
    fullGraph.edges.filter($"year" === 2020).select("tournament").distinct().show(200, false)

    println("\nExample: US Open 2020 (if data exists)")
    displayTournamentResults("US Open", 2020)

    // ------------------------------------------------------------
    // 7. Nitto ATP Finals 2020 – GROUP STAGE ONLY
    // ------------------------------------------------------------
    val allFinalsEdges = matches.filter($"tournament" === "Nitto ATP Finals" && $"year" === 2020)

    // Automatically detect group‑stage round values
    val distinctRounds = allFinalsEdges.select("round").distinct().as[String].collect()
    println(s"\nDistinct round values in Nitto ATP Finals 2020: ${distinctRounds.mkString(", ")}")

    val groupStageCondition = distinctRounds.exists { r =>
      r.toLowerCase.contains("round robin") || r.toLowerCase.contains("rr") || r.toLowerCase.contains("group")
    }

    val finalsEdges = if (groupStageCondition) {
      allFinalsEdges.filter($"round".rlike("(?i).*(round robin|rr|group).*"))
    } else {
      println("Warning: No distinct group stage round found; using all matches (groups may be connected).")
      allFinalsEdges
    }

    // Build graph with only group‑stage vertices
    val finalsPlayerIds = finalsEdges.select($"src").union(finalsEdges.select($"dst"))
      .distinct().withColumnRenamed("src", "id")
    val finalsPlayers = players.join(finalsPlayerIds, "id")
    val finalsGraph = GraphFrame(finalsPlayers, finalsEdges)

    println(s"\nNitto ATP Finals 2020 (group stage only): ${finalsGraph.vertices.count()} players, ${finalsGraph.edges.count()} matches")

    if (finalsGraph.edges.count() > 0) {
      // 5. Detect groups using connected components
      val cc = finalsGraph.connectedComponents.run()
      val groups = cc.join(finalsPlayers, "id")
        .select($"component", finalsPlayers("name"))
        .orderBy("component")
      println("\nGroups (connected components):")
      groups.show(false)

      // 6. Top 2 qualifiers per group by wins (only group stage)
      val wins = finalsGraph.edges.groupBy("src").count().as("wins")
      val withWins = wins.join(finalsPlayers, wins("src") === finalsPlayers("id"), "right")
        .select(finalsPlayers("id"), finalsPlayers("name").as("player_name"), coalesce($"count", lit(0)).as("wins"))
      val withComponent = withWins.join(cc, "id")
      val window = org.apache.spark.sql.expressions.Window.partitionBy("component").orderBy(desc("wins"))
      val top2 = withComponent.withColumn("rank", row_number().over(window)).filter($"rank" <= 2)
      println("\nTop 2 qualifiers per group (by group stage wins):")
      top2.select("component", "player_name", "wins").show(false)
    } else {
      println("No group stage matches found for Nitto ATP Finals 2020.")
    }

    // ------------------------------------------------------------
    // 8. Major tournaments (Grand Slam, Masters 1000, ATP 500) in 2020
    // ------------------------------------------------------------
    val importantTourneys = tournaments.filter(
      ($"tournament".contains("Grand Slam") ||
        $"tournament".contains("Masters") ||
        $"tournament".contains("500")) &&
        $"year" === 2020
    ).select("tournament").distinct()

    val importantEdges = matches.join(importantTourneys, "tournament")
    val importantPlayerIds = importantEdges.select($"src").union(importantEdges.select($"dst"))
      .distinct().withColumnRenamed("src", "id")
    val importantPlayers = players.join(importantPlayerIds, "id")
    val gImportant = GraphFrame(importantPlayers, importantEdges)

    println(s"\nMajor tournaments subgraph 2020: ${gImportant.vertices.count()} players, ${gImportant.edges.count()} matches")

    if (gImportant.edges.count() > 0) {
      // Alternative 1: total number of wins
      println("\nTop 10 players by number of wins (major tournaments 2020):")
      gImportant.edges.groupBy("src").count()
        .join(importantPlayers, $"src" === $"id")
        .select($"name", $"count".as("wins"))
        .orderBy(desc("wins"))
        .limit(10)
        .show(false)

      // Alternative 2: PageRank (original direction winner -> loser)
      val pr = gImportant.pageRank.resetProbability(0.15).tol(0.01).run()
      println("\nTop 10 players by PageRank (major tournaments 2020):")
      pr.vertices.orderBy(desc("pagerank")).select("name", "pagerank").limit(10).show(false)
    } else {
      println("No major tournaments found for 2020.")
    }
  }
}