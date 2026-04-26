# Apache Spark – Final Big Data Project

**Three-part implementation of tennis event analysis and Airbnb price prediction using Apache Spark (Scala).**

* **Part I.1** – ATP final tournament analysis with **GraphX** (PageRank, win/loss statistics, total points, etc.)
* **Part I.2** – ATP World Tour 2020-2022 analysis with **GraphFrames** (tournament filtering, group detection, top players)
* **Part II** – Airbnb price estimation using a complete **ML pipeline** (Gradient Boosted Trees, cross-validation, RMSE/MAE evaluation)

---

## 🛠️ Technologies

* **Apache Spark 3.5.8** (Core, SQL, MLlib, GraphX, GraphFrames)
* **Scala 2.12.15**
* **sbt** (build tool)
* **GraphFrames** (graph analytics on DataFrames)
* **Spark ML** (feature engineering, regression, cross-validation)

---

## 📁 Project Structure

```
spark-project-Intellij/
├── build.sbt
├── src/main/scala/
│   ├── Main.scala
│   ├── tennis/
│   │   └── ATPFinalTour.scala        # GraphX implementation
│   ├── atp/
│   │   └── ATPWorldTourAnalysis.scala # GraphFrames implementation
│   └── airbnb/
│       └── AirbnbPriceEstimation.scala
└── src/main/resources/
    ├── airbnb-data.csv
    ├── match_scores_2020-2022.csv
    ├── player_overviews.csv
    └── tournaments_2020-2022.csv
```

---

## 🚀 How to Run

1. **Clone the repository**

   ```bash
   git clone https://github.com/Maha-Jr10/spark-tennis-airbnb-analysis.git
   cd spark-tennis-airbnb-analysis
   ```

2. **Place the CSV files** in `src/main/resources/` (as shown above).
   The program expects the exact filenames (header-inferred CSV).

3. **Run with sbt**

   ```bash
   sbt compile
   sbt run
   ```

   The `Main` object executes all three parts sequentially.

4. **Output** – Results will be printed to the console (match lists, win tables, PageRank scores, sample Airbnb predictions, etc.).

---

## 📊 Key Results

### I.1 – ATP Final Tour (GraphX)

* Tournament winner by total points : **Novak Djokovic (18 pts)**
* Top 3 PageRank (reversed graph) : Djokovic (3.38), Federer (3.27), Nadal (0.39)
* Zero-win players : Tomas Berdych, David Ferrer

### I.2 – ATP World Tour 2020-2022 (GraphFrames)

* Full graph : **10,912 players**, **27,341 matches**
* **Nitto ATP Finals 2020** (group stage only) : 12 players, 36 matches
  Top 2 qualifiers : Novak Djokovic (8 wins), Daniil Medvedev (6 wins)
* **Major tournaments 2020** (Grand Slam / Masters 1000 / ATP 500) :

  * Most wins : Stefanos Tsitsipas (64)
  * Top PageRank : Pedro Martinez (3.40)

### II – Airbnb Price Prediction

* Data after cleaning : **4,560 rows**
* Model : **Gradient Boosted Trees** (log-transformed target)
* Cross-validation RMSE (log scale) : **0.400**
* Test RMSE (original price) : **$74.63**
* Test MAE (original price) : **$51.13**

Sample predictions (actual → predicted) :
`$229 → $237` , `$90 → $113`, `$195 → $207`, `$99 → $104`

---

## 📦 Dependencies (build.sbt)

```scala
val sparkVersion = "3.5.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion,
  "org.apache.spark" %% "spark-sql"       % sparkVersion,
  "org.apache.spark" %% "spark-mllib"     % sparkVersion,
  "org.graphframes"  % "graphframes"      % "0.10.0-spark3.2"
)
```

---

## 📝 Notes

* Part I.1 uses **GraphX** (RDD-based).
* Part I.2 uses **GraphFrames** (DataFrame-based) because the CSV data is easier to handle with Spark SQL.
* The Airbnb model includes outlier capping, log transformation, and a parameter grid for cross-validation.
* All results are deterministic (seeded splits and random states).

---

## 📄 License

This project is for educational purposes. Feel free to use it as a reference.

---

## 👤 Author

JOHN MUHAMMED
Big Data II 
