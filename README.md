# DEBS Challenge '24 -- International Conference on Distributed and Event-based Systems

This repository contains the solution developed by Abner Astete and Jonas Gottal for the DEBS Grand Challenge 2024, as part of the "Cloud Computing and Big Data Applications" course (OT6) at INSA (Institut National des Sciences Appliquées) Lyon.

## Introduction

Data streaming is essential in the era of big data and event-driven architectures, offering real-time insights, low-latency processing, and scalability. In the context of the DEBS Grand Challenge 2024, our team tackled the challenge of efficiently addressing provided queries on a data stream. Our approach leveraged PySpark, the Python version of Apache Spark. This README outlines our problem structuring and solution-finding process.

### DEBS Challenge

The ACM International Conference on Distributed and Event-Based Systems (DEBS) serves as a leading platform for discussing cutting-edge research in event-based computing, focusing on Big Data, AI/ML, IoT, and Distributed Systems. The 2024 DEBS Grand Challenge involves analyzing SMART data from over 200,000 hard drives provided by Backblaze. The challenge requires the implementation of two queries:

1. **Query 1:** Count of the recent number of failures detected for each vault (group by storage servers) (Continuous Querying).
2. **Query 2:** Use the count obtained from Query 1 to continuously compute a cluster of the drives (K-Means).

### Our Approach

We considered various frameworks and ultimately chose Apache Spark due to its polyglot nature, extensive libraries, and ease of initiating a data stream from a folder of CSV files. PySpark, with its structured streaming capabilities, was advantageous for team members familiar with Python.

## Implementation

### Schema Design

We initiated a structured data stream from CSV files using Apache Spark's Structured Streaming API. The schema for the PySpark data stream is defined as follows:

```python
# Schema for PySpark data stream
schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("serial_number", StringType(), True),
    StructField("model", StringType(), True),
    StructField("failure", IntegerType(), True),
    StructField("vault_id", IntegerType(), True),
    StructField("s1_read_error_rate", IntegerType(), True),
    ...
    StructField("s242_total_lbas_read", IntegerType(), True)
])
```

### First Query Implementation
The first query involves counting the recent number of failures detected for each vault using a sliding window. The PySpark [code](https://github.com/trashpanda-ai/Cloud-Computing-and-Big-Data-Applications/blob/0f9a8f9bd469ab7449575927856e6b02cc41f358/api/all_api.py) snippet for this query is as follows:

```python
# Apply Spark operations
windowedData = df \
    .withWatermark("date", "31 days") \
    .groupBy(
        df.vault_id,
        window(df.date, "30 days", "1 day"),
        df.model
    ) \
    .agg(_sum("failure").alias("total_failures"))

selectedData = windowedData.select("window.start", "vault_id", "total_failures")
filteredData = selectedData.filter(selectedData.total_failures > 0)
```


The provided PySpark code snippet addresses Query 1, which aims to "Count the recent number of failures detected for each vault." The code utilizes the PySpark Structured Streaming API to process data from CSV files as a stream.

1. **`withWatermark("date", "31 days")`:**
   - The `withWatermark` function sets a watermark on the "date" column, acting as a threshold for late data.
   - The watermark is set to 31 days, meaning the system considers data arriving up to 31 days late for processing.

2. **`groupBy(df.vault_id, window(df.date, "30 days", "1 day"), df.model)`:**
   - The `groupBy` operation groups data by "vault_id," creates a 30-day sliding window based on the "date" column with a 1-day interval, and further groups by "model."

3. **`agg(_sum("failure").alias("total_failures"))`:**
   - The `agg` function performs an aggregation operation on each group created by the `groupBy` clause.
   - It calculates the sum of the "failure" column for each group and names the resulting column as "total_failures."

4. **`selectedData = windowedData.select("window.start", "vault_id", "total_failures")`:**
   - The `select` operation chooses specific columns from the `windowedData`, including the start time of the window, "vault_id," and the aggregated "total_failures" column.

5. **`filteredData = selectedData.filter(selectedData.total_failures > 0)`:**
   - The `filter` operation is applied to keep only the rows where the "total_failures" column is greater than 0.
   - This step essentially removes records where there were no failures.

The overall process involves creating a structured data stream, defining a sliding window, grouping data, aggregating failures, selecting relevant columns, and filtering out records with no failures.

This PySpark code efficiently addresses the requirement to count recent failures for each vault in a streaming data scenario.


The live output of this query is presented in this Table:


| Start Time           | Vault ID | Total Failures |
|----------------------|----------|----------------|
| 2023-03-31 02:00:00  | 1149     | 1              |
| 2023-06-05 02:00:00  | 1042     | 1              |
| 2023-06-01 02:00:00  | 1097     | 1              |
| 2023-05-24 02:00:00  | 1042     | 1              |
| 2023-06-17 02:00:00  | 1406     | 1              |
| 2023-03-13 01:00:00  | 1124     | 1              |
| ...                  | ...      | ...            |


### Second Query Implementation
The second query involves dynamic K-Means clustering. Due to implementation challenges, the theoretical approach outlined in the project [report](https://www.overleaf.com/read/bngdyswhtzyp#929bac) is not fully functional. However, the [code](https://github.com/trashpanda-ai/Cloud-Computing-and-Big-Data-Applications/blob/c25a31f4f10fc60293f8c88a273c3efc88915c13/DEBS%20Challenge.ipynb) includes data normalization and an attempt at implementing the K-Means process.


To normalize the data within specified ranges, we use min-max scaling:

$$
x_{\text {normalized }}=\frac{x-x_{\min }}{x_{\max }-x_{\min }}
$$

This renders the data normalized in a range of $0$ to $1$ -- meaning we need to scale it to the desired range with data from the ```norm.csv```. We do so by multiplying with the difference from the provided upper and lower bounds and adding the min:

$$
x_{\text {scaled }}=  (x_{\text {upper }} - x_{\text {lower }}) \times x_{\text {normalized }}  + x_{\text {lower }}
$$

This renders a stream of normalized data in the same format as our incoming data.

The next step is to implement a custom K-Means model initialized by the provided centeroids - meaning we already know our $k=50$ and we only need to allocate the lowest euclidean distance:

- __Step 1:__ Perform the cross product of the data stream and center points, generating combinations of all incoming data points (ID: timestamp) and the current centroids (ID: label).
- __Step 2:__ Calculate the distance between all vectors using PySpark's ```Vectors.squared\_distance()```.
- __Step 3:__ Aggregate by selecting the minimum distance for all available non-identical pairings for each data stream ID (timestamp).
- __Step 4:__ Assign a dedicated label for the returned minimum distance.
- __Step 5:__ Count the number of drives for each label.
- __Step 6:__ Calculate the average vector for each label and overwrite the centroids.

The final implementation looks like this:

```python
# Initialize K and centroids
K = 3
centroids = data.takeSample(False, K, 1) # no replacement and seed 1


for _ in range(10):  # max 10 iterations
    # Assign each data point to the closest centroid 
    def closest(point):
        distances = [sqrt(sum((array(point) - c)**2)) for c in centroids]
        return distances.index(min(distances)), (point, 1)
    assignment = data.map(closest)

    # Recompute centroids (average of all points in the cluster)
    def combine(t1, t2):
        sum1, count1 = t1
        sum2, count2 = t2
        # sum all arrays and count the points to normalize
        return list(array(sum1) + array(sum2)), count1 + count2

    newCentroids = assignment.reduceByKey(combine).map(lambda t: [x / t[1][1] for x in t[1][0]]).collect()

    # Check for convergence
    if centroids == newCentroids:
        break

    centroids = newCentroids
```

Although this approach makes sense from a theoretical point of view, its practical efficiency is limited. Additionally, we encountered significant challenges in managing and consolidating various aggregations. Attempts with PySpark's out-of-the-box solutions proved unsuitable for meeting query 2 of the DEBS grand challenge.

## Conclusion
While PySpark offered a valuable learning experience, it posed challenges in terms of processing speed and integration into the benchmarking platform. The second query faced implementation challenges within the given time frame. Despite these challenges, the team recognizes PySpark's learning curve and versatile applications.
