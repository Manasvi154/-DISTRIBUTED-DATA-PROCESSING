# -DISTRIBUTED-DATA-PROCESSING

COMPANY: CODTECH IT SOLUTIONS

NAME: MANASVI KISAN PAWAR

INTERN ID: CT08DN290

DOMAIN: BIG DATA

DURATION: 8 WEEKS

MENTOR: NEELA SANTOSH

The second task centers on analyzing a large dataset using Apache Spark, with a focus on operations such as filtering, grouping, and aggregations. Apache Spark, being a distributed data
processing engine, is highly suitable for handling large-scale datasets efficiently.
Using the Titanic dataset again, the task began by reading the cleaned data into Spark. The analysis began with filtering operations—for example, selecting only female passengers using
filter(col("Sex") == "female"). This demonstrates how Spark can efficiently handle conditional queries over millions of records.
Next, grouping and aggregation were performed. One such analysis grouped the data by the Pclass (Passenger Class) to understand class-based differences. Metrics such as total passengers,
average age, and average fare were calculated using groupBy() combined with aggregation functions like count(), avg(), and sum().
Another key analysis was grouping by Sex and Survived to determine survival patterns by gender. This kind of multidimensional aggregation is valuable in deriving real-world insights, such as
how demographic factors influence outcomes.
These operations were executed using Spark’s lazy evaluation model, which optimizes query execution. The results were displayed using .show() and optionally written to an output folder using
.write() for reporting purposes.
This task emphasizes the power of Spark in handling analytics at scale, where filtering and aggregations that would take minutes on traditional tools happen almost instantly. It also mirrors
real-world use cases where businesses need quick insights from their data warehouses. The deliverable for this task was a complete .ipynb notebook or .py script showcasing filtered outputs,
grouped statistics, and total transaction-level summaries.

#OUTPUT
