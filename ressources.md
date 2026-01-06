## API PySpark
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html

### Replace
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.replace.html

### Remplacer et caster
```py
df_basics = df_basics.replace(
    to_replace="\\N",
    value=None,
    subset=["startYear"]
)

df_basics = df_basics.withColumn(
    "startYear",
    df_basics["startYear"].cast("integer")
)
```
## DAG
Directed Acyclic Graph.En français, on le traduit par Graphe Acyclique Dirigé. C'est la techologie utiisée par Spark.