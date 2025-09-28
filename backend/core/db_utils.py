from config.db_config import JDBC_DRIVER
from config.settings import NUM_PARTITIONS, PARTITION_COLUMN, LOWER_BOUND, UPPER_BOUND

def get_jdbc_url(config):
    return f"jdbc:mysql://{config['host']}:{config['port']}/{config['database']}"

def load_table(spark, config, table):
    return (spark.read.format("jdbc")
            .option("url", get_jdbc_url(config))
            .option("dbtable", table)
            .option("user", config["user"])
            .option("password", config["password"])
            .option("driver", JDBC_DRIVER)
            .option("numPartitions", NUM_PARTITIONS)
            .option("partitionColumn", PARTITION_COLUMN)
            .option("lowerBound", LOWER_BOUND)
            .option("upperBound", UPPER_BOUND)
            .load())

def write_table(df, config, table):
    (df.write.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", table)
        .option("user", config["user"])
        .option("password", config["password"])
        .option("driver", JDBC_DRIVER)
        .option("batchsize", 5000)
        .mode("append")
        .save())
