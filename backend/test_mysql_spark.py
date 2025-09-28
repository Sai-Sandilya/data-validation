from core.spark_session import get_spark

def test_mysql_connection():
    spark = get_spark()
    print("✅ Spark started. Version:", spark.version)

    try:
        # Test connection to source_db
        df = (spark.read.format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/source_db")
              .option("dbtable", "customers")   # customers table in source_db
              .option("user", "root")
              .option("password", "Sandy@123")
              .option("driver", "com.mysql.cj.jdbc.Driver")
              .load())

        print("✅ Connection successful. Showing 5 rows:")
        df.show(5)
    except Exception as e:
        print("❌ Connection failed:", e)

if __name__ == "__main__":
    test_mysql_connection()

