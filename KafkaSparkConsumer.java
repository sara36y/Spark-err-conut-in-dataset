package com.example;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;


public class KafkaSparkConsumer {
    public static void main(String[] args) {
        try {
            // إنشاء جلسة Spark مع تحديد الـ master
            SparkSession spark = SparkSession.builder()
                    .appName("ErrorLogAnalysis")
                    .master("local[*]") // تحديد أن الجلسة تعمل محليًا باستخدام جميع الأنوية المتاحة
                    .getOrCreate();

            // قراءة البيانات من Kafka
            Dataset<Row> df = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "logs-topic")
                    .load();

            // تحويل القيمة من بايت إلى سترينج
            Dataset<Row> logsDf = df.selectExpr("CAST(value AS STRING)");

            // تصفية السجلات اللي تحتوي على "ERR" أو "Error"
            Dataset<Row> errorLogsDf = logsDf.filter(col("value").contains("ERR")
                    .or(col("value").contains("Error")));

            // تجميع البيانات وعدد مرات ظهور كل رسالة
            Dataset<Row> errorCountsDf = errorLogsDf.groupBy("value").count();

            // كتابة النتائج إلى MySQL
            errorCountsDf.writeStream()
                    .outputMode("complete")
                    .foreachBatch((batchDF, batchId) -> {
                        try {
                            // الكتابة إلى قاعدة بيانات MySQL
                            batchDF.write()
                                    .format("jdbc")
                                    .option("url", "jdbc:mysql://localhost:3306/logs")
                                    .option("dbtable", "error_counts")
                                    .option("user", "root")
                                    .option("password", "password")
                                    .mode("append")
                                    .save();
                        } catch (Exception e) {
                            System.out.println("An error occurred while writing to MySQL.");
                            e.printStackTrace();
                        }
                    })
                    .start()
                    .awaitTermination();
        } catch (Exception e) {
            System.out.println("An error occurred while consuming data from Kafka.");
            e.printStackTrace();
        }
    }
}
