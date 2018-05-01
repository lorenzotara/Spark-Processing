import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lorenzotara on 30/04/18.
 */
public class SparkAppMain {

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // Reading articles and output files
        DataFrame articlesDF = sqlContext.read().json("data/dataset/*.gz");
        DataFrame outputDF = sqlContext.read().json("data/output.json");

        // Taking only useful information from articlesDF
        articlesDF.printSchema();
        articlesDF.show(5);

        DataFrame articles_id_content = articlesDF.withColumn("articleUID", articlesDF.col("feedEntry.identifier"))
                .withColumn("content", articlesDF.col("permalinkEntry.tokenizedContent"))
                .select("articleUID", "content");


        // Flattening output on occurrences
        DataFrame outputDFFlattened = outputDF.select(outputDF.col("speaker"), functions.explode(outputDF.col("occurrences")))
                .select("col.articleUID", "col.quotation", "col.extractedBy", "col.patternConfidence", "speaker");


        DataFrame joined = articles_id_content.join(outputDFFlattened, "articleUID");

        // Register the UDF with our SQLContext
        sqlContext.udf().register("tokenize", (String s) -> {

            String TOKENIZER_SETTINGS = "tokenizeNLs=false, americanize=false, " +
                    "normalizeCurrency=false, normalizeParentheses=false," +
                    "normalizeOtherBrackets=false, unicodeQuotes=false, ptb3Ellipsis=true," +
                    "escapeForwardSlashAsterisk=false, untokenizable=noneKeep, normalizeSpace=false";

            PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<>(new StringReader(s),
                    new CoreLabelTokenFactory(), TOKENIZER_SETTINGS);
            List<String> tokens = new ArrayList<>();
            while (ptbt.hasNext()) {
                CoreLabel label = ptbt.next();
                tokens.add(label.toString());
            }
            return tokens;

        }, DataTypes.createArrayType(DataTypes.StringType));

        joined.select(joined.col("articleUID"),
                        joined.col("content"),
                        joined.col("extractedBy"),
                        joined.col("patternConfidence"),
                        joined.col("speaker"),
                        functions.callUDF("tokenize", joined.col("quotation")))
                .withColumnRenamed("tokenize(quotation)", "quotation")
                .write().parquet("data/joined_java");

    }
}