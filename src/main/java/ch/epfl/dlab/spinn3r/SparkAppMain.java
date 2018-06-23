package ch.epfl.dlab.spinn3r;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConversions$;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by lorenzotara on 30/04/18.
 */
public class SparkAppMain {

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // Path to all articles
        String data_path = args[0];
        // Path to quootstrap output
        String quootstrap_path = args[1];
        // Path where to write the results
        String writePath = args[2];

        // Mapping: are you mapping between the discarded documents of quootstrap and the output?
        String mapping;
        // Path to the mapping file
        String mappingPath;

        try {
            mapping = args[3];
            mappingPath = args[4];

        } catch (ArrayIndexOutOfBoundsException e) {
            mapping = "from_output";
            mappingPath = "not_used";
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Register the UDF with our SQLContext
        sqlContext.udf().register("tokenize", (String s) -> {

            // HTML tags not to delete (quotations)
            ArrayList<String> tagNotToDel = new ArrayList<>();
            tagNotToDel.add("<q>");
            tagNotToDel.add("<blockquote>");
            tagNotToDel.add("</q>");
            tagNotToDel.add("</blockquote>");

            // Fixing parenthesis
            HashMap<String, String> fixMap = new HashMap<>();
            fixMap.put("-LRB-", "(");
            fixMap.put("-RRB-", ")");
            fixMap.put("-LSB-", "[");
            fixMap.put("-RSB-", "]");

            String tagDel = "<";

            String TOKENIZER_SETTINGS = "tokenizeNLs=false, americanize=false, " +
                    "normalizeCurrency=false, normalizeParentheses=false," +
                    "normalizeOtherBrackets=false, unicodeQuotes=false, ptb3Ellipsis=true," +
                    "escapeForwardSlashAsterisk=false, untokenizable=noneKeep, normalizeSpace=false";

            PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<>(new StringReader(s),
                    new CoreLabelTokenFactory(), TOKENIZER_SETTINGS);
            List<String> tokens = new ArrayList<>();
            while (ptbt.hasNext()) {
                CoreLabel label = ptbt.next();
                String labelString = label.toString();
                if (fixMap.containsKey(labelString)) {
                    tokens.add(fixMap.get(labelString));
                }
                else if (labelString.startsWith(tagDel)  && !tagNotToDel.contains(labelString)) {
                    continue;
                }
                // Splitting words with dash
                else if (labelString.contains("-") && labelString.length() > 1) {
                    String[] b = labelString.split("-");
                    if (b.length > 1) {
                        tokens.add(b[0]);
                        tokens.add("-");
                        tokens.add(b[1]);
                    }
                    else {
                        tokens.add(labelString);
                    }
                }

                else {
                    tokens.add(labelString);
                }
            }
            return tokens;

        }, DataTypes.createArrayType(DataTypes.StringType));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        sqlContext.udf().register("fixContent", (WrappedArray<String> content) -> {

            // HTML tags not to delete (quotations)
            ArrayList<String> tagNotToDel = new ArrayList<>();
            tagNotToDel.add("<q>");
            tagNotToDel.add("<blockquote>");
            tagNotToDel.add("</q>");
            tagNotToDel.add("</blockquote>");

            HashMap<String, String> fixMap = new HashMap<>();
            fixMap.put("-LRB-", "(");
            fixMap.put("-RRB-", ")");
            fixMap.put("-LSB-", "[");
            fixMap.put("-RSB-", "]");

            String tagDel = "<";

            Iterator<String> iterator = content.iterator();
            List<String> tokens = new ArrayList<>();
            while (iterator.hasNext()) {
                String token = iterator.next();
                if (fixMap.containsKey(token)) {
                    tokens.add(fixMap.get(token));
                }

                if (tagNotToDel.contains(token)) {
                    tokens.add(token);
                }

                else if (token.startsWith(tagDel) && !tagNotToDel.contains(token)) {
                    continue;
                }
                // Splitting words with dash
                else if (token.length() > 1 && token.contains("-")) {
                    String[] b = token.split("-");
                    if (b.length > 1) {
                        tokens.add(b[0]);
                        tokens.add("-");
                        tokens.add(b[1]);
                    }
                    else {
                        tokens.add(token);
                    }
                }

                else {
                    tokens.add(token);
                }
            }
            return tokens;

        }, DataTypes.createArrayType(DataTypes.StringType));


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Reading articles
        String serverPath = "data/";
        // String serverPath = "/Users/lorenzotara/Documents/EPFL/Semestral_Project/find_author/data/";
        DataFrame articlesDF = sqlContext.read().json(serverPath + data_path + "/*.gz");

        // Taking only useful information from articlesDF
        DataFrame articlesIdContent = articlesDF.withColumn("articleUID", articlesDF.col("feedEntry.identifier"))
                .withColumn("content", articlesDF.col("permalinkEntry.tokenizedContent"))
                .select("articleUID", "content");

        // Reading output files
        DataFrame outputDF = sqlContext.read().json(serverPath + quootstrap_path);

        // Mapping from mapped dataset
        if (mapping.contentEquals("map")) {

            articlesIdContent = articlesIdContent
                    .withColumn("articleUIDArt", articlesIdContent.col("articleUID").cast(DataTypes.LongType))
            .drop("articleUID");

            // Flattening output on occurrences
            DataFrame outputDFFlattened = outputDF
                    .select(outputDF.col("speaker"), outputDF.col("canonicalQuotation"), functions.explode(outputDF.col("occurrences")))
                    .select("canonicalQuotation", "col.articleUID", "col.quotation", "col.extractedBy", "col.patternConfidence", "speaker");

            // Renaming columns
            outputDFFlattened = outputDFFlattened.withColumnRenamed("extractedBy", "targetPattern")
                    .withColumn("articleUID", outputDFFlattened.col("articleUID").cast(DataTypes.LongType))
                    .withColumnRenamed("articleUID", "targetID")
                    .withColumnRenamed("quotation", "targetQuotation")
                    .withColumnRenamed("patternConfidence", "confidence");

            // Reading the mapping file
            DataFrame mappingDF = sqlContext.read().json(serverPath + mappingPath);

            // Taking the articles of interest from the whole collection
            // DataFrame mergedDF = articles_id_content.join(mappingDF, "articleUID");
            DataFrame mergedDF = articlesIdContent.
                    join(mappingDF, mappingDF.col("articleUID").equalTo(articlesIdContent.col("articleUIDArt"))
                            , "inner")
                    .drop("articleUIDArt");

            // Merging the articles corresponding to the mapping to the "target" in order to get the speaker
            DataFrame combinedDF = outputDFFlattened.join(mergedDF, outputDFFlattened.col("canonicalQuotation")
                    .equalTo(mergedDF.col("canonicalQuotationDestination")));

            combinedDF.show();

            DataFrame mergedTokenized = combinedDF
                    .select("articleUID", "confidence", "content", "canonicalQuotationSource", "speaker")
                    .withColumnRenamed("canonicalQuotationSource", "quotation");


            // Taking away repetitions
            DataFrame groupedTokenized = mergedTokenized.groupBy("articleUID", "speaker", "quotation")
                    .agg(functions.first("content").as("content"),
                            functions.first("confidence").as("confidence"));


            // Fixing the content and tokenizing the quotation
            groupedTokenized = groupedTokenized
                    .select(groupedTokenized.col("articleUID"),
                            groupedTokenized.col("confidence"),
                            groupedTokenized.col("speaker"),
                            functions.callUDF("fixContent", groupedTokenized.col("content")),
                            functions.callUDF("tokenize", groupedTokenized.col("quotation")))
                    .withColumnRenamed("fixContent(content)", "content")
                    .withColumnRenamed("tokenize(quotation)", "quotation");

            groupedTokenized.write().parquet(serverPath + writePath);

        }

        // Extracting quootstrap articles
        else {
            // Flattening output on occurrences
            DataFrame outputDFFlattened = outputDF.select(outputDF.col("speaker"), functions.explode(outputDF.col("occurrences")))
                    .select("col.articleUID", "col.quotation", "col.extractedBy", "col.patternConfidence", "speaker");

            // Taking only articles of interest
            DataFrame joined = articlesIdContent.join(outputDFFlattened, "articleUID");

            // Fixing the content and tokenizing the quotations
            joined.select(joined.col("articleUID"),
                            joined.col("extractedBy"),
                            joined.col("patternConfidence"),
                            joined.col("speaker"),
                            functions.callUDF("fixContent", joined.col("content")),
                            functions.callUDF("tokenize", joined.col("quotation")))
                    .withColumnRenamed("fixContent(content)", "content")
                    .withColumnRenamed("tokenize(quotation)", "quotation")
                    .write().parquet(serverPath + writePath);
        }

    }
}