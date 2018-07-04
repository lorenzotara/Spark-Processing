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
import scala.Array;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


/**
 * Created by lorenzotara on 04/07/18.
 */
public class ProcessAllMain {

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // Path to all articles
        // String data_path = args[0];
        String data_path = "/Users/lorenzotara/Documents/EPFL/Semestral_Project/find_author/data/dataset";

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

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        sqlContext.udf().register("findQuotations", (WrappedArray<String> content) -> {
            // Return ArrayType allQuotationsArray

            ArrayList<String> startTokens = new ArrayList<>();
            startTokens.add("``");
            startTokens.add("<q>");
            startTokens.add("<blockquote>");

            ArrayList<String> endTokens = new ArrayList<>();
            endTokens.add("''");
            endTokens.add("</q>");
            endTokens.add("</blockquote>");



            int minLength = 8;
            int maxLength = 100;

            Iterator<String> iterator = content.iterator();
            boolean inQuot = false;
            int startQuot = 0;
            int index = 0;
            List<List<Integer>> allQuotations = new ArrayList<>();
            while (iterator.hasNext()) {
                String token = iterator.next();

                if (startTokens.contains(token)) {
                    inQuot = true;
                    startQuot = index;
                }

                if (endTokens.contains(token) && inQuot) {
                    inQuot = false;
                    int lenQuot = index - startQuot;

                    if (lenQuot >= minLength && lenQuot < maxLength) {
                        List<Integer> tuple = new ArrayList<>();
                        tuple.add(startQuot);
                        tuple.add(index);
                        allQuotations.add(tuple);
                    }

                }
                index++;
            }

            return allQuotations;

        }, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType)));


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        sqlContext.udf().register("tagQuotation",
                (WrappedArray<String> content,
                 WrappedArray<WrappedArray<Integer>> quotationIndexes,
                 WrappedArray<Integer> quotationIndex) -> {

            // Copying content to array
            int contentLength = content.length();
            String[] tmpTokens = new String[contentLength];
            content.copyToArray(tmpTokens, 0, contentLength);
            List<String> tokens = Arrays.asList(tmpTokens);

            // Copying input quotation indexes to array
            Integer[] tmpInputQuotationIndex = new Integer[2];
            quotationIndex.copyToArray(tmpInputQuotationIndex, 0, 2);

            List<Integer> inputQuotationIndex = Arrays.asList(tmpInputQuotationIndex);

            List<List<Integer>> allIndexes = new ArrayList<>();
            Iterator<WrappedArray<Integer>> iterator = quotationIndexes.iterator();
            // all indexes to an array list of lists
            while (iterator.hasNext()) {
                WrappedArray<Integer> startEnd = iterator.next();
                Iterator<Integer> innerIterator = startEnd.iterator();

                ArrayList<Integer> singleQuotation = new ArrayList<>();

                while (innerIterator.hasNext()) {
                    singleQuotation.add(innerIterator.next());
                }

                allIndexes.add(singleQuotation);
            }

            ArrayList<String> newContent = new ArrayList<>();
            ArrayList<String> inputQuotation = new ArrayList<>();

            int i = 0;
            for (List<Integer> quotation : allIndexes) {
                Integer start = quotation.get(0);
                Integer end = quotation.get(1);
                while (i < start) {
                    newContent.add(tokens.get(i));
                    i++;
                }
                boolean isInputQuotation = inputQuotationIndex.contains(start) && inputQuotationIndex.contains(end);
                if (isInputQuotation) {
                    newContent.add("<INPUT_QUOTATION>");
                }
                else {
                    newContent.add("<QUOTATION>");
                }

                // Skip <blockquote>
                i++;
                if (isInputQuotation) {
                    while (i < end ) {
                        inputQuotation.add(tokens.get(i));
                        i++;
                    }
                    // Skip </blockquote>
                    i++;
                }

                else {
                    i = end + 1;
                }
            }

            while (i < tokens.size()) {
                newContent.add(tokens.get(i));
                i++;
            }

            List<List<String>> returnList = new ArrayList<>();
            returnList.add(newContent);
            returnList.add(inputQuotation);

            return returnList;

        }, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Reading articles
        // String serverPath = "data/";
        String serverPath = "";
        DataFrame articlesDF = sqlContext.read().json(serverPath + data_path + "/*10.gz")
                .sample(false, 0.2);

        // Taking only useful information from articlesDF
        DataFrame articlesIdContent = articlesDF.withColumn("articleUID", articlesDF.col("feedEntry.identifier"))
                .withColumn("content", articlesDF.col("permalinkEntry.tokenizedContent"))
                .select("articleUID", "content");

        articlesIdContent.printSchema();

        articlesIdContent = articlesIdContent
                .withColumn("content", functions.callUDF("fixContent", articlesIdContent.col("content")));
        articlesIdContent = articlesIdContent.
                withColumn("quotationIndexes", functions.callUDF("findQuotations", articlesIdContent.col("content")));
        articlesIdContent = articlesIdContent
                .withColumn("quotationIndex", functions.explode(articlesIdContent.col("quotationIndexes")));

        articlesIdContent = articlesIdContent
                .withColumn("taggedContent",
                        functions.callUDF("tagQuotation",
                                articlesIdContent.col("content"),
                                articlesIdContent.col("quotationIndexes"),
                                articlesIdContent.col("quotationIndex")
                                ));

        articlesIdContent = articlesIdContent
                .withColumn("content", articlesIdContent.col("taggedContent").getItem(0))
                .withColumn("quotation", articlesIdContent.col("taggedContent").getItem(1))
                .drop("quotationIndexes")
                .drop("taggedContent")
                .drop("quotationIndex");


        articlesIdContent.show(20);
        articlesIdContent.write().json("Tokenized");

    }
}