# Spark-Processing

The main script executes two different functions as explained below.

Main parameters:

0) data_path: path to the dataset containing all Spinn3r articles
1) quootstrap_path: path to quootstrap output (e.g.: quootstrap-quotation-dataset.json)
2) write_path: path to folder where to save .parquet files (new_merged_tokenized)
3) mapping (optional): write `map` as 4th argument if you want to map discarded (quotation, speaker) pairs from quootstrap output
4) mapping file (optional): only if mapping, path to the file containing the mapping of discarded files (merge_mapping.json.gz)

`quootstrap-quotation-dataset.json` columns:
* `speaker`: String, speaker of the quotation
* `occurrences`: List, list containing the different articleUIDs, quotations, patterns and pattern_confidence
* `canonicalQuotation`: canonical quotation that refers to all quotations in occurrences

### 1) Extract articles from dataset (no mapping)

Retrieve content of articles contained in `quootstrap_path`. Save the files in `write_path`.<br>
Structure of final file:
* `articleUID`: unique id of the article
* `extractedBy`: pattern used to extract the (quotation, speaker) pair
* `patternConfidence`: confidence given to the pattern
* `speaker`: speaker of the quotation
* `content`: content of the article
* `quotation`: quotation retrieved by the Quootstrap algorithm

### 2) Map discarded (quotation, speaker) pairs to articles

`mapping` argument = `map`<br>
`mapping file` argument = dataset containing the mapping (e.g.: `merge_mapping.json.gz`)<br>

`merge_mapping.json.gz` columns:
* `articleUID`: unique id of the article
* `canonicalQuotationDestination`: destination quotation of the mapping (quotation used to map). This quotation is equal to canonicalQuotation of the quootstrap file
* `confidence`: confidence of the pattern that extracted the (quotation, speaker) pair
* `canonicalQuotationSource`: real quotation found in the document

Retrieve content of articles contained in `mapping file`. Save the files in `write_path`.<br>
Structure of final file: same as in 1)

