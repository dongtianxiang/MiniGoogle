# CIS555 Course Project #

### Team member:# 
name 		|   seas login 	    |	division of labor 
--------------- | ----------------- |
Yifan Li:	|   lyifan	    |	PageRank
Tianxiang Dong: |   dtianx	    |	Crawler
Hanyu Yang:     |   hany	    |	Indexer
Yunwen Deng: 	|   dyunwen  	    |	Search Engine and User Interface   

### Implemented Features ###
We built a distributed and scalable search engine with distributed data storage system.
 
1. The crawler adopts the Mecartor style and is able to run distributedly and multithreading.

2. Indexer indexes on monogram, bigram and trigram words from the crawled documents and runs MapReduce job with Apache Hadoop to construct lexicon storage and inverted index storage. The storage is then partitioned into 5 machines with effort to balance and avoid data skew. 

3. Page rank engine builds revised web link graph then runs MapReduce job to do iterations of computation with Apache Spark. We consider 15 iteration as the point to converge.

4. All data, including inverted index, lexicon and pagerank scores, is partitioned and stored in local Berkeley DB on 5 machines distributiedly. All original documents are stored in Amazon S3.

5. The search engine retrieves data from storage worker servers directly when a query request arrives. Internally, it runs MapReduce job with Apache Spark to rank and sort and then output the final results to the search engine. The search engine renders result pages back to user.

### Extra Credits ###
1. Pagerank Mapreduce job was done with ApacheSpark.

2. We implemented fault tolerance with possible storage machine failure, meaningly if one or two machines are down during searching time, the search engine is still working and provides reliable searching results as well.

3. We implemented autocompletion feature to the search engine user interface.

### The main user interface ###
![Screen Shot 2016-12-22 at 12.37.00 PM.png](https://bitbucket.org/repo/KEGKdX/images/2204763337-Screen%20Shot%202016-12-22%20at%2012.37.00%20PM.png)