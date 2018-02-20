# Page Rank algorithm in Map Reduce

This is an implementation of the page rank algorithm [1] in map reduce specifically used for the wikipedia article data set.

## Assumptions

For this task I define below my assumptions that my solution is based on.

### Input

1. The revision data will always be 14 lines separated with one single empty line. ('\n\n')
2. The order of the lines of each revision will always be the same.
3. There are multiple revisions for the same article
4. Valid dates are of the format yyyy-MM-dd'T'HH:mm:ss'Z'
5. The list of outgoing links in MAIN tag may contain duplicates
6. Outgoing links for the revision are not only separated by spaces but there are also tabs or double spaces

### Output

1. Results do not have to be sorted
2. Output is of the format: Article_Title   Page_Rank

### Processing

1. The page rank is calculated based on the latest revision of each article. ( Variant 3 )

## Structure

This project consists of 3 map reduce jobs to adapt the dataset requirements and execute the page rank algorithm in an iterative fashion.

The jobs are executed in the following order:

1. Parsing Job
2. PageRank Calculation Job
3. Format Output Job

I chose to create three jobs rather than one to simplify the problem and separate each phase. Also, due to the iterative nature of the task, the page rank calculation job must by able to run multiple times and as such, the input must be in the same format as the output. Thus is reasonable to use a different job for formatting the final output.

### Parsing Job

This job is responsible for parsing the data from the data and output them in a format that can be used in an iterative manner. 

The wikipedia dataset consists of records that represent revisions of an article. A record is represented in 14 lines where each line is tagged with the appropriate information. Here we are only interested in the REVISION line which contains the article_id and MAIN which denotes the outgoing links to other article titles from the current one. The last line of each record is empty and there is also one line left blank between records and as such we can confidently use the '\n\n' for identifying records for the mapper. An alternative approach would be to always read the next 14 lines but the double line delimiter approach seems more resilient to inconsistent data.

The parsing job works with the assumption that we want to calculate the page rank by taking into consideration only the latest revision of each article.

#### Mapper

```
Input: (key: Position of file reading in bytes<LongWritable>, value: Article revision record<Text>)

Output: (key: Article Title, value: Revision_Date###Outgoing_Article Title<Text>)
```

In this step I parse each article revision using the default TextInputFormat but using a custom delimiter ('\n\n') and for each revision I emit a key value pair of the current article title and the revision date joined with the outgoing links using the string '###'.

NOTE: '###' is just my personal preference for a delimiter but something else unique string could be used instead.

### Combiner

```
Input: (key: Article_Title<Text>, value: Revision_Date###Outgoing_Article Titles<Iterable<Text>>)

Output: (key: Article_Title<Text>, value: Revision_Date###Outgoing_Article_Titles<Text>)
```

In the combiner step I iterate through the dates and only emit the latest date locally for that mapper so that the traffic to the reducer decreased and since some of the dates are partially filtered out.

#### Reducer

```
Input: (key: Article_Title<Text>, value: Revision_Date###Outgoing_Article_Titles<Iterable<Text>>)

Output: (key: Article_Title<Text>, value: 1.0###Space delimited_outgoing_article_titles<Text>)
```

Initially I iterate through the revision dates to find the latest revision and thus store the latest outgoing links.

Then, since we expect to have duplicate outgoing article titles so I used Java's Set data structure to keep a distinct set of the outgoing links from the latest revision per article title.

Finally when I have the set I use join from StringUtils to create a space delimited text of the outgoing links and I emit the following key value pair:

```
1.0###<Space_delimited_Outgoing_Links_String>
```

To clarify, the 1.0 here is the initial default page rank for the article and the '###' is just my unique delimiter that I used to separate the page rank from the links.

One may argue why I joined the outgoing links with space and not just output them as they are since they are already separated with spaces. This is because of the assumption that there could be double spaces or tabs which we want to eliminate before passing to next jobs.

### Page Rank Calculation Job

This job is responsible for calculating the new page rank for each article title. Given that, the output must be in the same format as the input and as such it can run multiple times. 

#### Mapper

```
Input: (key: Article_Title<Text>, value: PageRank###Outgoing Articles<Text>)

Output: (key: Outgoing_Article_Title, value:  PageRank_Contribution<Text>)

OR 

Output: (key: Article_Title, value: $LINKSOUT$<Outgoing Links>)
```

In this step the mapper splits the outgoing article and for each one emits a key value pair with the outgoing article title as the key and the Page Rank contribution as the value.

The contribution is calculated by : pageRank/NumberOfOutLinks

This will allow me to aggregate all the contributions of each link to the page rank in the reducer and combiner phase. However as shown above , there is another output format. The second one is to restore the original outgoing links for the current article since we are breaking them down for the next job. To pass the outgoing articles to the next job I emit just once, a key value pair of the current article with its outgoing links which includes a special identifier ("$LINKSOUT$") in order to differentiate it from the contribution emission.

#### Combiner

```
Input: (key: Outgoing_Article_Title<Text>, value: PageRank_Contribution<Iterable<Text>>)

OR 

Input: (key: Article_Title<Text>, value: $LINKSOUT$Outgoing_Links<Iterable<Text>>)


Output: (key: Outgoing_Article_Title, value:  PageRank_Contribution<Text>)

OR 

Output: (key: Article_Title, value: $LINKSOUT$<Outgoing Links>)
```

Again a combiner is used to reduce the data sent to the reducer by summing up all the contributions locally per mapper before sending them to the reducers.

When the combiner encounters the $LINKSOUT$ identifier it just reemits it as is.

#### Reducer

```
Input: (key: Outgoing_Article_Title<Text>, value: PageRank_Contribution<Iterable<Text>>)

OR 

Input: (key: Article_Title<Text>, value: $LINKSOUT$Outgoing_Links<Iterable<Text>>)

Output: (key: Article_Title<Text>, value: PageRank###Outgoing_Articles<Text>)
```

In this reduce step all contributions from each ingoing link are summed up and the final page rank is calculated based on the following formula:

```
Given a set S of links, in the form: <source article> <target article>, the PageRank score value for any page u can be expressed as:

PR(u)=0.15 + 0.85 * Sum(PR(v)/L(v)), ∀v: ∃(v,u) ∈S, where L(v) is the number of out-links of page v.
```

When the reducer encounters the $LINKSOUT$ identifier it skips the calculation part and appends the maintained outgoing articles to the current output for the next job.

### Format Output Job

This is a simple map only job which is responsible for formatting the output to the required format.

#### Mapper

```
Input: (key: Article_Title<Text>, value: PageRank###Outgoing Articles<Text>)

Output: (key: Article_Title<Text>, value: PageRank<Text>)
```

This mapper simply re emits the key value pair that was received without the outgoing articles.


## Optimizations

Below you can find the optimizations used for improving the performance of teh jobs.

1. Compression

Although, compressing the data before emitting them add some CPU overhead it worths the effort since it reduces the disk IO during shuffle.

When using compression it was noted a significant decrease in the File System Counters (Number of bytes read, Number of bytes written)

Compression was enabled by setting the `mapreduce.map.output.compress` flag to true in the job configuration

2. Reusable Writables

For reducing the heap space used by the jobs I utilized reusable writables rather than creating new Writables in each job.

Although Java's garbage collection does a very good job for that matter I thought that this would improve performance on large datasets with many iterations.

This has been achieved by definining the key and value Writables as class variables and reusing them using the `set` method

3. Combiners

The use of combiners improved the performance significantly as it reduced the data sent to the reducer for processing.

Therefore decreased CPU time was noted on the reducers when combiners were utilised.

## References:

[1] http://en.wikipedia.org/wiki/PageRank

