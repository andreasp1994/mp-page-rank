# Page Rank algorithm in Map Reduce

This is an implementation of the page rank algorithm [1] in map reduce specifically used for the wikipedia article data set.

## Structure

This project consists of 3 map reduce jobs to adapt the dataset requirements and execute the page rank algorithm in an iterative fashion.

The jobs are executed in the following order:

1. Parsing Job
2. PageRank Calculation Job
3. Format Output Job

### Parsing Job

This job is responsible for parsing the data from the data and output them in a format that can be used in an iterative manner. 

The wikipedia dataset consists of records that represent revisions of an article. A record is represented in 14 lines where each line is tagged with the appropriate information. Here we are only interested in the REVISION line which contains the article_id and MAIN which denotes the outgoing links to other article titles from the current one. The last line of each record is empty and there is also one line left blank between records and as such we can confidently use the '\n\n' for identifying records for the mapper. An alternative approach would be to always read the next 14 lines but the double line delimiter approach seems more resilient to inconstistent data.

The parsing job works with the assumption that we want to calculate the page rank by taking into consideration all the revisions of the same article not just the latest one.

#### Mapper

```
Input: (key: Position of file reading in bytes, value: Article revision record)

Output: (key: Article Title, value: Outgoing Article Title)
```

In this step I parse each article revision and for each one revision I iterate through the outgoing links and emit a key value pair of the current article title and the outgoing one.

#### Reducer

```
Input: (key: Article Title, value: Space delimited text of outgoing links to article titles)

Output: (key: Article Title, value: 1.0###Space delimited text of outgoing links to article titles)
```

In this step we expect to have duplicated key value pairs for outgoing article titles ( due to the multiple revisions ) I used Java's Set data structure to keep a distrinct set of the intersection of all outgoing links from each revistion per article title.

When I gather all the outgoing articles for each article_title I output them in the following format.

```
1.0###<Space delimited Outgoing Links String>
```

To clarify, the 1.0 here is the initial default page rank for the article and the '###' is just my unique delimeter that I use to separate the page rank from the links.

NOTE: It is important to state here that this work with the assumption that article_titles do not contain spaces!

### Page Rank Calculation Job

This job is responsible for calculating the new page rank for each article title. Given that, the output must be in the same format as the input and as such it can run multiple times. 

#### Mapper

```
Input: (key: Article Title, value: <PageRank>###<Outgoing Articles>)

Output: (key: Outgoing Article Title, value: < PageRank>###<OutgoingLinkLength>###<From Article Title>)

OR 

Output: (key: Article Title, value: $LINKSOUT$<Outgoing Links>)
```

In this step the mapper splits the outgoing article and for each one emits a key value pair with the outgoing article title as the key and the Page Rank, The number of outgoing articles from the current article and lastly the current article as the value.

This will allow me to calcuate the contribution of each link to the page rank in the reducer phase. However as shown above , there is another output format. The second one is to restore the original outgoing links for the current article since we are breaking down for the next job. To pass the outgoing articles to the next job I just emit once a key value pair of the current article with its outgoing links which includes a special identifier ("$LINKSOUT$") in order to diferentiate it from the other mapper emittion.

#### Reducer

```
Input: (key: Outgoing Article Title, value: < PageRank>###<OutgoingLinkLength>###<From Article Title>)

OR 

Input: (key: Article Title, value: $LINKSOUT$<Outgoing Links>)

Output: (key: Article Title, value: <PageRank>###<Outgoing Articles>)
```

In this reduce step all contributions from each ingoing link is summed up and the final page rank is calcualted based on the following formula:

```
Given a set S of links, in the form: <source article> <target article>, the PageRank score value for any page u can be expressed as:

PR(u)=0.15 + 0.85 * Sum(PR(v)/L(v)), ∀v: ∃(v,u) ∈S, where L(v) is the number of out-links of page v.
```

When the reducer encounters the $LINKSOUT$ identifier it skips the calculation part and appends the maintained outgoing articles to the current output for the next job.

### Format Output Job

This is a simple map only job which is responsible for formatting the output to the required format.

#### Mapper

```
Input: (key: Article Title, value: <PageRank>###<Outgoing Articles>)

Output: (key: Article Title, value: <PageRank>)
```

This mapper simply re emits the key value pair that was received without the outgoing articles.


## Optimizations

TO BE ADDED 


## References:

[1] http://en.wikipedia.org/wiki/PageRank

