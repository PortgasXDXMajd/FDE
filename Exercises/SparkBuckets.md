# Exercise 3.6

## Task

Find song pairs with similarly familiar artists.
That is: `abs(a.artist_familiarity − b.artist_familiarity) < 0.001`.
How many pairs are there?

## Solution

First, we load the data:

```
val songs = spark.read.format("csv").option("header", true).option("inferSchema", "true").load("songs.csv").cache()
```

If we naïvely express the query using the Spark Dataset API, it will most likely not terminate in a reasonable time:

```
songs.as("a").join(songs.as("b"), abs($"a.artist_familiarity" - $"b.artist_familiarity") < 0.001).count()
```

Since the join condition contains an inequality (`<`), Spark will evaluate the query using the nested loop join algorithm:

```
== Physical Plan ==
CartesianProduct (abs((artist_familiarity#21 - artist_familiarity#4606)) < 0.001)
:- *(1) Filter isnotnull(artist_familiarity#21)
:  +- InMemoryTableScan [title#17, release#18, artist_name#19, duration#20, artist_familiarity#21, artist_hotttnesss#22, year#23], [isnotnull(artist_familiarity#21)]
:        +- InMemoryRelation [title#17, release#18, artist_name#19, duration#20, artist_familiarity#21, artist_hotttnesss#22, year#23], StorageLevel(disk, memory, deserialized, 1 replicas)
:              +- FileScan csv [title#17,release#18,artist_name#19,duration#20,artist_familiarity#21,artist_hotttnesss#22,year#23] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/leonard/fde_tutorium_2022/session_08/songs.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<title:string,release:string,artist_name:string,duration:double,artist_familiarity:double,a...
+- *(2) Filter isnotnull(artist_familiarity#4606)
   +- InMemoryTableScan [title#4602, release#4603, artist_name#4604, duration#4605, artist_familiarity#4606, artist_hotttnesss#4607, year#4608], [isnotnull(artist_familiarity#4606)]
         +- InMemoryRelation [title#4602, release#4603, artist_name#4604, duration#4605, artist_familiarity#4606, artist_hotttnesss#4607, year#4608], StorageLevel(disk, memory, deserialized, 1 replicas)
               +- FileScan csv [title#17,release#18,artist_name#19,duration#20,artist_familiarity#21,artist_hotttnesss#22,year#23] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/leonard/fde_tutorium_2022/session_08/songs.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<title:string,release:string,artist_name:string,duration:double,artist_familiarity:double,a...
```

By looking at the pseudo-code, you see that this algorithm has quadratic time complexity:

```
counter = 0
for song_a in songs:
	for song_b in songs:
		if abs(song_a.artist_familiarity − song_b.artist_familiarity) < 0.001:
			counter += 1
return counter
```

Quadratic time complexity is very problematic.
**Let's rewrite the query so that Spark can evaluate it in linear time complexity.**

Let's say this is an excerpt of our data:

| Name         | Familiarity |
| ------------ | ----------- |
| Rihanna      | 10.0055     |
| Taylor Swift | 10.0064     |
| Beyoncé      | 10.0046     |
| Adele        | 10.0066     |
| Lady Gaga    | 10.0044     |
| Miley Cyrus  | 10.0039     |

By looking at the data, you see that Rihanna is similarly familiar as Taylor Swift and Beyoncé, but not similarly familiar as Adele, Lady Gaga, and Miley Cyrus.

Rihanna and Miley Cyrus are obviously not similarly familiar.
You can figure that out by looking at the third digit after the decimal separator.
For Rihanna, it's 5, and for Miley Cyrus, it's 3.
There is no way those two values can be less than 0.001 apart.
Even if Rihanna had a familiarity value of 10.0050, the two values would still have an absolute difference of 0.0011.

For Rihanna and Adele, it's not so obvious because the third digit after the decimal separator would still allow for the inequality (`< 0.001`) to hold.
We additionally have to look at the fourth digit after the decimal separator.
Only then do we see that the difference is too large.

We learned something interesting.
If the absolute difference between the familiarity values of two artists up to the third digit after the decimal separator is too large, the join condition cannot hold.
How can we cut off all digits following the third digit after the decimal separator?
Easy: divide the number by `0.001` and then truncate it using the `floor` function.

| Name         | Familiarity | floor(Familiarity / 0.001) AS bucket |
| ------------ | ----------- | ------------------------------------ |
| Rihanna      | 10.0055     | 10005                                |
| Taylor Swift | 10.0064     | 10006                                |
| Beyoncé      | 10.0046     | 10004                                |
| Adele        | 10.0066     | 10006                                |
| Lady Gaga    | 10.0044     | 10004                                |
| Miley Cyrus  | 10.0039     | 10003                                |

Since we need to reference that table later, let's call it A.

If the absolute difference between two `bucket` values is greater than or equal to 2, the two artists cannot be similarly familiar. Just look at Rihanna and Miley Cyrus to illustrate this point.

Consequently, Rihanna can only be similarly familiar to artists having a bucket value of 10004, 10005, or 10006.
But this condition alone is not sufficient as you can see by comparing Rihanna and Adele: even though their bucket values look similar, the actual familiarity values are too far apart.

| Name         | Familiarity | floor(Familiarity / 0.001) AS bucket | array(bucket - 1, bucket, bucket + 1) AS neighbors |
| ------------ | ----------- | ------------------------------------ | -------------------------------------------------- |
| Rihanna      | 10.0055     | 10005                                | [10004, 10005, 10006]                              |
| Taylor Swift | 10.0064     | 10006                                | [10005, 10006, 10007]                              |
| Beyoncé      | 10.0046     | 10004                                | [10003, 10004, 10005]                              |
| Adele        | 10.0066     | 10006                                | [10005, 10006, 10007]                              |
| Lady Gaga    | 10.0044     | 10004                                | [10003, 10004, 10005]                              |
| Miley Cyrus  | 10.0039     | 10003                                | [10002, 10003, 10004]                              |

Working with arrays is complicated. Therefore, we want to flatten the array using the `explode` function. Here is what it does:

| Name         | Familiarity | explode(neighbors) AS bucket |
| ------------ | ----------- | ---------------------------- |
| Rihanna      | 10.0055     | 10004                        |
| Rihanna      | 10.0055     | 10005                        |
| Rihanna      | 10.0055     | 10006                        |
| Taylor Swift | 10.0064     | 10005                        |
| Taylor Swift | 10.0064     | 10006                        |
| Taylor Swift | 10.0064     | 10007                        |
| Beyoncé      | 10.0046     | 10003                        |
| Beyoncé      | 10.0046     | 10004                        |
| Beyoncé      | 10.0046     | 10005                        |
| Adele        | 10.0066     | 10005                        |
| Adele        | 10.0066     | 10006                        |
| Adele        | 10.0066     | 10007                        |
| Lady Gaga    | 10.0044     | 10003                        |
| Lady Gaga    | 10.0044     | 10004                        |
| Lady Gaga    | 10.0044     | 10005                        |
| Miley Cyrus  | 10.0039     | 10002                        |
| Miley Cyrus  | 10.0039     | 10003                        |
| Miley Cyrus  | 10.0039     | 10004                        |

Since we need to reference that table later, let's call it B.
Please note that flattening an array does not come for free.
We now have thrice as many rows.

I now show you the rewritten query using the Spark Dataset API:

```
val songsBuck = songs.withColumn("fam_bucket", floor($"artist_familiarity" / 0.001)).cache()
val songsBuckNeighbors = songsBuck.withColumn("fam_bucket", explode(array($"fam_bucket" - 1, $"fam_bucket", $"fam_bucket" + 1)))
songsBuck.as("a").join(songsBuckNeighbors.as("b"), ($"a.fam_bucket" === $"b.fam_bucket") && abs($"a.artist_familiarity" - $"b.artist_familiarity") < 0.001).count()
```

See how we join table A and table B showed above using an equality condition on the `bucket` column.
The great thing about the rewritten query is that Spark can evaluate it using a hash join followed by a filter:

```
# Build the hash table which maps a fam_bucket value
# to a list of songs having this fam_bucket value
for song in songsBuck:
	hash_table[song.fam_bucket].append(song)

counter = 0
for song_b in songsBuckNeighbors:
	for song_a in hash_table[song_b.fam_bucket]:
		if abs(song_a.artist_familiarity − song_b.artist_familiarity) < 0.001:
			counter += 1

return counter
```

The rewritten query produces the same result as the original query.
But thanks to the equality condition in the join predicate, we reduced the number of songs we have to compare by a significant factor.
Now, Spark can evaluate the query in a reasonable time.

## Addendum

The result of the rewritten query is actually not equal to the result of the original query.
In the original query, Rihanna is similarly familiar as Rihanna, and this is counted once.
In the rewritten query, this is counted thrice. But let's ignore that.
