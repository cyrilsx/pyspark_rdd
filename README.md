
# Spark Operation
 
The first step is to initiate Spark using `SparkContext` and `SparkConf`. The configuration allows to give parameter to the job. There are 3 parameters you will always need:
     * Master node
     * Application name
     * JVM configurations (such as set memory size for workers)
 


```python
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

conf = SparkConf().setMaster("local").setAppName("Spark Operation")
sc = SparkContext(conf=conf)
```

Loading Data
--
In order to load data, `SparkContext` provides the following methods:
    * textFile(pathToFile)
    * parallelize(collection)

Most of the time, `parallelize` is used only for debugging. `textFile` can load files from local system, from HDFS and from S3.

In this notebook, data will be loaded using `parallelize`.
In the snippet below, the action `first()` returns the first element of the RDD.


```python
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd.first()
```




    1



Transformation
--

Transformations return another RDD from the first one. Actions compute a result based from an RDD.

Transformations are _lazy_. This means that when you call a transformation, nothing will happen until an action is performed. 


```python
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd.filter(lambda x: x % 2 == 0) # Nothing actually happens
```

So nothing happened. To illustrate transformation, the action `collect` will be used. 
`collect` returns the RDD as a list

Filter
--

`filter` takes a predicate and return an RDD with all elements matching the predicate.


```python
# collect
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd.collect()

rdd.filter(lambda x: x % 2 == 0).collect()
```




    [2, 4, 6, 8, 10]



Map
--
`map` transform RDD's elements to another RDD.


```python
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd.map(lambda n: "%s element" % str(n)).collect()
```




    ['1 element',
     '2 element',
     '3 element',
     '4 element',
     '5 element',
     '6 element',
     '7 element',
     '8 element',
     '9 element',
     '10 element']




```python
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd.map(lambda n: n * 2).collect()
```




    [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]



FlatMap
--
`flatMap` transform and flatten an RDD with a function 


```python
rdd = sc.parallelize([1,2,3])
rdd.flatMap(lambda n: [n, n * 2, n * 3]).collect()
```




    [1, 2, 3, 2, 4, 6, 3, 6, 9]



Distinct
---
Remove duplicates


```python
rdd = sc.parallelize(["bien", "ou", "bien", "?"])
rdd.distinct().collect()
```




    ['bien', 'ou', '?']



Sample
---
Provides a data sample from the RDD.
This method has 2 arguments:
    - with replacement or not
    - fraction


```python
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd.sample(False, 0.5).collect()
```




    [3, 4, 9, 10]



Transformation between RDD
---
These transformation allows the user to play with sets.
The parameter of all these methods is another RDD.

Union
---
Produce an RDD containing elements of both RDD



```python
rdd_a = sc.parallelize([1,2,3])
rdd_b = sc.parallelize([3,4,5,6])
rdd_a.union(rdd_b).collect()
```




    [1, 2, 3, 3, 4, 5, 6]



Intersection
---
Produce an RDD containing elements found in both RDD



```python
rdd_a = sc.parallelize([1,2,3])
rdd_b = sc.parallelize([3,4,5,6])
rdd_a.intersection(rdd_b).collect()
```




    [3]



Sustract
---
Produce an RDD that remove all elements of one RDD


```python
rdd_a = sc.parallelize([1,2,3,4])
rdd_b = sc.parallelize([3,4,5,6])
rdd_a.subtract(rdd_b).collect()
```




    [2, 1]



Cartesian
---
Cartesian product with another RDD


```python
rdd_a = sc.parallelize([1,2,3])
rdd_b = sc.parallelize([3,4,5])
rdd_a.cartesian(rdd_b).collect()
```




    [(1, 3), (1, 4), (1, 5), (2, 3), (2, 4), (2, 5), (3, 3), (3, 4), (3, 5)]



Action
---
The only action used until now is `collect`. 

Reduce
---
The most common action is `reduce`. 
`reduce` takes 2 elements of the RDD and return only one of the same type.



```python
rdd = sc.parallelize([1,2,3,4])
rdd.reduce(lambda a, b: a + b)
```




    10



Fold
---
`fold` is similar to `reduce` but in addition, a _zero value_ must be provided. In mathematics, this _zero value_ is called identity element. 

e.g: 
    *  +, identity element 0
    *  x, identity element 1
    *  collections, identity element empty collections

NB: The identity element is applied foreach partition in parallel.



```python
rdd = sc.parallelize([1,2,3,4])
rdd.fold(0, lambda a, b: a + b)
```




    10




```python
rdd = sc.parallelize([1,2,3,4])
rdd.fold(1, lambda a, b: a * b)
```




    24



Aggregate
---
`fold` and `reduce` always return the same type. `aggregate` combines and reduces.
The signature of aggregate:
    1. The identity element
    2. The operation to apply for each record
    3. The combine function is applied for each partition as local result at first, then for the global result to combine result for all partitions.
    
NB: The second argument in `parallelize` is the number of partition. 


```python
rdd = sc.parallelize([1,2,3,4], 2)
rdd.aggregate((0, 0), \
              lambda local_result, current_value: (local_result[0] + current_value, local_result[1] + 1), \
              lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
```




    (10, 4)



First
---
`first` returns the first element of a RDD


```python
rdd = sc.parallelize([4,3,2,1])
rdd.first()
```




    4



Count
---
`count` returns the RDD's size


```python
rdd = sc.parallelize([1,2,3,4,5,6])
rdd.count()
```




    6



Take & Take Ordered
---
`take` return n elements from a RDD
`takeOrdered` return n element from a RDD based on the provided ordering (naturalOrder by default)
`top` return the top number of element


```python
rdd = sc.parallelize([1,2,3,4,5,6])
rdd.take(3)
```




    [1, 2, 3]




```python
rdd = sc.parallelize([4,2,1,5,6,3,7])
rdd.takeOrdered(3)
```




    [1, 2, 3]




```python
rdd = sc.parallelize([4,2,1,5,6,3,7])
rdd.takeOrdered(3, key = lambda x: -x)
```




    [7, 6, 5]




```python
rdd = sc.parallelize([4,2,1,5,6,3,7])
rdd.top(3)
```




    [7, 6, 5]



Take sample
---
`takeSample` return a number of random element 


```python
rdd = sc.parallelize([1,2,3,4,5,6])
rdd.takeSample(False, 3)
```




    [2, 1, 4]



Count by value
---
`countByValue` return the number of time each element occurs in the RDD


```python
rdd = sc.parallelize([1,2,2,3,4,3,4])
rdd.countByValue()
```




    defaultdict(int, {1: 1, 2: 2, 3: 2, 4: 2})



Foreach
---
`foreach` applies a function to each element of an RDD.
NB: `foreach` returns nothing.


```python
rdd = sc.parallelize([1,2,2,3,4,3,4])
rdd.foreach(lambda x: print("this -> %s"%x, end=' ')) # show nothing :-)
```

Other actions
---
Other actions are available depending of the RDD's type.
See below a non exhausting list (for numerical RDD):
    * mean
    * variance


```python
rdd = sc.parallelize([1,2,2,3,4,3,4])
rdd.variance()
```




    1.0612244897959184


