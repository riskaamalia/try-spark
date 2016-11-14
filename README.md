# try-spark
learning using spark

//belajar spark

SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
JavaSparkContext sc = new JavaSparkContext(conf);

class Contains implements Function<String, Boolean>() {
private String query;
public Contains(String query) { this.query = query; }
public Boolean call(String x) { return x.contains(query); }
}
RDD<String> errors = lines.filter(new Contains("error"));
//ini di java 8
RDD<String> errors = lines.filter(s -> s.contains("error"));


//ini lanjutan setelah sc spark content
JavaRDD<String> input = sc.textFile(inputFile);
// Split up into words.
JavaRDD<String> words = input.flatMap(
new FlatMapFunction<String, String>() {
public Iterable<String> call(String x) {
return Arrays.asList(x.split(" "));
}});
// Transform into pairs and count.
JavaPairRDD<String, Integer> counts = words.mapToPair(
new PairFunction<String, String, Integer>(){
public Tuple2<String, Integer> call(String x){
return new Tuple2(x, 1);
}}).reduceByKey(new Function2<Integer, Integer, Integer>(){
public Integer call(Integer x, Integer y){ return x + y;}});
// Save the word count back out to a text file, causing evaluation.
counts.saveAsTextFile(outputFile);

//FILTER itu bisa filter per string, misal cari txt yang ada kata error, pake filter,
//terus filter juga bisa di union, dua filter cari yang error dan failed, union deh
//ini suka paralize
JavaRDD<String> lines = sc.parallelize(Arrays.asList("pandas", "i like pandas"));

//ini contoh untuk contain yang error
JavaRDD<String> inputRDD = sc.textFile("log.txt");
JavaRDD<String> errorsRDD = inputRDD.filter(
new Function<String, Boolean>() {
public Boolean call(String x) { return x.contains("error");
//ini kayaknya bisa di taro gini
errorsRDD = inputRDD.filter(lambda x: "error" in x)
warningsRDD = inputRDD.filter(lambda x: "warning" in x)
badLinesRDD = errorsRDD.union(warningsRDD)

 }
}
});

//ada beda flatmap ama map, yang kalo flatMap itu dicampur semua
//terus ada distinct, intersection, union juga
//ada reduce yg 2 parameter + terus return di parameter ke 3
//agregat di spark itu mungkin jumlah
//ada konversi ke double juga
// ada key value juga, bisa di filter
//ini yg filter key value
Function<Tuple2<String, String>, Boolean> longWordFilter =
new Function<Tuple2<String, String>, Boolean>() {
public Boolean call(Tuple2<String, String> keyValue) {
return (keyValue._2().length() < 20);
}
};
JavaPairRDD<String, String> result = pairs.filter(longWordFilter);
//paralel yg pake scala tapi

val result = input.map(x => x * x)
result.persist(StorageLevel.DISK_ONLY)
println(result.count())
println(result.collect().mkString(","))

//aneka input file
JavaRDD<String> input = sc.textFile("file:///home/holden/repos/spark/README.md")
