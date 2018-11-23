# bigdata_weblog

原有日志数据是长这个样子:
60.208.6.156 - - [18/Sep/2013:06:49:48 +0000] "GET /wp-content/uploads/2013/07/rcassandra.png HTTP/1.0" 200 185524 "http://cos.name/category/software/packages/" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36"

清洗后得到两类结构化数据:

第一类--原生清洗数据 数据之间以\001分割
false60.208.6.156-2012-12-30 06:49:48/wp-content/uploads/2013/07/rcassandra.png200185524http://cos.name/category/software/packages/"Mozilla/5.0(WindowsNT6.1)AppleWebKit/537.36(KHTML,likeGecko)Chrome/29.0.1547.66Safari/537.36"

第二类--以session进行分组 (同一个IP两次访问间隔小于30分钟为同一个session, 大于30分钟被认为不同的session)
c1da40a1-61c9-4a47-a2b1-944c7889b5ef122.95.27.176-2012-12-30 02:56:02/finance-rhive-repurchase/20011879http://blog.fens.me/series-it-finance/"Mozilla/5.0(WindowsNT6.1)AppleWebKit/537.36(KHTML,likeGecko)Chrome/29.0.1547.66Safari/537.36"

中间又遇到一些问题, 例如在第二类数据在输出到HDFS时, 要输出的是ListBuffer(Array())这样的数据结构, 使用saveAsTextFile的时候发现保存的是地址, 而不是真实数据. 
后经过查找得出解决方案: rdd.map(_.flatten.mkstring) flatten可以将内层的Array也进行mkstring操作. 

还遇到一个问题: 中间用到了对时间的解析使用了SimpleDateFormat, 时不时出现报错NumberFormatException, 经查证原来这个类在多线程情况下是不安全的, 所以才会报错.
原来我把该类定义在main方法外面, 将其移动到main方法内部就不会在报这个错误了.
