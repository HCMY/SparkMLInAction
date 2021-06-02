spark-submit \
--class org.zhouycml.HelloSpark  \
--master local[2]  \
/data_disk/sparktest/jars/SparkMLInAction-1.0-SNAPSHOT.jar file:///data_disk/sparktest/data/UserPurchaseHistory.csv UserPurchaseHistory-o.csv