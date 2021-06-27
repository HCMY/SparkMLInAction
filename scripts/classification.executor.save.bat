spark-submit ^
--class org.zhouycml.classification.Executor  ^
--master local[2]  ^
../target/SparkMLInAction-1.0-SNAPSHOT.jar file:///C:\\Users\\15431\\Documents\\SparkMLInAction\\data\\stumbleupon\\train.tsv ^
rf file:///C:\\Users\\15431\\Documents\\SparkMLInAction\\predictions\\stumbleupon_rf.csv ^
file:///C:\\Users\\15431\\Documents\\SparkMLInAction\\models\\stumbleupon_rf.model