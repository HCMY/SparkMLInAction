spark-submit ^
--class org.zhouycml.classification.Executor  ^
--master local[2]  ^
../target/SparkMLInAction-1.0-SNAPSHOT.jar ^
--input file:///C:\\Users\\15431\\Documents\\SparkMLInAction\\data\\stumbleupon\\train.tsv ^
--alg xgb