spark-submit \
--class org.zhouycml.moviline.MovieData  \
--master local[2]  \
/data_disk/sparktest/jars/SparkMLInAction-1.0-SNAPSHOT.jar file:///data_disk/sparktest/data/movie100k/u.item