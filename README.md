# bdt_movies . 
day 1 . 
Spark Streaming: save 'genre', 'company', 'movie' tables . 

day2   
hive, Spark SQL . 

day3  
kafka .  

day4 . 
visulization . 


Analyze:  
Net profit . 
Number of movies released by Distributors .  
Warner Bros budget/revenue . 



Start:  
/usr/local/Cellar/hadoop/3.0.0/sbin/start-all.sh . 
/usr/local/Cellar/zookeeper/3.4.10/bin/zkServer start . 
hive --service metastore . 
brew services start kafka . 


For convenience to see logs:  
vi /usr/local/Cellar/apache-spark/2.3.0/libexec/conf/log4j.properties . 
log4j.rootCategory=WARN, console . 
