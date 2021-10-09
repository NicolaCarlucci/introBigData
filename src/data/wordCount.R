timer = function(){
  mapreduce_job1 = from.dfs(
    mapreduce(
      input = hdfs_input,
      map = mapper,
      reduce = reducerr
    ))
}

mapper = function(., v){
    keyval(v, 1)
  }
  
f = read.table("/home/hduser/R/prova/wordcount.txt")
hdfs_input = to.dfs(f)

reducerr = function(k, vv){
  keyval(k, length(vv))
}
  mapreduce_job = from.dfs(
    mapreduce(
      input = hdfs_input,
      map = mapper,
      reduce = reducerr
    ))
  
  system.time(timer())

result = as.data.frame(cbind(mapreduce_job$key, mapreduce_job$val))
result


