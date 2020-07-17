#!/bin/bash
case $1 in
"start"){
for i in hadoop102 hadoop103 hadoop104
 do
   echo "===========启动日志服务器:$i==========="
   ssh $i "source /etc/profile ; nohup java -jar /home/gavin/gmall-realtime/gmall-logger-1.0-SNAPSHOT.jar 1>/dev/null 2>&1 &"
 done
};;
"stop"){
  for i in hadoop102 hadoop103 hadoop104
 do
   echo "===========关闭日志服务器:$i==========="
   ssh $i "source /etc/profile ; jps | grep gmall-logger-1.0-SNAPSHOT.jar | awk '{print \$1}' | xargs kill -9"
 done
};;
*){
  echo "你这个姿势不太对"
  echo "format: gmall_cluster arg"
  echo "args: [start], [stop]"
};;
esac