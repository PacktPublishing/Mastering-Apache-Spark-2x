romeos-mbp:~ romeokienzler$ kubectl create -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/examples/spark/spark-master-controller.yaml
replicationcontroller "spark-master-controller" created

romeos-mbp:~ romeokienzler$  kubectl get pods 
NAME                            READY     STATUS              RESTARTS   AGE
spark-master-controller-ljvq1   0/1       ContainerCreating   0          6s

romeos-mbp:~ romeokienzler$  kubectl get pods 
NAME                            READY     STATUS    RESTARTS   AGE
spark-master-controller-ljvq1   1/1       Running   0          17m

romeos-mbp:~ romeokienzler$ kubectl create -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/examples/spark/spark-master-service.yaml
service "spark-master" created

githubusercontent.com/kubernetes/kubernetes/master/examples/spark/spark-master-service.yaml
service "spark-master" created
romeos-mbp:~ romeokienzler$  kubectl get pods 
NAME                            READY     STATUS    RESTARTS   AGE
spark-master-controller-ljvq1   1/1       Running   0          18m
romeos-mbp:~ romeokienzler$ kubectl logs spark-master-controller-5u0q5
Error from server (NotFound): pods "spark-master-controller-5u0q5" not found
romeos-mbp:~ romeokienzler$ kubectl logs spark-master-controller-ljvq1
17/07/02 05:47:14 INFO Master: Registered signal handlers for [TERM, HUP, INT]
17/07/02 05:47:15 INFO SecurityManager: Changing view acls to: root
17/07/02 05:47:15 INFO SecurityManager: Changing modify acls to: root
17/07/02 05:47:15 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: Set(root); users with modify permissions: Set(root)
17/07/02 05:47:15 INFO Slf4jLogger: Slf4jLogger started
17/07/02 05:47:15 INFO Remoting: Starting remoting
17/07/02 05:47:16 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkMaster@spark-master:7077]
17/07/02 05:47:16 INFO Utils: Successfully started service 'sparkMaster' on port 7077.
17/07/02 05:47:16 INFO Master: Starting Spark master at spark://spark-master:7077
17/07/02 05:47:16 INFO Master: Running Spark version 1.5.2
17/07/02 05:47:26 INFO Utils: Successfully started service 'MasterUI' on port 8080.
17/07/02 05:47:26 INFO MasterWebUI: Started MasterWebUI at http://172.17.0.2:8080
17/07/02 05:47:26 INFO Utils: Successfully started service on port 6066.
17/07/02 05:47:26 INFO StandaloneRestServer: Started REST server for submitting applications on port 6066
17/07/02 05:47:26 INFO Master: I have been elected leader! New state: ALIVE

                                                                            romeos-mbp:~ romeokienzler$ kubectl create -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/examples/spark/spark-ui-proxy-controller.yaml
replicationcontroller "spark-ui-proxy-controller" created
                                                                            romeos-mbp:~ romeokienzler$  kubectl get pods 
NAME                              READY     STATUS              RESTARTS   AGE
spark-master-controller-ljvq1     1/1       Running             0          19m
spark-ui-proxy-controller-k3nqs   0/1       ContainerCreating   0          5s
                                                                            
romeos-mbp:~ romeokienzler$  kubectl get pods 
NAME                              READY     STATUS    RESTARTS   AGE
spark-master-controller-ljvq1     1/1       Running   0          24m
spark-ui-proxy-controller-k3nqs   1/1       Running   1          5m

romeos-mbp:~ romeokienzler$ kubectl create -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/examples/spark/spark-ui-proxy-service.yaml
service "spark-ui-proxy" created
                                                                            
                                                                            romeos-mbp:~ romeokienzler$ kubectl get svc spark-ui-proxy -o wide
NAME             CLUSTER-IP   EXTERNAL-IP   PORT(S)        AGE       SELECTOR
spark-ui-proxy   10.0.0.146   <pending>     80:30621/TCP   16s       component=spark-ui-proxy
                                                                            
                                                                            
romeos-mbp:~ romeokienzler$ minikube service spark-ui-proxy --url
http://192.168.99.100:30621                                                                            
                                                                            
                                                                            romeos-mbp:~ romeokienzler$ kubectl create -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/examples/spark/spark-worker-controller.yaml
replicationcontroller "spark-worker-controller" created
                                                                            
                                                                            romeos-mbp:~ romeokienzler$ kubectl get pod
NAME                              READY     STATUS    RESTARTS   AGE
spark-master-controller-ljvq1     1/1       Running   0          23h
spark-ui-proxy-controller-k3nqs   1/1       Running   23         22h
spark-worker-controller-cz8rx     1/1       Running   0          4s
spark-worker-controller-l121v     1/1       Running   0          4s

                                                                            
                                                                            romeos-mbp:~ romeokienzler$ kubectl logs spark-master-controller-ljvq1
17/07/02 05:47:14 INFO Master: Registered signal handlers for [TERM, HUP, INT]
17/07/02 05:47:15 INFO SecurityManager: Changing view acls to: root
17/07/02 05:47:15 INFO SecurityManager: Changing modify acls to: root
17/07/02 05:47:15 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: Set(root); users with modify permissions: Set(root)
17/07/02 05:47:15 INFO Slf4jLogger: Slf4jLogger started
17/07/02 05:47:15 INFO Remoting: Starting remoting
17/07/02 05:47:16 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkMaster@spark-master:7077]
17/07/02 05:47:16 INFO Utils: Successfully started service 'sparkMaster' on port 7077.
17/07/02 05:47:16 INFO Master: Starting Spark master at spark://spark-master:7077
17/07/02 05:47:16 INFO Master: Running Spark version 1.5.2
17/07/02 05:47:26 INFO Utils: Successfully started service 'MasterUI' on port 8080.
17/07/02 05:47:26 INFO MasterWebUI: Started MasterWebUI at http://172.17.0.2:8080
17/07/02 05:47:26 INFO Utils: Successfully started service on port 6066.
17/07/02 05:47:26 INFO StandaloneRestServer: Started REST server for submitting applications on port 6066
17/07/02 05:47:26 INFO Master: I have been elected leader! New state: ALIVE
17/07/03 04:42:53 INFO Master: Registering worker 172.17.0.6:35693 with 2 cores, 1024.0 MB RAM
17/07/03 04:42:53 INFO Master: Registering worker 172.17.0.7:36563 with 2 cores, 1024.0 MB RAM
                                                                            
                                                                            romeos-mbp:~ romeokienzler$ kubectl create -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/examples/spark/zeppelin-controller.yaml
replicationcontroller "zeppelin-controller" created
                                                                            
                                                                            
romeos-mbp:~ romeokienzler$ kubectl get pod
NAME                              READY     STATUS              RESTARTS   AGE
spark-master-controller-ljvq1     1/1       Running             0          23h
spark-ui-proxy-controller-k3nqs   1/1       Running             23         22h
spark-worker-controller-cz8rx     1/1       Running             0          3m
spark-worker-controller-l121v     1/1       Running             0          3m
zeppelin-controller-csmvr         0/1       ContainerCreating   0          3s

                                                                            
                                                                            romeos-mbp:~ romeokienzler$ kubectl get pod
NAME                              READY     STATUS    RESTARTS   AGE
spark-master-controller-ljvq1     1/1       Running   0          23h
spark-ui-proxy-controller-k3nqs   1/1       Running   23         23h
spark-worker-controller-cz8rx     1/1       Running   0          33m
spark-worker-controller-l121v     1/1       Running   0          33m
zeppelin-controller-csmvr         1/1       Running   0          29m
                                                                            
                                                                            romeos-mbp:~ romeokienzler$ kubectl create -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/examples/spark/zeppelin-service.yaml
service "zeppelin" created
                                                                            
                                                                            romeos-mbp:~ romeokienzler$ minikube service zeppelin --url
http://192.168.99.100:30510

                                                                            
                                                                            
                                                                            
                                                                            

