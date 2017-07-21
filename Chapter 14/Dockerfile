#This image is based on the ubuntu root image version 16:04
FROM ubuntu:16.04

#Update and install required packages
RUN apt-get update
RUN apt-get install -y curl wget python openssh-server sudo

#Install the JVM 
RUN mkdir -p /usr/java/default
RUN curl -Ls 'http://download.oracle.com/otn-pub/java/jdk/8u102-b14/jdk-8u102-linux-x64.tar.gz' |tar --strip-components=1 -xz -C /usr/java/default/

#Install and configure ApacheSpark 
RUN wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.7.tgz
RUN tar xvfz spark-2.0.0-bin-hadoop2.7.tgz
RUN chown -R 1000:1000 /spark-2.0.0-bin-hadoop2.7
RUN echo "SPARK_LOCAL_IP=127.0.0.1" > /spark-2.0.0-bin-hadoop2.7/conf/spark-env.sh
RUN groupadd -g 1000 packt
RUN useradd -g 1000 -u 1000 --shell /bin/bash packt
RUN usermod -a -G sudo packt
RUN mkdir /home/packt
RUN chown packt:packt /home/packt
RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config
RUN echo "packt ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
USER packt
RUN ssh-keygen -f /home/packt/.ssh/id_rsa -t rsa -N ''
RUN cp /home/packt/.ssh/id_rsa.pub /home/packt/.ssh/authorized_keys
ENV JAVA_HOME=/usr/java/default/
ENV SPARK_HOME=/spark-2.0.0-bin-hadoop2.7/
RUN echo "export JAVA_HOME=/usr/java/default/" >> /home/packt/.bashrc
RUN echo "export SPARK_HOME=/spark-2.0.0-bin-hadoop2.7/" >> /home/packt/.bashrc
RUN echo ". ~/.bashrc" >> /home/packt/.bash_profile

#Allow external connections to the cluster
EXPOSE 8080
EXPOSE 8081