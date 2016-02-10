FROM centos:centos6

ADD docker_files/cdh_centos_startup_script.sh /usr/bin/cdh_centos_startup_script.sh
ADD docker_files/cdh_centos_installer.sh /tmp/cdh_centos_installer.sh

RUN \
    chmod +x /tmp/cdh_centos_installer.sh && \
    chmod +x /usr/bin/cdh_centos_startup_script.sh && \
    bash /tmp/cdh_centos_installer.sh

# To avoid error: sudo: sorry, you must have a tty to run sudo: https://hub.docker.com/r/liubin/fluentd-agent/~/dockerfile/
RUN sed -i -e "s/Defaults    requiretty.*/ #Defaults    requiretty/g" /etc/sudoers

# MySQL is needed for CGS
RUN yum -y install mysql-server

# CGS stuff
RUN echo 'stuff'
ADD docker_files/cgs-apps.zip /home/
RUN yum -y install unzip
RUN unzip /home/cgs-apps.zip -d /home/
RUN chmod 777 /home/cgs-apps/installCGSInDocker.sh
# We cannot call it right now... It will be in cdh_centos_startup_script.sh RUN cd /home/cgs-apps/ && sudo ./installCGSInDocker.sh

# private and public mapping
EXPOSE 8040:8040
EXPOSE 8088:8088
EXPOSE 8020:8020
EXPOSE 8888:8888
EXPOSE 11000:11000
EXPOSE 11443:11443
EXPOSE 9090:9090


