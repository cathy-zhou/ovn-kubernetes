FROM centos:8

USER root

ENV PYTHONDONTWRITEBYTECODE yes

# Get openvswitch from Nvidia controlled source
# Based on nv OVS commit: 5f3ff22f
RUN curl -s https://3c720378ea793bfa1cdbc4180c64fca33176674914260f6d:@packages.nvidia.com/install/repositories/sdn/nv-ovs-L1/script.rpm.sh | bash
# Based on nv OVN commit: c740f0c4
RUN curl -s https://4a6c3c2ab9c3468ee4624cf102ba7dc81436598399db8477:@packages.nvidia.com/install/repositories/sdn/nv-ovn-L1/script.rpm.sh | bash 

COPY kubernetes.repo /etc/yum.repos.d/kubernetes.repo
# install epel.repo
RUN yum install -y epel-release
RUN INSTALL_PKGS=" \
	bind-utils procps-ng openssl numactl-libs firewalld-filesystem \
	libpcap kubectl \
	iproute strace socat \
	unbound-libs python2 \
	openvswitch \
	ovn ovn-host ovn-central \
	openvswitch-devel \
        " && \
	yum install -y --setopt=tsflags=nodocs --setopt=skip_missing_names_on_install=False $INSTALL_PKGS

RUN yum -y update && yum clean all && rm -rf /var/cache/dnf/*

RUN mkdir -p /var/run/openvswitch

# Built in ../../go_controller, then the binaries are copied here.
# put things where they are in the rpm
RUN mkdir -p /usr/libexec/cni/
COPY ovnkube ovn-kube-util /usr/bin/
COPY ovn-k8s-cni-overlay /usr/libexec/cni/ovn-k8s-cni-overlay

# ovnkube.sh is the entry point. This script examines environment
# variables to direct operation and configure ovn
COPY ovnkube.sh /root/
COPY ovndb-raft-functions.sh /root/
COPY chassis-del.sh /root/
# override the rpm's ovn_k8s.conf with this local copy
COPY ovn_k8s.conf /etc/openvswitch/ovn_k8s.conf

# copy git commit number into image
COPY git_info /root

# iptables wrappers
COPY ./iptables-scripts/iptables /usr/sbin/
COPY ./iptables-scripts/iptables-save /usr/sbin/
COPY ./iptables-scripts/iptables-restore /usr/sbin/
COPY ./iptables-scripts/ip6tables /usr/sbin/
COPY ./iptables-scripts/ip6tables-save /usr/sbin/
COPY ./iptables-scripts/ip6tables-restore /usr/sbin/

LABEL io.k8s.display-name="ovn kubernetes" \
      io.k8s.description="ovnkube centos image" \
      maintainer="Girish Moodalbail <gmoodalbail@nvidia.com>"

WORKDIR /root
ENTRYPOINT /root/ovnkube.sh
