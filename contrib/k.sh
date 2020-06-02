#!/usr/bin/env bash


# ensure j2 renderer installed
pip freeze | grep j2cli || pip install j2cli[yaml] --user
export PATH=~/.local/bin:$PATH

run_kubectl() {
  local retries=0
  local attempts=10
  while true; do
    if kubectl "$@"; then
      break
    fi

    ((retries += 1))
    if [[ "${retries}" -gt ${attempts} ]]; then
      echo "error: 'kubectl $*' did not succeed, failing"
      exit 1
    fi
    echo "info: waiting for 'kubectl $*' to succeed..."
    sleep 1
  done
}

usage()
{
    echo "usage: kind.sh [[[-cf|--config-file <file>] [-kt|keep-taint] [-ha|--ha-enabled]"
    echo "                 [-ii|--install-ingress] [-n4|--no-ipv4] [-i6|--ipv6]"
    echo "                 [-wk|--num-workers <num>]] | [-h]]"
    echo ""
    echo "-cf | --config-file          Name of the KIND J2 configuration file."
    echo "                             DEFAULT: ./kind.yaml.j2"
    echo "-kt | --keep-taint           Do not remove taint components."
    echo "                             DEFAULT: Remove taint components."
    echo "-ha | --ha-enabled           Enable high availability. DEFAULT: HA Disabled."
    echo "-ii | --install-ingress      Flag to install Ingress Components."
    echo "                             DEFAULT: Don't install ingress components."
    echo "-n4 | --no-ipv4              Disable IPv4. DEFAULT: IPv4 Enabled."
    echo "-i6 | --ipv6                 Enable IPv6. DEFAULT: IPv6 Disabled."
    echo "-wk | --num-workers          Number of worker nodes. DEFAULT: HA - 2 worker"
    echo "                             nodes and no HA - 0 worker nodes."
    echo ""
} 

parse_args()
{   
    while [ "$1" != "" ]; do
        case $1 in
            -cf | --config-file )      shift
                                       if test ! -f "$1"; then
                                          echo "$1 does not  exist"
                                          usage
                                          exit 1
                                       fi
                                       KIND_CONFIG=$1
                                       ;;
            -ii | --install-ingress )  KIND_INSTALL_INGRESS=true
                                       ;;
            -ha | --ha-enabled )       KIND_HA=true
                                       ;;
            -kt | --keep-taint )       KIND_REMOVE_TAINT=false
                                       ;;
            -n4 | --no-ipv4 )          KIND_IPV4_SUPPORT=false
                                       ;;
            -i6 | --ipv6 )             KIND_IPV6_SUPPORT=true
                                       ;;
            -wk | --num-workers )      shift
                                       if ! [[ "$1" =~ ^[0-9]+$ ]]; then
                                          echo "Invalid num-workers: $1"
                                          usage
                                          exit 1
                                       fi
                                       KIND_NUM_WORKER=$1
                                       ;;
            -h | --help )              usage
                                       exit
                                       ;;
            * )                        usage
                                       exit 1
        esac
        shift
    done
}

print_params()
{ 
     echo "Using these parameters to install KIND"
     echo ""
     echo "KIND_INSTALL_INGRESS = $KIND_INSTALL_INGRESS"
     echo "KIND_HA = $KIND_HA"
     echo "KIND_CONFIG_FILE = $KIND_CONFIG "
     echo "KIND_REMOVE_TAINT = $KIND_REMOVE_TAINT"
     echo "KIND_IPV4_SUPPORT = $KIND_IPV4_SUPPORT"
     echo "KIND_IPV6_SUPPORT = $KIND_IPV6_SUPPORT"
     echo "KIND_NUM_WORKER = $KIND_NUM_WORKER"
     echo ""
}

parse_args $*

# Set default values
KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-ovn}
K8S_VERSION=${K8S_VERSION:-v1.18.2}
KIND_INSTALL_INGRESS=${KIND_INSTALL_INGRESS:-false}
KIND_HA=${KIND_HA:-false}
KIND_CONFIG=${KIND_CONFIG:-./kind.yaml.j2}
KIND_REMOVE_TAINT=${KIND_REMOVE_TAINT:-true}
KIND_IPV4_SUPPORT=${KIND_IPV4_SUPPORT:-true}
KIND_IPV6_SUPPORT=${KIND_IPV6_SUPPORT:-false}

# Input not currently validated. Modify outside script at your own risk.
# These are the same values defaulted to in KIND code (kind/default.go).
# NOTE: Upstream KIND IPv6 masks are different (currently rejected by ParseClusterSubnetEntries()):
#  Upstream - NET_CIDR_IPV6=fd00:10:244::/64 SVC_CIDR_IPV6=fd00:10:96::/112
NET_CIDR_IPV4=${NET_CIDR_IPV4:-10.244.0.0/16}
SVC_CIDR_IPV4=${SVC_CIDR_IPV4:-10.96.0.0/12}
NET_CIDR_IPV6=${NET_CIDR_IPV6:-fd00:10:244::/48}
SVC_CIDR_IPV6=${SVC_CIDR_IPV6:-fd00:10:96::/64}

KIND_NUM_MASTER=1
if [ "$KIND_HA" == true ]; then
  KIND_NUM_MASTER=3
  KIND_NUM_WORKER=${KIND_NUM_WORKER:-0}
else
  KIND_NUM_WORKER=${KIND_NUM_WORKER:-2}
fi

print_params

set -euxo pipefail

# Detect IP to use as API server
API_IPV4=""
if [ "$KIND_IPV4_SUPPORT" == true ]; then
  # ip -4 addr -> Run ip command for IPv4
  # grep -oP '(?<=inet\s)\d+(\.\d+){3}' -> Use only the lines with the
  #   IPv4 Addresses and strip off the trailing subnet mask, /xx
  # grep -v "127.0.0.1" -> Remove local host
  # head -n 1 -> Of the remaining, use first entry
  API_IPV4=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | grep -v "127.0.0.1" | head -n 1)
  if [ -z "$API_IPV4" ]; then
    echo "Error detecting machine IPv4 to use as API server"
    exit 1
  fi
fi

API_IPV6=""
if [ "$KIND_IPV6_SUPPORT" == true ]; then
  # ip -6 addr -> Run ip command for IPv6
  # grep "inet6" -> Use only the lines with the IPv6 Address
  # sed 's@/.*@@g' -> Strip off the trailing subnet mask, /xx
  # grep -v "^::1$" -> Remove local host
  # sed '/^fe80:/ d' -> Remove Link-Local Addresses
  # head -n 1 -> Of the remaining, use first entry
  API_IPV6=$(ip -6 addr  | grep "inet6" | awk -F' ' '{print $2}' | \
             sed 's@/.*@@g' | grep -v "^::1$" | sed '/^fe80:/ d' | head -n 1)
  if [ -z "$API_IPV6" ]; then
    echo "Error detecting machine IPv6 to use as API server"
    exit 1
  fi
fi

if [ "$KIND_IPV4_SUPPORT" == true ] && [ "$KIND_IPV6_SUPPORT" == false ]; then
  API_IP=${API_IPV4}
  IP_FAMILY=""
  NET_CIDR=$NET_CIDR_IPV4
  SVC_CIDR=$SVC_CIDR_IPV4
  echo "IPv4 Only Support: API_IP=$API_IP --net-cidr=$NET_CIDR --svc-cidr=$SVC_CIDR"
elif [ "$KIND_IPV4_SUPPORT" == false ] && [ "$KIND_IPV6_SUPPORT" == true ]; then
  API_IP=${API_IPV6}
  IP_FAMILY="ipv6"
  NET_CIDR=$NET_CIDR_IPV6
  SVC_CIDR=$SVC_CIDR_IPV6
  echo "IPv6 Only Support: API_IP=$API_IP --net-cidr=$NET_CIDR --svc-cidr=$SVC_CIDR"
elif [ "$KIND_IPV4_SUPPORT" == true ] && [ "$KIND_IPV6_SUPPORT" == true ]; then
  #TODO DUALSTACK: Multiple IP Addresses for APIServer not currently supported.
  #API_IP=${API_IPV4},${API_IPV6}
  API_IP=${API_IPV4}
  IP_FAMILY="DualStack"
  NET_CIDR=$NET_CIDR_IPV4,$NET_CIDR_IPV6
  SVC_CIDR=$SVC_CIDR_IPV4,$SVC_CIDR_IPV6
  echo "Dual Stack Support: API_IP=$API_IP --net-cidr=$NET_CIDR --svc-cidr=$SVC_CIDR"
else
  echo "Invalid setup. KIND_IPV4_SUPPORT and/or KIND_IPV6_SUPPORT must be true."
  exit 1
fi

# Output of the j2 command
KIND_CONFIG_LCL=./kind.yaml

ovn_apiServerAddress=${API_IP} \
  ovn_ip_family=${IP_FAMILY} \
  ovn_ha=${KIND_HA} \
  ovn_num_master=${KIND_NUM_MASTER} \
  ovn_num_worker=${KIND_NUM_WORKER} \
  j2 ${KIND_CONFIG} -o ${KIND_CONFIG_LCL}

# Create KIND cluster. For additional debug, add '--verbosity <int>': 0 None .. 3 Debug
kind create cluster --name ${KIND_CLUSTER_NAME} --kubeconfig ${HOME}/admin.conf --image kindest/node:${K8S_VERSION} --config=${KIND_CONFIG_LCL}
export KUBECONFIG=${HOME}/admin.conf
run_kubectl -n kube-system delete ds kube-proxy
CONTROL_NODES=$(docker ps -f name=ovn-control | grep -v NAMES | awk '{ print $NF }')
for n in $CONTROL_NODES; do
  run_kubectl label node $n k8s.ovn.org/ovnkube-db=true
  if [ "$KIND_REMOVE_TAINT" == true ]; then
    run_kubectl taint node $n node-role.kubernetes.io/master:NoSchedule-
  fi
done
