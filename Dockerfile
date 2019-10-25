FROM registry.opensource.zalan.do/stups/alpine:latest

# add binary
ADD build/linux/kube-metrics-adapter /

ENTRYPOINT ["/kube-metrics-adapter"]
