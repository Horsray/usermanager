#!/bin/bash
cd /root/home/hueying_proxy
/home/miniconda3/envs/proxyenv/bin/gunicorn -w 2 -b 0.0.0.0:5001 server:app >> /var/log/server_proxy.log 2>> /var/log/server_proxy.err.log
