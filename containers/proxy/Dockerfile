FROM public.ecr.aws/amazonlinux/amazonlinux:latest

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

COPY proxy.py /opt/proxy.py

RUN yum install -y python3
RUN yum install -y wget
RUN /usr/bin/wget https://www.amazontrust.com/repository/AmazonRootCA1.pem -O /opt/AmazonRootCA1.pem
RUN /usr/bin/pip3 install flwr
RUN /usr/bin/pip3 install boto3

ENV AWS_CA_BUNDLE="/opt/AmazonRootCA1.pem"

CMD ["/usr/bin/python3","/opt/proxy.py"]
