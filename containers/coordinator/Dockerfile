FROM public.ecr.aws/amazonlinux/amazonlinux:latest

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

COPY server.py /opt/server.py

RUN yum install -y python3
RUN /usr/bin/pip3 install flwr boto3 requests

CMD ["/usr/bin/python3","/opt/server.py"]
