# Federated Learning using AWS IoT 

Federated learning (FL) is a machine learning (ML) scenario with two distinct characteristics.  First, training occurs on multiple machines.  Second, each machine involved in training keeps training data locally; the only information shared between machines is the ML model and its parameters.  FL solves challenges related to data privacy and scalability in scenarios such as mobile devices and IoT.

## Considerations for federated learning

Compared to traditional ML workflows, FL poses two challenges.

* Training cluster unpredictability.  There are mature methods for distributed training using multiple machines.  In the case of FL, however, we may have a very large number of training machines that each has a small segment of the overall data set, and these machines may not be online all the time.  Think of mobile devices or a fleet of industrial monitoring equipment.
* ML framework.  While most popular ML frameworks like TensorFlow and PyTorch support distributed training, FL poses new challenges.  In particular, the training coordinator may not itself see any of the data, and it has to somehow combine the training results from multiple other machines.  

There are a new class of FL frameworks that attempt to handle some of these challenges.  TensorFlow Federated (TFF) handles data sharding between training machines and result aggregation, but it is only intended for simulations.  PyTorch Mobile intends to address FL in the future.  PySyft is another emerging framework for FL.

## Design choices

In order to overcome the two challenges noted above, I decided to use IoT devices and the [Flower](https://flower.dev/) FL framework.  IoT devices are somewhat easier to simulate for demonstration purposes, and the AWS IoT services provide mature capabilities for handling intermittent connectivity and pushing large amounts of data to and from devices.

The Flower framework supports both TensorFlow and PyTorch, and has a well-defined separation of duties between the training coordinator and the training machines.

## Architecture

Conceptually, our architecture might look like this.

![High level pipeline](diagrams/overview.png)

A pipeline or workflow coordinator kicks off an FL job.  We simulate IoT devices using EC2 instances running the GreenGrass Core.  Because Flower uses gRPC for communication between the training coordinator and the training machines, we use a container as a proxy in between the coordinator and the IoT devices.

![Detailed pipeline](diagrams/detailed.png)

At the next level down, we can use a Step Functions workflow to coordinate this process.  It'll launch one container to run the training coordinator, and one more container for each IoT device.  The GreenGrass instances will run a persistent Lambda function that is ready to respond to requests from the proxy.

Looking at the methods that a Flower training machine has to implement, they require passing in a potentially large `parameters` object (the model weights), and returning updated `parameters` as well as other data.  The `parameters` may be too large to pass directly as MQTT payloads.  We will use GreenGrass Streams to let the IoT devices send large objects back to S3, and the deviceds will directly download from S3 when they need to get updated `parameters`.  

![Ack flow](diagrams/ack.png)
Since the Flower FL paradigm is synchronous, we need to emulate that behavior between the proxies and the devices.  The proxies can send commands as MQTT messages.  The devices will respond to those, and write response messages to other topics.  The IoT rules engine will relay those response messages into a DynamoDB table, and the proxies will poll for response updates.

### MQTT topics

Here are the MQTT topics we use currently.

* flower/clients/<client id> for heartbeats
    * Every device sends a message here once a minute
* commands/client/<client id>/update to send commands to the GG core
    * proxy publishes messages here to effectively make a call to the device
    * message will include a `method` parameter and other necessary arguments
    * device subscribes to this topic
* parameters/client/<client id>/sent to send response of `get_parameters`
    * device sends messages here to indicate that it's done with a method
    * message will include return values
    * IoT rule publishes these messages to DynamoDB
    * proxy will poll the table for the message, and then remove it
* set/client/<client id>/sent to send response of `set_parameters`
    * device sends messages here to indicate that it's done with a method
    * message will include return values
    * IoT rule publishes these messages to DynamoDB
    * proxy will poll the table for the message, and then remove it
* fit/client/<client id>/sent to send response of `fit`
    * device sends messages here to indicate that it's done with a method
    * message will include return values
    * IoT rule publishes these messages to DynamoDB
    * proxy will poll the table for the message, and then remove it
* evaluate/client/<client id>/sent to send response of `evaluate`
    * device sends messages here to indicate that it's done with a method
    * message will include return values
    * IoT rule publishes these messages to DynamoDB
    * proxy will poll the table for the message, and then remove it

### DynamoDB

We use one table that has a partition key of `client` and a range key of `type`, which is the type of method the message is for.  The IoT rule inserts the message payload as a JSON document in the `path` column.

### Client discovery

We set up an IoT Analytics flow (channel -> pipeline -> data set) that gives us the ten devices with the most recent heartbeats.

    SELECT DISTINCT client, MAX(time) 
    FROM flower_ds2
    GROUP BY client
    ORDER BY MAX(time), client DESC 
    LIMIT 10

### The ML model

In a real scenario, we'd have to distribute the model artifact to all the devices.  The devices would gather local data for the training process.  For the sake of demonstration, here we're using the canned CIFAR10 model available through PyTorch.

### Security group notes

We create two security groups, one for the GreenGrass cores and another for the ECS tasks, to allow communication inside these groups.  The tasks communicate with each other over port 8080.

In a real deployment, the GreenGrass core has to accept MQTT communication on port 8883 from local devices.  

## Deployment

### Create S3 bucket

In the AWS console, create a new S3 bucket.  Enable versioning and default encryption.
### Build containers

Go into the `containers` directory and build the two images we need.

* Coordinator
    * Go to `containers/coordinator`
    * `../build_and_push.sh flower-coordinator`
* Proxy
    * Go to `containers/proxy`
    * `../build_and_push.sh flower-proxy`

After each build finishes, note the container image URL.  This will look like:

    <account ID>.dkr.ecr.<region>.amazonaws.com/<image name>

### Upload Lambda function

First, create the zip file.

    cd lambda
    zip flower.zip flower.py

Now upload to S3.  You'll need the name of the S3 bucket you created earlier.

    aws s3 cp flower.zip s3://<BUCKET>/lambda/flower.zip

### Deploy starting resources with CloudFormation

* Log in to the AWS console and switch to your desired region
* Go to the CloudFormation console
* Deploy the template `cfn/flower-demo.yaml`.  The default parameters should be fine in most cases.  You will need to provide the name of the S3 bucket you created earlier.

### Set up GreenGrass core device

We'll follow the [quick start](https://docs.aws.amazon.com/greengrass/v1/developerguide/quick-start.html) to turn an EC2 instance into a GreenGrass core device.  

First, we'll use Session Manager to connect to the instance.

* Go to the EC2 console and select the instance whose name starts with `GGCore`.
* Click `Connect`.
* Select `Session Manager` and press `Connect`.

You should now have a connection opened in a browser tab.  Switch to the `ec2-user` account and run the quick start script.

    sudo su - ec2-user
    wget -q -O ./gg-device-setup-latest.sh https://d1onfpft10uf5o.cloudfront.net/greengrass-device-setup/downloads/gg-device-setup-latest.sh && chmod +x ./gg-device-setup-latest.sh && sudo -E ./gg-device-setup-latest.sh bootstrap-greengrass-interactive

The script will prompt for several values.  You must override these:

* Your AWS access key and secret key
* The AWS region you want to work in

You can leave the rest as the default settings.
When the installation finished take note of the GG Core Group Id as it will be needed in the next step.

**Repeat these steps for ALL GG Core instances**

### Finish configuring GreenGrass

Edit the file `scripts/gg_setup.json` and insert the correct values for each item in the dictionary.

* `GG_ROLE_ARN` - Refer to Cloudformation Output with key `GgRoleArn`
* `FN_ARN` - Refer to Cloudformation Output with key `GgFnArn`
* `DEF_UNIQUE_KEY` - A unique identifier for this environment
* `GG_GROUP` - Use the GG Group Id from the step above

Run this python script to finish setting up subscriptions and other resources for GreenGrass.

    cd scripts
    python gg_setup.py

**Repeat these steps for ALL GG groups**

### Deploy IoT rules and analytics

Edit the file `scripts/iot_setup.json` and insert the correct values for each item in the dictionary.

* `TABLE` - Refer to Cloudformation Output with key `TableName`
* `RULE_ROLE_ARN` - Refer to Cloudformation Output with key `IotRoleArn`
* `DATASET_PARAM` - Refer to Cloudformation Output with key `DatasetParam`
* `DEF_UNIQUE_KEY` - Use a unique identifier for this environment
 
Run this python script to finish setting up IoT rules and analytics.

    cd scripts
    python iot_setup.py

### Execute State Machine

Before executing the State Machine:

* The Green Grass Core Lambda functions must complete initialization.  For details, see cloudwatch logs at: `/aws/greengrass/Lambda/${REGION}/${ACCOUNTID}/${PROJECT_TAG}-FlowerFn`
* IOT Analytics DataSet must run at least once

Once the prerequisites above have been completed: 

* Navigate to the StepFunctions console
* Select the FederatedLearning-StateMachine
* Click the `Start execution` button 
* View the logs of the flower-proxy and flower-coordinator ECS Tasks for results

### View Metrics

Once the state machine has run, create the a set of metrics and a CloudWatch dashboard using the CloudFormation template `cfn/flower-metrics.yaml`.  

This stack creates CloudWatch log metric filters.  We pull six pieces of information out of the log files from the ECS tasks:

* Reported accuracy from the clients
* Reported loss from the clients
* Count of clients used by the coordinator
* Count of successful client responses as seen by the coordinator
* Count of failed client responses as seen by the coordinator
* Overall run time as reported by the coordinator

We also prepared a dashboard called `Flower-FL-Metrics` that shows these data points.

![Dashboard](diagrams/dashboard.png)

### Clean up

Follow these steps to clean up the resources used in this workshop:

* Delete the two CloudFormation templates
* Delete the S3 bucket
* Delete the two container images from ECR
* Delete the GreenGrass group
* Delete the IoT rules
* Delete the IoT Analytics resources

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
