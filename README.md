# AWS Glue NOTES
This is a notes on AWS Glue for data engineers.

# Prerequisites
## Dataset used:
https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project/blob/main/data/uber_data.csv

## AWS CLI :
AWS CLI setup documentation : <br>
AWS CLI docs : https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-commandstructure.html

## Topics to cover : 
- AWS S3 bucket
- Managed Tables
- Lamda Triggers
- Glue Catalog
- Glue Crawlers
- Cron Triggers
- IAM 
- Schema Evolution
- Spark Scripts
- CTAS
- Incremental Load
- Glue Data Brew
- External Tables
- Event Based Trigger 
- Custom Functions
- Work flow orchestration
- Custom collection methods in AWS Glue
- AWS IAM and policies
- AWS event bridge 
- AWS cloudwatch
- AWS Step functions 
- AWS Lambda functions
- AWS SQS
- AWS Athena
- AWS DynamoDB

## Underline theory of cloud computing 
### Virtualization and Hypervisor
#### Why a data engineer needs to understand this at all?
When you run a Glue job, a Lambda function, or a Databricks cluster, you are not getting a dedicated physical machine. You are getting a slice of a physical machine that AWS or Databricks manages. Virtualisation is the technology that makes that slicing possible. Understanding it helps you reason about why Glue has a cold start, why Databricks clusters take time to spin up, why two Glue jobs running simultaneously do not interfere with each other, and why serverless feels instant compared to cluster-based compute.

#### What virtualisation is?
A physical server is a box with CPUs, RAM, storage, and network cards. In the early days of computing, one application ran on one physical server. If the application only used 10% of the CPU, the other 90% sat idle. This was enormously wasteful — expensive hardware doing nothing most of the time.

Virtualisation solves this by creating an abstraction layer that makes one physical machine appear to be multiple independent machines. Each virtual machine believes it has its own dedicated CPU, its own RAM, its own storage, and its own operating system. In reality all of them are sharing the same physical hardware underneath. The software layer that creates and manages this illusion is called a hypervisor.

#### What a hypervisor is?
A hypervisor is the software that sits between the physical hardware and the virtual machines running on top of it. It has two core jobs. The first is to divide the physical hardware resources CPU cores, RAM, storage, network bandwidth into isolated allocations and assign them to each virtual machine. The second is to intercept every instruction that a virtual machine tries to execute against hardware and translate it into safe operations on the actual physical hardware, preventing any virtual machine from accidentally or maliciously affecting another.

Think of it like an apartment building manager. The building has one set of physical infrastructure walls, pipes, electricity. The manager divides it into apartments, ensures each tenant only accesses their own space, handles disputes, and makes sure one tenant flooding their bathroom does not destroy everyone else's apartment. The hypervisor is that manager, the building is the physical server, and the apartments are the virtual machines.

#### types of hypervisors
**Type 1 hypervisors** run directly on the physical hardware with no operating system underneath. They are sometimes called bare-metal hypervisors. The hypervisor itself is the first thing that runs when the machine boots, and it then starts the virtual machines on top of itself. AWS uses a Type 1 hypervisor called the Nitro Hypervisor, which is a custom-built system derived from KVM. Because there is no general-purpose operating system between the hardware and the hypervisor, Type 1 hypervisors are extremely fast and efficient. Microsoft Hyper-V, VMware ESXi, and Xen are other examples.

**Type 2 hypervisors** run as an application on top of an existing operating system. The operating system boots first, then the hypervisor runs as a program on that OS, and then virtual machines run on top of the hypervisor. VirtualBox on your laptop is a Type 2 hypervisor. You run Windows or macOS, then you install VirtualBox, then you create Ubuntu virtual machines inside it. Type 2 hypervisors are convenient for development and testing but slower than Type 1 because every hardware instruction passes through two layers the hypervisor and then the host operating system — before reaching actual hardware.

#### How AWS uses virtualisation the Nitro system?
AWS built its own hypervisor called Nitro specifically because the existing open-source hypervisors were not fast enough for what AWS needed. The Nitro system has three parts that work together.

The Nitro Hypervisor is an extremely thin layer it handles only CPU and memory virtualisation. It is so minimal that it adds almost no overhead. AWS measured that Nitro gives EC2 instances access to nearly 100% of the underlying hardware performance, compared to older hypervisors that consumed 30% of hardware resources just running the virtualisation layer itself.

Nitro Cards are dedicated hardware chips that offload the work of virtualising storage and networking away from the CPU entirely. Instead of the hypervisor software handling every network packet and storage operation on the main CPU, dedicated silicon chips handle it. This means your Glue job's 10 workers are not competing with hypervisor overhead for CPU time the Nitro Cards handle all the network and storage virtualisation in hardware, essentially for free.

The Nitro Security Chip handles the security and integrity of the instance. It cryptographically verifies the firmware at boot, provides hardware root of trust, and enables features like Nitro Enclaves which create isolated execution environments even the AWS operators cannot access.

When I start a Glue job and AWS provisions G.1X workers, each worker is an EC2 instance running inside the Nitro hypervisor. My PySpark code runs in a JVM inside that virtual machine, which runs on a physical server in a Mumbai data centre that is also running dozens of other customers' virtual machines at the same time, completely isolated from mine.

#### Containers versus virtual machines what Databricks and Lambda actually use
This is where it gets specifically relevant for your work. Not everything in AWS uses full virtual machines. Some services use containers instead, and understanding the difference explains the startup time differences you experience.

A virtual machine virtualises hardware it pretends to be a complete computer with its own CPU, RAM, and operating system kernel. Creating a new virtual machine means booting an entire operating system, which takes 30 to 90 seconds.

A container virtualises the operating system it shares the host OS kernel but gets its own isolated filesystem, network namespace, and process space. Creating a new container means starting a process and setting up the isolation, which takes milliseconds to a few seconds.

AWS Lambda uses a system called Firecracker, which is a microVM hypervisor also built by AWS. Firecracker creates extremely lightweight virtual machines that boot in under 125 milliseconds. This is why Lambda cold starts feel nearly instant you are still getting a real virtual machine with hardware isolation, but it is so stripped down that it boots faster than most containers. Each Lambda invocation runs in its own Firecracker microVM. When you uploaded a CSV file and Lambda triggered, a Firecracker microVM was created, your Lambda function ran, and the microVM was either recycled for a future warm invocation or destroyed.

Databricks clusters use EC2 instances full virtual machines under the Nitro hypervisor. When you start a Databricks job cluster, AWS spins up new EC2 instances, each booting a full Linux operating system, installing the Databricks runtime, starting Java, starting Spark, and registering with the driver. This takes 3 to 8 minutes. Databricks pools reduce this by keeping terminated instances in a warm state so they can be reused, which is why interactive clusters on Databricks feel much faster than job clusters for iterative notebook work.

Your Glue jobs also use EC2 instances under Nitro. The 2-minute cold start you observed is the time to provision the EC2 instances, boot the OS, start the Glue runtime, initialise the SparkContext, and connect the workers to the driver. There is no way to avoid this cold start with Glue because it is inherent to spinning up a cluster of virtual machines.

### How isolation works? why your Glue job cannot see another customer's data?
This is a question that matters deeply in cloud computing from a security perspective. How can AWS guarantee that your Glue job running on a shared physical server cannot access another company's data running on the same server?

The answer is hardware-enforced memory isolation. Modern CPUs have a feature called virtual memory where each process is given a virtual address space that maps to different physical memory locations through a translation table managed by the OS. The hypervisor extends this each virtual machine gets its own set of translation tables, managed by the hypervisor. A virtual machine can only access memory addresses in its own translation table. Even if malicious code inside a virtual machine tried to read memory belonging to another virtual machine, the CPU hardware would reject the access and raise a fault before any data was exposed.

The Nitro hypervisor further strengthens this by running in a separate hardware domain entirely. The hypervisor code itself runs in a privileged CPU ring that even the virtual machines' operating systems cannot access. AWS engineers operating the infrastructure also cannot access your virtual machine's memory at runtime this is enforced at the hardware level by the Nitro Security Chip.

This is why you can trust that your sales CSV data inside a Glue job is not visible to another company's Glue job running on the same physical server at the same time. The isolation is not just a software policy it is enforced by the CPU hardware itself.

#### What this means practically for your data engineering work?
**Glue cold starts** exist because spinning up EC2 virtual machines takes time. The hypervisor needs to allocate memory pages, set up translation tables, boot the OS, and start the runtime. You cannot eliminate this. You can mitigate it by batching small files together into fewer larger Glue runs rather than 1000 individual runs, but the fundamental cause is VM startup time.

**Lambda's near-instant response** is because Firecracker microVMs are designed for this use case minimal boot time, maximum isolation. Your Lambda idempotency function runs in a microVM that boots in under 125ms. The warm invocation path reuses an existing microVM, making it even faster.

**Databricks cluster startup** is the same EC2 VM startup problem as Glue, plus the additional time to install the Databricks runtime and start Spark. Databricks pools work by keeping VMs running but in a paused state, so reuse is fast. This is why in production Databricks deployments, operations teams keep a pool of warm instances pre-provisioned rather than starting fresh clusters for every job.

**Why serverless feels different from cluster-based** Lambda and Glue Serverless use microVMs that start fast and die after each invocation. Databricks and traditional Glue jobs use full EC2 VMs that take minutes to start but then stay warm for the duration of your work session. For a data engineer, this means serverless is better for event-driven, short, frequent tasks like your Lambda idempotency check while cluster-based is better for long-running, compute-heavy transformations where the startup cost amortises over a long execution time.

## AWS Region and availibility zone 
### AWS Regions
An AWS Region is a physical geographic location in the world where Amazon has built a cluster of data centres. Each Region is completely independent of every other Region — it has its own power supply, networking, and physical infrastructure.

AWS built this model for three reasons. First, latency if your users are in India, running your application in ap-south-1 (Mumbai) is faster than running it in us-east-1 (Virginia) because the data travels a shorter physical distance. Second, data residency — many countries and industries have laws requiring that data stays within specific geographic boundaries. 

### AWS Zones
Availability Zones are the individual data centre clusters that exist inside a Region. Every AWS Region contains multiple Availability Zones typically three to six and each one is physically separate from the others.

A single Availability Zone is one or more physical data centre buildings located in the same metropolitan area as the other zones in that Region. Each zone has its own independent power supply from a different grid, its own cooling systems, its own physical security, and its own network connections to the internet. The independence is the entire point if one zone loses power or catches fire, the others keep running.

Within a Region, all the Availability Zones are connected to each other through dedicated private fibre cables that AWS owns and operates. The latency between zones is under 1 millisecond, which is fast enough that your application can treat them as if they are in the same location while still getting the fault isolation benefit.

## AWS IAM and policies 
### AWS IAM 
IAM stands for Identity and Access Management. It is the security system that controls who can do what inside your AWS account. Nothing in AWS can talk to anything else without IAM being involved every API call, every service interaction, every CLI command goes through IAM before it is allowed to proceed.

#### The core problem IAM solves
When you have multiple services, users, and applications inside one AWS account, you need a way to say precisely which entity is allowed to perform which actions on which resources. Without IAM, either everything would have access to everything which is a massive security risk or you would have no way to automate anything because services could not communicate with each other.

#### The four main IAM concepts
**Users** are human identities. An IAM user represents a person a developer, a data engineer, an administrator. Users have permanent credentials in the form of a password for the AWS console and access keys for the CLI or SDK. In modern AWS practice, creating individual IAM users for humans is being replaced by identity federation where your company's existing login system handles authentication, but the concept still exists.

**Groups** are collections of users. Instead of attaching permissions directly to each individual user, you create a group called data-engineers, attach the relevant permissions to that group, and then add users to it. Any user in the group inherits the group's permissions. This makes managing permissions for teams much simpler.

**Roles** are the most important IAM concept for building AWS systems. A role is a set of permissions that is assumed temporarily by a service, application, or user. Unlike users, roles do not have permanent credentials they issue temporary credentials that expire after a short period, typically one hour. When an AWS service like Lambda needs to access DynamoDB, it assumes an IAM role and gets temporary credentials valid for that session only.

**Policies** are the actual permission definitions. A policy is a JSON document that says what actions are allowed or denied on what resources. Policies are attached to users, groups, or roles to grant or restrict permissions.

#### How policies work?
A policy is made of statements. Each statement has four key parts. The Effect is either Allow or Deny. The Action is what operation is being controlled, written as ```service:operation``` for example ```dynamodb:GetItem```, ```s3:PutObject```, or ```glue:StartJobRun```. The Resource is which specific AWS resource the action applies to, written as an ARN. The Condition is an optional filter that makes the permission apply only under certain circumstances.
Full Example : 
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:GetItem",
      "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
    }
  ]
}
```

## AWS Elastic Block Storage (EBS) : 
### What is BLOCK storage ?
There are three main types of storage block storage, file storage, and object storage. Understanding the difference explains why block storage exists.

**Block storage** exposes raw blocks with addresses. The consumer typically an operating system or database directly controls how data is organised on those blocks. It is the lowest level, closest to bare hardware. It is fast because there is minimal overhead between your application and the actual data.

**File storage** exposes a filesystem with files and folders over a network. NFS and SMB are protocols for this. Multiple machines can mount the same filesystem simultaneously and see the same files. The storage system manages the filesystem structure for you. It is convenient but slower than block storage because of the filesystem overhead and network protocol.

**Object storage** which is what S3 is exposes a flat namespace of objects identified by keys. There are no folders, no blocks, no filesystem. You PUT an object, you GET an object by its key. Object storage is designed for massive scale and high durability, not for low-latency random access. You cannot mount S3 as a drive and run a database on it the way you can with block storage.

### What AWS EBS is?
Amazon Elastic Block Store is AWS's block storage service for EC2 instances. When you launch an EC2 instance which as we discussed is a virtual machine running under the Nitro hypervisor that instance needs a disk. EBS provides that disk as a network-attached block device.

The word elastic means the volume can be resized, the performance can be adjusted, and it exists independently of the instance. The word block refers to the fact that it presents raw block storage to the operating system running inside the EC2 instance, not a filesystem or an object store.

When your EC2 instance boots, it sees an EBS volume as if it were a physical disk directly attached to the machine. The Linux kernel inside your instance talks to it using standard block device protocols. The instance formats it with a filesystem like ext4, mounts it, and uses it exactly like a local hard drive. The fact that the actual storage hardware is somewhere else in the AWS data centre connected over a high-speed network is completely transparent to the operating system.

### How EBS works under the hood?
EBS volumes are not stored on the same physical server as your EC2 instance. They live on separate, dedicated storage servers connected to the compute servers through AWS's internal network fabric a high-bandwidth, low-latency network that AWS built specifically for this purpose using the Nitro Cards discussed earlier.

When your EC2 instance writes data to its EBS volume, the Nitro Card on the compute server intercepts the block write operation and sends it over the internal network to the storage server where the actual EBS volume lives. This happens so fast sub-millisecond latency on io2 volumes that the operating system inside the EC2 instance cannot distinguish it from a local disk.

This separation of compute and storage is architecturally important. It means your EC2 instance can be terminated and a new one started, and the EBS volume with all your data is completely unaffected. The data persists independently of the compute instance lifecycle. You can detach an EBS volume from one instance and attach it to a different instance, and the new instance sees all the data exactly as the previous one left it.

### EBS volume types
AWS offers several EBS volume types optimised for different use cases, and the differences matter for data engineering workloads.

**gp3** is the general purpose SSD and the default for most workloads. It provides a baseline of 3000 IOPS and 125 MB/s throughput regardless of volume size. You can independently provision additional IOPS and throughput up to 16,000 IOPS and 1000 MB/s without increasing storage size. IOPS means input/output operations per second — the number of individual read or write operations the volume can handle per second. For most Glue jobs and general EC2 workloads this is the right choice.

**io2 Block Express** is the high-performance SSD for latency-sensitive workloads. It provides up to 256,000 IOPS and sub-millisecond latency. This is what you would use for a high-throughput database like a production PostgreSQL or Oracle instance where every millisecond of storage latency matters. It is significantly more expensive than gp3.

**st1** is a throughput-optimised hard drive designed for large sequential reads and writes rather than random access. It has low IOPS but high throughput — up to 500 MB/s. This is appropriate for workloads that read large files sequentially, like a Hadoop data node or a log processing system. It is much cheaper than SSD volumes.

**sc1** is a cold hard drive for infrequently accessed data. The cheapest EBS option. Used for archival data that is rarely read.

### EBS versus S3 a comparison that matters directly for your pipeline
This distinction is critical for a data engineer and it directly explains architectural decisions in your pipeline.

**EBS** is a disk attached to one EC2 instance. Only that instance can read and write to it. It is fast sub-millisecond latency for random reads and writes. It is limited in size by what you provision. It exists in one Availability Zone. If the AZ goes down, the volume is inaccessible until AWS restores the AZ. It is designed for structured access patterns a database writing rows, an OS reading executables, a virtual machine swapping memory pages.

**S3** is an object store accessible by any AWS service from anywhere. It is slower than EBS for small random reads — typically 10 to 100 milliseconds per request. But it is infinitely scalable, replicated across multiple Availability Zones automatically, and accessible by Glue, Lambda, Athena, Step Functions, and any other AWS service simultaneously. It costs roughly 10 times less per GB than EBS.

This is precisely why your data engineering pipeline uses S3 for data storage and not EBS. Your CSV files land in S3. Your Silver parquet files are written to S3. Your Glue job reads from S3 and writes to S3. This works because Glue workers which are EC2 instances with EBS root volumes read from S3 over the network, transform the data in memory, and write back to S3. The EBS volume on each Glue worker is only used for the operating system, the Glue runtime, and temporary Spark shuffle data during the job. It is not used for your actual pipeline data.

**Real cost impact :** <br>
If your pipeline stored data on EBS instead of S3, only one EC2 instance could access it at a time, it would cost 10 times more, it would not survive instance termination, and Athena could not query it. S3 is the correct storage layer for a data lake specifically because it is not block storage.

### What EBS means in your specific data engineering context
Your Glue workers are EC2 instances. Each one has an EBS root volume this is where the Linux OS boots from, where the Glue runtime is installed, and where Spark writes shuffle files when data overflows memory during operations like ```groupBy```, ```join```, and ```dropDuplicates```. When your Glue job runs ```dropDuplicates(["review_id"])``` on a large dataset, Spark may need to spill intermediate data to disk. That disk is the EBS volume on the worker.

If your Glue job fails with a disk space error or shuffle spill errors on very large datasets, the fix is to increase the EBS volume size on your Glue workers or increase the number of workers so each one handles less data. This is a real operational concern at scale even though at your current dataset size of 489 rows per file it will never happen.

Databricks clusters have the same structure each EC2 node has an EBS volume for the OS and Spark shuffle, and your actual data lives in S3. The EBS volume is infrastructure plumbing that you rarely think about directly, but it is always there underneath every compute workload you run on AWS.

### Why shuffle spill happens first the root cause?
When Spark executes an operation that requires comparing or grouping rows across the entire dataset like ```dropDuplicates(["review_id"])```, ```groupBy```, or ```join``` it cannot do this locally on each worker in isolation. It needs all rows with the same key to land on the same worker so they can be compared. This process of redistributing data across workers based on key is called a shuffle.

During a shuffle every worker sends some of its rows to other workers and receives rows from other workers. All of this intermediate data — rows in transit, rows waiting to be processed needs to live somewhere. Spark first tries to hold it in RAM. If the data is larger than the available RAM on that worker, Spark writes the overflow to local disk. This is called spill data spilling from memory onto disk.

The disk it spills to is the EBS volume attached to that Glue worker EC2 instance. If the spill data is larger than the available EBS space, the job fails with a disk space error.

Your current dataset of 489 rows per file is a few hundred kilobytes. This fits in RAM thousands of times over. But if the same pipeline processed a 10GB CSV file with 50 million rows, the shuffle for dropDuplicates would generate gigabytes of spill data and could exhaust disk space on the workers.

### How to increase EBS volume size on Glue workers?
Glue does not expose EBS volume configuration directly in the console the way EC2 does. Instead you control it through a job parameter called --job-disk-size. This parameter sets the EBS volume size in GB for each worker.

**In the AWS Glue console:**

Go to AWS Glue → Jobs → click your ingest_sales_data job → click Edit → scroll down to Advanced properties → find the Job parameters section → add a new parameter:
```bash
Key:   --job-disk-size
Value: 96
```
The default disk size per Glue worker depends on the worker type. For G.1X the default is 64GB. For G.2X it is 128GB. Setting ```--job-disk-size``` to 96 would give each worker 96GB of EBS storage for shuffle spill and temporary data.

In your Glue job script you can also set it programmatically when starting the job via boto3, which is relevant because your Step Function starts the Glue job with glue:startJobRun. You would add it to the Arguments:
```json
"Arguments": {
    "--source_bucket.$": "$.bucket",
    "--source_key.$": "$.key",
    "--job-disk-size": "96"
}
```
Increasing from 10 to 20 doubles the parallelism and halves the data each worker handles. It also doubles the cost : 20 workers × $0.44/DPU-hour instead of 10.

Via boto3 in your Step Function your orchestrate_data_ingestion Step Function starts the Glue job like this:
```json
"Parameters": {
    "JobName": "ingest_sales_data",
    "Arguments": {
        "--source_bucket.$": "$.bucket",
        "--source_key.$": "$.key"
    }
}
```
You can override the worker count per execution by adding NumberOfWorkers and WorkerType to the Parameters:
```json
"Parameters": {
    "JobName": "ingest_sales_data",
    "NumberOfWorkers": 20,
    "WorkerType": "G.1X",
    "Arguments": {
        "--source_bucket.$": "$.bucket",
        "--source_key.$": "$.key"
    }
}
```
This lets you dynamically control workers per execution, which is useful if some files are much larger than others.

### Worker types and what they mean?

Glue offers several worker types and the choice affects both memory and EBS disk size:

| Worker type | vCPUs | RAM | Default EBS | DPU count | Price/hour |
|---|---|---|---|---|---|
| G.1X | 4 | 16GB | 64GB | 1 | $0.44 |
| G.2X | 8 | 32GB | 128GB | 2 | $0.88 |
| G.4X | 16 | 64GB | 256GB | 4 | $1.76 |
| G.8X | 32 | 128GB | 512GB | 8 | $3.52 |

Switching from G.1X to G.2X doubles the RAM per worker, which directly reduces spill because more data fits in memory before Spark needs to write to disk. If your job is failing due to memory pressure rather than disk pressure, upgrading the worker type is often more effective than adding more workers.

The trade-off between adding workers and upgrading worker type:

Adding more workers is better when your data can be parallelised effectively — more partitions, more workers processing them simultaneously. The job runs faster and each worker handles less data.

Upgrading worker type is better when the bottleneck is within a single partition — a single worker receiving a large amount of data for one key during a shuffle. No amount of additional workers helps if one key has 50% of all the rows, because all those rows must land on one worker during a `groupBy` or `dropDuplicates`. Giving that worker more RAM via G.2X or G.4X is the only solution.

### How to diagnose which problem you actually have

When your Glue job fails at scale you need to know whether the issue is disk space, memory, or something else. Glue writes its logs to CloudWatch under `/aws-glue/jobs/output` and `/aws-glue/jobs/error`.

A disk space error looks like this in the logs:
```
java.io.IOException: No space left on device
```

A memory error looks like this:
```
java.lang.OutOfMemoryError: Java heap space
Container killed by YARN for exceeding memory limits
```

A shuffle spill warning — not a failure but a performance concern — looks like this:

ExternalSorter: Spilling data to disk (25 times so far)

If you see disk space errors, increase --job-disk-size. If you see memory errors or excessive spill warnings, upgrade the worker type from G.1X to G.2X. If the job is slow but not failing, add more workers to increase parallelism.

For your current pipeline these errors will not appear. But when you move to larger datasets in future projects, reading these CloudWatch logs and knowing which parameter to adjust is a core data engineering operational skill.

## AWS S3 Bucket
S3 is not a filesystem. It is not a database. It is not block storage like EBS. It is a flat key-value store where the key is a string — the object key — and the value is the bytes of your file plus some metadata. That is the entire model. Every object in S3 is identified by three things: the bucket name, the object key, and optionally a version ID.

When you upload sales_data_1.csv to the path ```raw_data/sales_data/sales_data_1.csv``` in your bucket ```aws-glue-s3-bucket-one```, S3 stores it as a single object. The key is literally the string ```raw_data/sales_data/sales_data_1.csv```. The forward slashes are not real folder separators they are just characters in the key string. S3 has no concept of folders or directories. The AWS console shows you a folder-like interface because it groups objects that share a common prefix, but underneath there are no folders. There is just a flat collection of objects identified by their key strings.

### How S3 works underneath?
S3 is one of the most complex distributed systems ever built. Understanding its internals helps you understand its behaviour why large uploads need multipart, why listing objects is slower than getting them, why eventual consistency existed historically.

**Physical storage** S3 stores every object across multiple physical devices in multiple Availability Zones within a region. When you upload a file to ```ap-south-1```, S3 automatically replicates it across at least three separate physical storage locations in Mumbai. You never configure this it happens automatically. This is why S3 achieves 99.999999999% durability eleven nines. Losing your data in S3 would require simultaneous physical failures across multiple independent data centres, which has essentially never happened.

**The index** S3 maintains a distributed index that maps object keys to their physical locations. When you do a GET request for an object, S3 looks up the key in this index, finds the physical location, and retrieves the bytes. This lookup adds latency typically 5 to 50 milliseconds for the first byte which is why S3 is slower than EBS for random access. EBS is directly attached block storage with sub-millisecond latency. S3 goes through a network lookup before any data is returned.

**Partitioning** S3 automatically partitions your objects across its internal infrastructure based on the key prefix. This is why the old advice was to add random prefixes to your keys if all your keys started with a date like ```2024-01-01/file1.csv```, all those keys would land in the same internal partition and you would hit throughput limits. Modern S3 automatically detects hot partitions and redistributes them, so this is less of a concern today. But your pipeline already uses good key structure ```raw_data/sales_data/sales_data_1.csv``` that distributes naturally.

### S3 storage classes
S3 has multiple storage classes designed for different access patterns and cost profiles. This matters for your pipeline's long-term cost management.

**S3 Standard** is what your pipeline uses by default. It is designed for frequently accessed data low latency, high throughput, replicated across three AZs, no minimum storage duration. Your Bronze CSV files and Silver parquet files should live here while they are actively being processed and queried.

**S3 Intelligent-Tiering** automatically moves objects between access tiers based on how often they are accessed. If an object has not been accessed for 30 days it moves to an infrequent access tier at lower cost. If it is accessed again it moves back to standard. It charges a small monitoring fee per object. For data lake files that are initially hot during ingestion and analysis but then rarely touched, this is cost-effective.

**S3 Standard-IA** Infrequent Access is cheaper per GB stored but charges a retrieval fee per GB read. Designed for data you access less than once a month. Appropriate for older partitions of your Silver layer that are rarely queried.

**S3 Glacier** Instant Retrieval is for archival data that needs to be retrieved occasionally with millisecond access. Significantly cheaper storage than Standard-IA but retrieval costs more.

**S3 Glacier Flexible Retrieval** is for true cold archival where you can wait minutes to hours for retrieval. Very cheap storage, used for compliance archives and long-term backups.

**S3 Glacier Deep Archive** is the cheapest storage in all of AWS approximately $0.00099 per GB-month in us-east-1. Retrieval takes 12 hours. Used for data that must be retained for regulatory reasons but will almost never be accessed.

**Suggestion for what to use for data pipelines:** <br>
For your pipeline a practical approach is to keep your Silver parquet files in S3 Standard for the current month, use a lifecycle rule to transition them to Standard-IA after 30 days, and to Glacier after 1 year if regulatory retention requires it.

### S3 lifecycle rules
```bash
Prefix: silver_layer/sales_data/
Rule 1: Transition to Standard-IA after 30 days
Rule 2: Transition to Glacier Instant Retrieval after 90 days
Rule 3: Delete after 365 days (if no regulatory requirement to keep longer)
```

### S3 versioning
Versioning is a bucket-level setting that keeps every version of every object instead of overwriting on upload. Your README actually mentions enabling versioning as a best practice.

When versioning is enabled and you upload ```sales_data_1.csv``` twice, S3 keeps both versions. Each version gets a unique version ID a string like ```3HL4kqtJlcpXrof5_Y5KJI1bkJP4gMkR```. The latest version is returned on a normal GET request. To retrieve an older version you specify the version ID explicitly.

For your pipeline versioning provides two benefits. First, if a corrupted file is uploaded with the same name as a previously good file, you can retrieve the previous good version rather than losing it. Second, it provides an audit trail — you can see every version of every file that was ever uploaded, when it was uploaded, and retrieve any historical version.

The cost of versioning is that you pay for storage of every version. A file uploaded 10 times with versioning enabled costs 10 times the storage of a single version. Lifecycle rules can be configured to expire old versions automatically for example keeping the last 3 versions and deleting older ones.

### S3 security model
S3 has three layers of access control that work together.

**Bucket policies** are JSON documents attached to a bucket that define who can do what to objects in that bucket. Your pipeline uses bucket policies to allow EventBridge to send events and to allow your Glue role to read and write. A bucket policy applies to the entire bucket or specific prefixes within it.

**IAM policies** are attached to users, roles, or groups and define what S3 actions those identities can perform. Your ```AWSGlueRole``` has the ```LimitedS3PermissionPolicy``` which grants ```s3:GetObject```, ```s3:PutObject```, and ```s3:ListBucket``` on specific bucket ARNs. An S3 request is allowed only if both the IAM policy on the requester and the bucket policy on the bucket permit it.

**Block Public Access** is a safety setting at the account and bucket level that prevents any configuration from accidentally making objects publicly accessible. You should always have Block Public Access enabled on all your data buckets. S3 public access has been the cause of some of the most significant data breaches in cloud computing history companies accidentally left sensitive data publicly readable. Block Public Access prevents this even if a misconfigured bucket policy would otherwise allow it.

**Server-side encryption** S3 encrypts all objects at rest by default using SSE-S3, which is AWS-managed encryption. Every object stored in S3 is encrypted on the physical storage device. You can also use SSE-KMS which uses AWS Key Management Service and gives you control over the encryption keys, enabling audit logs of every encryption and decryption operation.

### S3 performance characteristics that matter for Glue
Understanding S3 performance helps you understand why your Glue jobs behave the way they do.

**Request rate** S3 supports at least 3,500 PUT/COPY/POST/DELETE and 5,500 GET/HEAD requests per second per prefix. For your pipeline at 1000 files per day this is not even close to a concern. At massive scale — thousands of concurrent Glue workers all reading from the same prefix simultaneously — you would need to distribute reads across multiple prefixes to avoid throttling.

**First byte latency** Getting the first byte of an object from S3 takes 5 to 50 milliseconds. This is the network round trip plus the index lookup. For your Glue job reading a 1MB CSV file this adds a negligible 50ms to a job that takes 3 minutes. For a Lambda function doing many small S3 reads in a tight loop, this latency compounds and can become significant.

**Throughput** Once S3 starts streaming an object, throughput is very high — easily hundreds of MB/s per request, and multiple parallel requests can be made simultaneously. Glue reads your parquet files using parallel requests across all workers, each worker reading a different set of files concurrently. This is why columnar formats like parquet work so well with S3 — Glue can read only the columns it needs using byte-range requests rather than downloading the entire file.

**Multipart upload** For files larger than 100MB, S3 recommends using multipart upload. Instead of uploading one large file as a single request — which would fail and require restarting if the connection drops — multipart upload splits the file into parts, uploads each part independently, and assembles them at the end. Glue handles this automatically when writing large parquet files. You do not need to implement it yourself.

**S3 in the context of your pipeline** 

Every design decision in your pipeline connects back to S3's characteristics.

Your EventBridge rule fires on Object Created events from S3. This works because S3 publishes events to EventBridge automatically when versioning and EventBridge integration are enabled on the bucket. The event contains the bucket name and object key ```raw_data/sales_data/sales_data_1.csv ```which your Lambda extracts and passes to the Step Function, which passes it to Glue.

Your Glue job reads exactly one file per execution using ```connection_options={"paths": [f"s3://{source_bucket}/{source_key}"], "recurse": False}```. This is a single S3 GET request for one specific object key. The recurse: False tells Glue not to list and read all objects under that prefix just the one file. This is the file-level ingestion pattern that makes your pipeline efficient and isolates failures to individual files.

Your Silver layer is written to ```s3://data-sink-one/silver_layer/sales_data/``` partitioned by category. Partitioning in the context of S3 means Glue creates subfolders actually key prefixes like ```silver_layer/sales_data/category=Electronics/``` and writes parquet files into them. When Athena queries this table and filters by category, it reads only the objects under the matching prefix and skips all others. This is partition pruning instead of scanning all 51GB of Silver data, a query filtered to one category only reads that category's parquet files. This directly reduces your Athena cost because Athena charges per GB scanned.

## AWS Glue
### **What is ETL/ELT?** <br>
We want to **E**xtract the data and **L**oad that data somewhere and that loaded data should be **T**ransformed as per requirments.
- Data engineer will pull data from the sources 
- Data engineer make sure that the data is in the form which can be used by the downstream services or application
- This is the part where ETL/ELT comes in and its designed and maintained by data engineer.


| Aspect                       | ETL                                         | ELT                            |
| ---------------------------- | ------------------------------------------- | ------------------------------ |
| Order                        | **Extract → Transform → Load**              | **Extract → Load → Transform** |
| Where transformation happens | Before loading (external processing engine) | Inside the data warehouse      |
| Best for                     | Traditional data warehouses                 | Modern cloud data platforms    |
| Scalability                  | Limited by ETL server                       | Scales with warehouse compute  |

**ETL :** <br>
**Pros :** <br>
- Clean data before storage
- Strong governance
- Lower storage cost (no raw dump)

**cons :**<br>
- Less flexible
- Slower to adapt to new schema
- Doesn't scale well for massive data

**ELT :** <br>
**Pros :**
- Highly scalable
- Store raw data (future proof)
- Faster iteration
- Works well with analytics + ML

**Cons :**<br>
- Raw data storage costs
- Requires powerful warehouse

#### Points to remember : 
- ETL scaling is:
    - Vertical (bigger machine)
    - Operationally heavy
- ELT scaling is:
    - Horizontal (distributed warehouse engine)
    - Cloud-managed

#### Deep architectural comparison

| Property             | ETL                  | ELT                              |
| -------------------- | -------------------- | -------------------------------- |
| Raw data retained    | Usually no           | Yes                              |
| Schema flexibility   | Low                  | High                             |
| Reprocessing ability | Hard                 | Easy                             |
| Scaling compute      | Hard                 | Elastic                          |
| Best for             | Structured reporting | Analytics + AI + experimentation |
| Change tolerance     | Low                  | High                             |

### **Why AWS Glue?** <br>
AWS glue is a combination of many components that are stiched together. It is a serverless data integration service.

It helps you:
- Discover data
- Catalog metadata
- Transform data
- Prepare data for analytics

AWS glue docs : 
https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html

AWS glue pricing related details are here : 
https://aws.amazon.com/glue/pricing/

### **What is Serverless?**
Serverless means that you don't need to manage the underlying infrastructure, scaling etc. you just have to run the service.

## AWS Glue components 
- Direct tools: 
    - Data Catalog
    - Crawler
    - Glue Data Brew
- Indirect tools:
    - S3
    - Athena
    - IAM
    - Lambda

**NOTE :** IAM is the most important component in AWS you cannot build anything without IAM in AWS because security is the most important thing. <br>
IAM is a security through which multiple components talk to each other.

**What is IAM ?** <br>
AWS Identity and Access Management (IAM) is a web service for securely controlling access to AWS services. With IAM, you can centrally manage users, security credentials such as access keys, and permissions that control which AWS resources users and applications can access.
Docs link related to IAM in AWS docs : <br>
https://docs.aws.amazon.com/iam/

**What is AWS S3?** <br>
Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. Customers of all sizes and industries can use Amazon S3 to store and protect any amount of data for a range of use cases, such as data lakes, websites, mobile applications, backup and restore, archive, enterprise applications, IoT devices, and big data analytics <br>
- AWS S3 bucket means our data lake
- Bucket means object storage i.e I can upload anything
https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html

**Points to remember :**
- When creating AWS S3 bucket make sure that you enable bucket versioning because when this is enabled then if you are getting a file with the same name and type then the s3 will not replace the file it will simply give the file a new name which is a good thing in this case.
- Always create folders when working with S3
- We should not work on file level in data egineering we should always work on folder level.

## AWS Glue Catalog
Suppose you a data stored in csv files and that csv file is stored in the S3 bucket and you want to query this data using SQL. <br>
Normally this is not possible because csv is just a text file how can someone query data using SQL stored in csv files. Csv files are not a structured data <br>
Whenever you use SQL to query data that data needs to be a structured data i.e rows and columns <br>
You may say that if you open a csv file in excel you can view it in rows and columns, but excel is a software that is built to render the data inside the csv file in rows and columns but that doesn't mean the data iself is stored in rows and columns to be able to call it a structured data. <br>
So here we will apply an abstraction layer and this abstraction layer will be applied by **Data Catalog**. <br>
The moment you apply the abstraction applied by the **Data Catalog** the csv files stored in S3 starts behaving like objects of the database. FYI catalogs are equivalent to database. **So whenever you go inside the AWS Glue service and under the section Data Catalog you create a new database that means you are creating a new catalog.**<br>
This catalog will register the metadata and schema of the files and will create an object in the catalog. <br>
**ADVANTAGE :**
- This AWS Glue catalog gives the devs the power to use their SQL skills to query the data stored in csv, parquete etc. files (a non structured data) in the same way they would have queried the objects of databases.
- Catalog is the backbone of all the queries that you do in Athena. 
- If catalog is not there you will not be able to query the data.

There are three things in Data Catalog: 
- External table
- CTAS managed
- CTAS external

Here are some references that you can use from the official docs <br> 
https://docs.aws.amazon.com/glue/latest/dg/components-overview.html#data-catalog-intro
Getting started with the AWS Glue Data Catalog <br>
https://docs.aws.amazon.com/glue/latest/dg/start-data-catalog.html

### How to create databases in AWS Glue?
You can create a database in AWS Glue by going to the menue option named database under the Data Catalog section on the side panel provided by AWS. <br>
On top right corner you will find the Add database button press it and create your database <br>

### How to create tables in AWS Glue catalog?
There are two ways to create a table in catalog:
- Manual
- Through crawlers

#### External table
When you have the data in the datalake (S3 bucket) and you want to register the data in the data catalog. <br>
Table metadata lives in the catalog but the real data is stored in the S3 bucket which is managed by you. <br>
amazon athena docs link : <br>
https://docs.aws.amazon.com/athena/

## AWS Athena
#### How to create External table using Athena.
External table is the same as externally managed tables that I read in databricks.

**STEP 1:** <br>
Go to athena query console > settings > set query result path <br>
NOTE : The source and destination should not be the same i.e you cannot give the same s3 bucket url where you have the csv files to save your athena query results, it must be different or else you will keep getting errors saying destination to store query results is not provided when you try to create the external table.

**STEP 2:** <br>
From the left side panel > select create > and from the create table from data source section > select S3 bucket data

**STEP 3:** <br>
You will be greeted with a page where you will have to set 
- The table name, table description
- Dataset : set the location to the csv file in the s3 bucket that you are trying to use to create an external table from.
- Add bulk column button to set the column names and their data types. Since I am creating an external managed table from a csv file I can set all the columns to be of string type because csv files are not a structured data and it stores data in form of strings itself saperated by comma.

**STEP 4:** <br>
Once you did all the above steps correctly you will see a query to create external table generated for you. Your query should look like this : 

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `aws-glue-tutorial-aditya`.`uber_data_external_table` (
  `vendorid` string,
  `tpep_pickup_datetime` string,
  `tpep_dropoff_datetime` string,
  `passenger_count` string,
  `trip_distance` string,
  `pickup_longitude` string,
  `pickup_latitude` string,
  `ratecodeid` string,
  `store_and_fwd_flag` string,
  `dropoff_longitude` string,
  `dropoff_latitude` string,
  `payment_type` string,
  `fare_amount` string,
  `extra` string,
  `mta_tax` string,
  `tip_amount` string,
  `tolls_amount` string,
  `improvement_surcharge` string,
  `total_amount` string
) COMMENT "This table holds the data related to customer behaviour and ride metrics for all the taxis that work under uber fleet"
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://aws-glue-s3-bucket-one/raw_data/'
TBLPROPERTIES ('classification' = 'parquet');
```

#### Issues I ran into : After creating an external table
After I was able to successfully create an external table. I tried querying it using this sql query. 

```sql
SELECT * FROM "aws-glue-tutorial-aditya"."uber_data_external_table" limit 10;
```

The moment I ran this query I was slapped by this error right on my face.

```bash
HIVE_BAD_DATA: Malformed Parquet file. Expected magic number: PAR1 got: 6.8 [s3://aws-glue-s3-bucket-one/raw_data/uber_data.csv]

This query ran against the "aws-glue-tutorial-aditya" database, unless qualified by the query. Please post the error message on our forum  or contact customer support  with Query Id: 0a40a5c0-1kk6-4fuc-9kyo-21uc7c7e5521
```

**Reason why this happened?** <br>
After some research I found out that in the UI I may have accidently selected the paraquet format instead of csv. <br>
Look at the error where it says ```Expected magic number: PAR1 got: 6.8``` this means that the metadata created by athena says that it is a paraquet file but my actual data file in the s3 is a csv file and hence I am getting a file format mismatch error.

**Solution :**
Since I am trying to create an external table where the actual data is stored in csv file hence I will have to choose the file format to be of type CSV under the Data format section. <br>
Once you do that and re-create the external table you will get the generated query that looks something like this. 

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `aws-glue-tutorial-aditya`.`uber_data_external_table` (
  `vendorid` string,
  `tpep_pickup_datetime` string,
  `tpep_dropoff_datetime` string,
  `passenger_count` string,
  `trip_distance` string,
  `pickup_longitude` string,
  `pickup_latitude` string,
  `ratecodeid` string,
  `store_and_fwd_flag` string,
  `dropoff_longitude` string,
  `dropoff_latitude` string,
  `payment_type` string,
  `fare_amount` string,
  `extra` string,
  `mta_tax` string,
  `tip_amount` string,
  `tolls_amount` string,
  `improvement_surcharge` string,
  `total_amount` string
) COMMENT "This is a test table to see if I am able to create an externally managed table using a csv file"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://aws-glue-s3-bucket-one/raw_data/'
TBLPROPERTIES ('classification' = 'csv');
```

If you look at the generated SQL code closely, you will see that instead of:

`OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'`

you now have:

`OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'`

Now when I query the table to get the data from my csv file using athena it works 

```sql
SELECT * FROM "aws-glue-tutorial-aditya"."uber_data_external_table" limit 10;
```

OR 

```sql
SELECT * FROM uber_data_external_table LIMIT 10;
```

**Output:**  

| #  | vendorid | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | pickup_longitude | pickup_latitude | ratecodeid | store_and_fwd_flag | dropoff_longitude | dropoff_latitude | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 1 | 2016-03-01 00:00:00 | 2016-03-01 00:07:55 | 1 | 2.5 | -73.97674560546875 | 40.765151977539055 | 1 | N | -74.00426483154298 | 40.74612808227539 | 1 | 9.0 | 0.5 | 0.5 | 2.05 | 0.0 | 0.3 | 12.35 |
| 3 | 1 | 2016-03-01 00:00:00 | 2016-03-01 00:11:06 | 1 | 2.9 | -73.98348236083984 | 40.767925262451165 | 1 | N | -74.00594329833984 | 40.7331657409668 | 1 | 11.0 | 0.5 | 0.5 | 3.05 | 0.0 | 0.3 | 15.35 |

#### Conclusion
- You can query your data stored in a string format separated by comma in a csv file using SQL just like you would query any other database object and this alone gives you a lot of power.
- You can gain this un godly power just by registering external table on top of those csv files. You suddenly find your self capable performing joins transform data using SQL the way you want based on your business requirements. 
- One another super power that you get if decide to use AWS athena.
    - You can query your logs provided the developer had the foresight to generate logs that is seperated by either comma like csv for pipe like psv or tabs like tsv then you can use athena to query your application logs using sql all you have to do is register an external table for that log file in athena. 
    - Why you would want to query your logs using Athena? 
        - You can make debugging in PROD a hell of a lot easier if you decide to design your logs Athena friendly in moments where your project is working fine in local, dev and QA environment but some error is happening in the PROD and you need to know why is this happening and from where? 
        - So make sure you always generate athena friendly logs in your projects if you are going to write it from scratch.

#### CTAS
There are two types of CTAS
- CTAS external
- CTAS managed

CTAS means **C**reate **T**able **A**s **S**elect. If I want to create a new table instead of using the csv file (source file) I want to use the already existing external or managed table then in that case CTAS will help me achieve my goal.

#### CTAS External
Below are the steps to create a external CTAS table where the altered csv files will be stored in the s3 bucket and its metadata will be registered in Athena. 
#### Issues I faced when creating a CTAS External in athena
I first ran this SQL query where I filtered the records based on total_amount where the total_amount must have value in it more than 100 

```sql
SELECT *
FROM uber_data_external_table
WHERE TRY_CAST(total_amount AS DOUBLE) > 100
LIMIT 10;
```
Once the sql query ran successfully and I was satisfied with the result I got I ended up creating a CTAS of this query.

```sql
CREATE TABLE uber_data_total_amount_filtered
WITH (
    format = "PARQUET",
    external_location = "s3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data"
    write_compression = 'SNAPPY'
) AS
SELECT *
FROM uber_data_external_table
WHERE TRY_CAST(total_amount AS DOUBLE) > 100;
```

Then when I ran this sql query athena again slapped me with this error 

```bash
line 5:5: mismatched input 'write_compression'. Expecting: '%', ')', '*', '+', ',', '-', '.', '/', 'AND', 'AS', 'AT', 'OR', '[', '||', <EOF>, <predicate>
```

**Why this error happened?** <br>
If you look closely at this portion of the above code you will see that I have made several mistakes in the above query.

**Mistake 1:**<br>

```sql
WITH (
    format = "PARQUET",
    external_location = "s3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data"
    write_compression = 'SNAPPY'
)
```

As you can see I forgot to put comma after defining external_location and white_compression.

**Mistake 2:** <br>
Another mistake that I did I wrapped the PARQUET and the external_location in double quotes which is wrong. I din't realise that athena query editor would treat it as a literal string instead of the provided value.

Here is the corrected code block 

```sql
WITH (
    format = 'PARQUET',
    external_location = 's3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data',
    write_compression = 'SNAPPY'
)
```

Here is the complete sql query to create this CTAS 

```sql
CREATE TABLE uber_data_total_amount_filtered
WITH (
    format = 'PARQUET',
    external_location = 's3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data',
    write_compression = 'SNAPPY'
) AS
SELECT *
FROM uber_data_external_table
WHERE TRY_CAST(total_amount AS DOUBLE) > 100;
```

Now when I use this sql query 

```sql
SELECT * fROM uber_data_total_amount_filtered;
```

I get this output where total_amount is greater than 100

| # | vendorid | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | pickup_longitude | pickup_latitude | ratecodeid | store_and_fwd_flag | dropoff_longitude | dropoff_latitude | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount |
|---|----------|----------------------|------------------------|-----------------|--------------|------------------|------------------|------------|--------------------|-------------------|------------------|--------------|------------|-------|---------|------------|-------------|----------------------|--------------|
| 1 | 2 | 2016-03-01 00:00:00 | 2016-03-01 00:00:00 | 5 | 30.43 | -73.97174072265625 | 40.79218292236328 | 3 | N | -74.17716979980467 | 40.69505310058594 | 1 | 98.0 | 0.0 | 0.0 | 0.0 | 15.5 | 0.3 | 113.8 |
| 2 | 2 | 2016-03-10 07:09:59 | 2016-03-10 07:36:24 | 1 | 11.43 | -73.87612915039062 | 40.77179336547852 | 5 | N | -73.9721450805664 | 40.847412109375 | 1 | 90.0 | 0.0 | 0.5 | 8.0 | 18.04 | 0.3 | 116.84 |
| 3 | 2 | 2016-03-10 07:20:15 | 2016-03-10 07:50:14 | 6 | 17.34 | -74.00823974609375 | 40.70524978637695 | 3 | N | -74.17768096923827 | 40.6953010559082 | 1 | 65.5 | 0.0 | 0.0 | 24.99 | 17.5 | 0.3 | 108.29 |
| 4 | 2 | 2016-03-10 07:20:50 | 2016-03-10 07:52:07 | 5 | 18.41 | -74.00350189208984 | 40.742141723632805 | 3 | N | -74.17713928222656 | 40.695011138916016 | 1 | 66.5 | 0.0 | 0.0 | 16.86 | 17.5 | 0.3 | 101.16 |
| 5 | 2 | 2016-03-10 07:25:36 | 2016-03-10 08:13:46 | 2 | 25.82 | -73.99234008789062 | 40.731342315673835 | 3 | N | -74.36077880859375 | 40.742630004882805 | 1 | 87.5 | 0.0 | 0.0 | 25.08 | 12.5 | 0.3 | 125.38 |
| 6 | 2 | 2016-03-10 07:26:30 | 2016-03-10 07:45:42 | 2 | 10.78 | -73.84951782226561 | 40.748043060302734 | 5 | N | -73.70035552978516 | 40.758079528808594 | 1 | 84.95 | 0.0 | 0.0 | 17.05 | 0.0 | 0.3 | 102.3 |
| 7 | 2 | 2016-03-10 07:26:57 | 2016-03-10 08:15:27 | 1 | 21.83 | -73.92046356201173 | 40.74689102172852 | 3 | N | -74.1777801513672 | 40.69536972045898 | 1 | 79.0 | 0.0 | 0.0 | 28.89 | 17.0 | 0.3 | 125.19 |
| 8 | 2 | 2016-03-10 07:34:42 | 2016-03-10 08:04:53 | 1 | 19.88 | -73.78182983398438 | 40.64485549926758 | 4 | N | -73.58753204345702 | 40.72392272949219 | 1 | 83.0 | 0.0 | 0.5 | 16.76 | 0.0 | 0.3 | 100.56 |
| 9 | 2 | 2016-03-10 07:36:42 | 2016-03-10 08:29:31 | 1 | 29.3 | -73.86388397216798 | 40.76948165893555 | 4 | N | -73.42192840576173 | 40.77008819580078 | 1 | 125.5 | 0.0 | 0.5 | 25.26 | 0.0 | 0.3 | 151.56 |
| 10 | 2 | 2016-03-10 07:40:55 | 2016-03-10 08:39:06 | 1 | 44.49 | -74.01718139648438 | 40.70839691162109 | 5 | N | -74.64081573486328 | 40.574981689453125 | 1 | 225.0 | 0.0 | 0.0 | 47.56 | 12.5 | 0.3 | 285.36 |
| 11 | 2 | 2016-03-10 07:49:11 | 2016-03-10 07:49:24 | 1 | 0.0 | -73.94847869873048 | 40.7972526550293 | 5 | N | -73.94839477539062 | 40.797229766845696 | 2 | 200.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.3 | 200.3 |
| 12 | 2 | 2016-03-10 07:51:38 | 2016-03-10 08:33:43 | 1 | 18.69 | -73.98118591308595 | 40.741558074951165 | 3 | N | -74.17737579345702 | 40.69525909423828 | 1 | 70.5 | 0.0 | 0.0 | 15.0 | 15.0 | 0.3 | 100.8 |
| 13 | 2 | 2016-03-10 07:53:24 | 2016-03-10 08:59:22 | 1 | 43.65 | -74.00305938720702 | 40.72332382202149 | 5 | N | -74.45540618896483 | 40.84825134277344 | 1 | 143.33 | 0.0 | 0.5 | 39.16 | 12.5 | 0.3 | 195.79 |
| 14 | 2 | 2016-03-10 07:57:54 | 2016-03-10 09:24:29 | 4 | 45.68 | -73.87310028076173 | 40.7741584777832 | 4 | N | -73.19992828369139 | 40.708599090576165 | 2 | 212.5 | 0.0 | 0.5 | 0.0 | 0.0 | 0.3 | 213.3 |
| 15 | 2 | 2016-03-10 08:00:55 | 2016-03-10 08:03:20 | 1 | 1.51 | -74.18675994873048 | 40.69744873046875 | 5 | N | -74.17733764648438 | 40.69512176513672 | 1 | 100.0 | 0.0 | 0.0 | 0.01 | 0.0 | 0.3 | 100.31 |
| 16 | 2 | 2016-03-10 08:01:08 | 2016-03-10 08:02:46 | 1 | 0.0 | -73.81371307373048 | 40.89707946777344 | 5 | N | -73.81371307373048 | 40.897090911865234 | 1 | 106.0 | 0.0 | 0.0 | 15.0 | 0.0 | 0.3 | 121.3 |
| 17 | 2 | 2016-03-10 08:05:32 | 2016-03-10 08:58:31 | 1 | 19.24 | -73.9886703491211 | 40.77503967285156 | 3 | N | -74.17704010009764 | 40.69496154785156 | 1 | 77.0 | 0.0 | 0.0 | 17.96 | 12.5 | 0.3 | 107.76 |
| 18 | 2 | 2016-03-10 08:06:43 | 2016-03-10 09:05:58 | 1 | 40.27 | -74.0052719116211 | 40.744930267333984 | 5 | N | -74.48404693603516 | 40.52020263671875 | 1 | 171.0 | 0.0 | 0.5 | 20.0 | 17.0 | 0.3 | 208.8 |
| 19 | 2 | 2016-03-10 08:08:23 | 2016-03-10 08:57:15 | 5 | 13.87 | -73.87235260009764 | 40.77408218383789 | 5 | N | -73.97357177734375 | 40.86507034301758 | 1 | 100.0 | 0.0 | 0.0 | 20.06 | 0.0 | 0.3 | 120.36 |
| 20 | 2 | 2016-03-10 08:10:49 | 2016-03-10 08:57:12 | 1 | 25.62 | -73.96985626220702 | 40.75365829467773 | 1 | N | -73.80845642089844 | 41.0317497253418 | 1 | 69.0 | 0.0 | 0.5 | 34.0 | 5.54 | 0.3 | 109.34 |

#### CTAS Managed
- Everything is the same except the data is now owned by athena because the data will be stored in the S3 bucket managed by Athena. Hence the data will not be owned by you but AWS.
- Unlike External CTAS here in managed CTAS you don't have to provide the location of where you want to store the data.
- What is a managed location? 
    - There is a special folder that AWS glue will create whenever you want to create a managed data or managed table in AWS
- Unlike External CTAS if you delete the managed CTAS then the actual data will also be deleted along with the metadata. Hence we can say that the data is not owned by you but AWS Glue.
- How to create a managed CTAS
```sql
CREATE TABLE uber_data_most_tipped_filtered_managed_table 
AS 
SELECT * FROM uber_data_external_table WHERE TRY_CAST(tip_amount AS DOUBLE) > 5; 
```

#### Conclusion
**Advantages of External CTAS :**<br>
- Full control over data location 
    - This is mandatory in:
        - Medallion architecture
        - Enterprise data lakes
        - Glue + Athena + EMR setups
    - Managed CTAS:
        - Dumps data in Athena’s results bucket
        - Breaks lake organization
        - Mixes temp + permanent data
- Data is not tied to athena
    - External CTAS output can be used by:
        - AWS Glue
        - Amazon Redshift Spectrum
        - EMR / Spark
        - AWS Lake Formation
        - Other Athena workgroups
    - Managed CTAS data:
        - Is logically Athena-owned
        - Lives in query-result bucket
        - Not intended as a shared dataset
- Predictable permissions and security
    - With external CTAS:
        - You control bucket policies
        - You control IAM roles
        - You control cross-account access
    - Managed CTAS:
        - Uses Athena’s execution role
        - Harder to govern
        - Risk of accidental exposure
- Lifecycle and cost control
    - External CTAS lets you:
        - Apply S3 lifecycle rules
        - Transition to Glacier
        - Auto-delete old partitions
    - Managed CTAS:
        - Lives in Athena results bucket
        - Often accumulates junk
        - Higher long-term cost
- Required for production pipelines
    - External CTAS is required for:
        - Silver / Gold layers
        - Repeatable pipelines
        - Versioned datasets
        - CI/CD-controlled analytics tables
    - Managed CTAS are not pipeline safe.

#### **External CTAS VS Managed CTAS** <br>

| Aspect              | Managed CTAS   | External CTAS |
| ------------------- | -------------- | ------------- |
| Data lake friendly  | No             | yes           |
| Governance          | No             | yes           |
| Reusable by Glue    | questionable   | yes           |
| Safe for production | No             | yes           |
| Easy cleanup        | No             | manual        |
| Best for            | Experiments    | Pipelines     |

**NOTE :** <br>
- Usually when we create a Managed table or CTAS in traditional platform like Databricks and if you drop that table when data is also deleted along with table's metadata.
    - Databricks chooses storage location inside DBFS / Unity catalog
    - Owns both metadata and data
    - Treats it as fully lifecycle-managed
    - So when I drop a managed table in databricks then it not only is able to delete the metadata of the table but also its actual data.
        - How you drop a managed CTAS
        ```sql
        DROP TABLE uber_data_most_tipped_filtered_managed_table;
        ```
- But in AWS the behaviour is different. Even if you delete the Managed table in AWS athena your managed table data in S3 will not be deleted because:
    - Athena is a query engine only
    - Athena is serverless
    - Athena is stateless
    - Athena does not own storage
    - All data lives in
        - All data lives in AWS S3
        - Glue Data Catalog is only used to store table's metadata
    - **What managed table actually means in Athena?**
        - Athena chooses an S3 path automatically
        - Usually inside the Athena results bucket
        - But the data is still in my S3 bucket
    - So when I delete a managed table from AWS using Athena
        - Athena deletes the managed table's metadata
        - Athena does not deletes the S3 objects.
        - S3 is an independednt service of Athena i.e Athena does not assumes it owns the S3 bucket.
- **So we can say that the difference between the managed and external table in case of Athena is that:**
    - When creating external table you have to tell Athena where to store your query result
    - When creating managed table you don't have to tell Athena where to store the query result it will automatically store the query result in the S3 bucket that you set in the Athena's settings

Athena related docs : https://docs.aws.amazon.com/athena/latest/ug/what-is.html

## AWS Glue Crawlers 
- Normally when you have data in files lets say a bunch of csv files in S3 and you want to query those csv files using Athena.
- The only way you do that is by registering those files as a table in AWS Glue catalog using Athena
    - One of the way to register the tables is to define the schema like I did using the AWS UI where I used bulk add columns option provided under column details section.
    - This method is only good for when you only have 8 to 10 columns beyond that this method doesn't make any sense.
    - Suppose when we do not want to prune the columns and we simple want to do the lift and shift and during the lift and shift we find out the number of columns are really really huge lets say 100 columns are there in the table.
    - Writing 100s of columns manually is not possible in this case.
- This is where the concept of crawler comes into the picture.
    - Now when we use crawler we can just use it to crawl through all of the csv files where our actual data is present (It has the capability to crawl through multiple hirerachial folders : In form of partitions we have to crawl through the hirerachial folders)
    - This crawler will crawl through all of the files and this will infer the schema (It will guess which column should be of which type) and it will automatically register this table in AWS glue catalog.
    - Creawler is also used for schema evolution
        - Suppose we have recieved a file in day 1 where the file has 100 columns and in day 2 we recieve a file where the file has 101 columns instead of 100, this is called schema evolution.
        - We just have to say it to our crawler that if you see any other column just add it to the table.
        - It can literally infer the schema whenever you want.

#### NOTE:
- When I was registering a managed or external table in AWS glue catalog from data files stored in S3 I was able to do that because I was the root user and as a root user I had the permissions to access the S3 bucket and Glue catalogs etc. 
- But when I try to register the crawler to register the managed or external tables using the data files stored in S3 bucket then I will have to give appropriate permissions to crawler to register the table on my behalf.
- By default crawler won't be able to see anything in S3 by itself because S3 is fully secured and crawler also doesn't have any permissions to execute anything in AWS glue.
- This means crawler cannot do anything without IAM (permissions)
- IAM is a component of AWS which allows us to provide access to users and services within the AWS ecosystem.
- So a crawler must carry the access (IAM aka Role) under the hood and this access is actually carry the permissions for AWS glue and S3 bucket.

#### What are IAM roles?
- An IAM Role is a set of permissions that can be assumed temporarily by:
    - AWS services
    - Users
    - Applications
    - Other AWS accounts
- Unlike IAM users, roles do not have permanent credentials (no fixed passwords or access key)
- They provide temporary security credentials when assumed.

| Feature                      | IAM User | IAM Role     |
| ---------------------------- | -------- | ------------ |
| Has permanent credentials    | Yes      |   No         |
| Used by humans               | Yes      |   Usually no |
| Used by AWS services         | No       |   Yes        |
| Temporary access             | No       |   Yes        |
| Best practice for production | No       |   Yes        |

### Before you create a crawler you need to create a role that the crawler will use to access S3 and AWS glue catalog for it to be able to register tables from the data present in the S3 bucket
**STEP 1:**<br>
- Search for IAM in AWS and once you hit enter you will be greeted with IAM main page.
- Once you are there you need to go to the side panel on the left and select Role option there.

**STEP 2:**<br>
- Create a new role for Glue service and then hit next
- After hitting next you will be greeted with a new page where you will have to configure the permissions policies for your role that you are creating.

![create_a_role_for_glue](images/create_a_role_for_glue.png)

**STEP 3:**<br>
- In the permission policies page search for glue related permissions
- There you need to check AWSGlueServiceRole IAM policy 
- AWSGlueServiceRole is an AWS managed IAM policy designed for: Allowing AWS Glue to run jobs, crawlers, and workflows. It gives Glue the basic permissions it needs to function properly.
    - This allows:
        - Creating tables
        - Updating metadata
        - Managing crawlers
        - Reading the data catalog

![providing_access_to_glue_service_role](images/providing_access_to_glue_service_role.png)

**STEP 4:** <br>
- Now in permissions policies page I need to search for s3
- There from the list select AmazonS3FullAccess
- AmazonS3FullAccess is an AWS managed IAM policy that grants full administrative access to all Amazon S3 resources in your account.
    - Whoever has this policy can do anything in S3.
    - For learning purposes and to keep things simple I am going to use this policy.
    - It is not at all recommended to use this AmazonS3FullAccess policy in the production environment since it can compromise the data sitting in the S3 in the event if the security key gets leaked.
    - You should always use your custom policy and exactly define what permissions you want to give to crawler when performing operations in S3.

![provide_s3_permissions_to_this_role](images/provide_s3_permissions_to_this_role.png)

**STEP 5:**<br>
- Once you give appropriate permissions just hit next and in the next page you just have to give a name to the role and hit create and your new role will be created.

### How to register a table in AWS glue using AWS crawler?
**STEP 1:**<br>
Go to AWS glue and on the side panel you will find the option named crawlers press this opiton button. After pressing this button you will be greeted with the crawler page.

**STEP 2:**<br>
In this crawler page you will see create crawler button by pressing this button you will greeted with another screen create crawler page.

**STEP 3:**<br>
- In set crawler properties page give your crawler a name and hit next button

![set_crawler_properties](images/set_crawler_properties.png)

**STEP 4:**<br>
- Here in this page I have to set the data sources : Just click on ```add data sources``` button

![choose_data_source_and_classifiers](images/choose_data_source_and_classifiers.png)

- A new ```add data sources``` pop up will open

![add_source_pop_up](images/add_source_pop_up.png)

- Here in this add data sources pop up I am going to set S3 location and hit next after saving the S3 bucket location in that pop up.
- After hitting next you will be greeted with Configure security settings

![choose_s3_path](images/choose_s3_path.png)

**STEP 5:**<br>
- In Configure security settings page you will have to attach the IAM role that you created earlier to this crawler.
- If you have not created the IAM role then this page will provide you with an option to create a new IAM role right then and there but in this case that role will only have the AWSGlueServiceRole permission and not AmazonS3FullAccess which if not carefull can cause problem down the line.

**STEP 6:**<br>
- Attach the database and tell the crawler what should be your table name even though it is optional and hit create.
- Your new crawler will be created and after creation hit run 
- After the successful run of the crawler the table will be created in AWS glue catalog.

#### **Error I faced when ingesting data from csv files using crawler** <br>
- In my case the table that it generated was not right for some reason It added the column names as col_1, col_2 etc.. and the actual column names were inserted as the first row in the table.
- ![error_when_ingesting_data_via_crawler](images/error_when_ingesting_data_via_crawler.png)
#### Solution : Error I faced when ingesting data from csv files using crawler
- The reason why the crawler when ingesting data from the csv file stored in S3 created a table where the actual column names were added in the first row instead of the header is because when creating crawler I forgot to create classifier for it.
- **How to create classifier for crawler?**
    - ![crawler_classifier](images/crawler_classifier.png)
    - You need to configure your crawler's classifier depending on the type of file you want to ingest
    - Here in this case I am trying to register the table for my csv file using crawler hence I chose the options as shown in the picture above.
- One thing to note even after adding this crawler I was not able to get the proper results when I attempted to re-ingest a csv file after deleting its data from the s3 bucket manually where athena stores its query results. 
- The reason for it to happen is because if you crawled a file without any classifer attached to it and you attached the classifier after you crawled that file.
- The classifier will not be used even if you re-ingest that file after deleting all its data from the s3 bucket where athena stores its query result. 
- The only way is to delete the csv file from the source , delete the data where the query results are stored and then re-run the classifier.


## AWS Visual ETL
AWS Visual ETL is a blend of data factory and data flows. <br>
![AWS_Visual_ETL](images/AWS_Visual_ETL.png)

- This is the visual editor where you define your ETL jobs. This is visual ETL builder with No code philosophy. 
- Keep in mind eveything you do in this visual canvas there is a script behind it. The script is being generated behind the scene dynamically based on the componenets you choose and set in your ETL pipeline when desinging the ETL pipeline using this AWS visual ETL tool.
- In here we work with apache spark under the hood when using this tool using the low or no code philosophy.

### Nodes : 
Nodes are tasks that you add in your AWS visual ETL job job.

AWS has created different connectors that allows us to pull data from different sources.

The first transformation in any data engineering pipeline is that you will have to apply drop duplicate transformation before anything else because one must not have duplicate values in the table. 

### Setting up version control for ETL pipeline
![version_control](images/version_control.png)
One thing to note is that when setting up your version control you must have a main branch and if you newly created a repository on github then your branch doesn't exists so you may get an error when trying to save the job.

To solve this issue you will have to create a file and push it into the repo the moment you do it the your AWS visual ETL version control tool automatically picks up the main branch hence you are now able to set a branch to which the code will be pushed to.


You will generally not have to set the version control related stuff in your pipeline you will simply create a new branch and set that branch to it but you should know it.


## How to trigger crawlers on file arrival?
### Architecture
```bash
S3 (new file)
   ↓  (Event notification)
Lambda
   ↓
Glue Crawler (StartCrawler API)
   ↓
Glue Data Catalog updated
   ↓
Athena ready
```

### Create an IAM role for Lambda function
For this I will be needing an IAM role with a custom policy. 
Policy that I require is this : 
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartCrawler",
        "glue:GetCrawler",
        "glue:StopCrawler"
      ],
      "Resource": "arn:aws:glue:ap-south-1:YOUR_ACCOUNT_ID:crawler/crawler_demo"
    }
  ]
}
```
along with this custom policy I will also be needing AmazonS3FullAccess policy provided my AWS

**STEP 1 :** 
- Create a custom policy which gives permissions to other AWS services (in this my Lambda function) to execute and run my AWS glue crawler. <br>
- Go to the side panel > policies > create policy
![policy_page](images/IAM/policy_page.png)

**STEP 2:** 
- Here in the create policy page you will see specify permissions section.
- Toggle from visual to JSON here you will have to paste this JSON code that will define your custom policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartCrawler",
        "glue:GetCrawler",
        "glue:StopCrawler"
      ],
      "Resource": "arn:aws:glue:ap-south-1:YOUR_ACCOUNT_ID:crawler/crawler_demo"
    }
  ]
}
```
![specify_permission](images/IAM/specify_permission.png)

**STEP 3 :**
- In review and create page you will have to provide the policy name and description what your custom policy does 
- After that hit create policy button
![review_and_create_policy](images/IAM/review_and_create_policy.png)

**STEP 4 :**
- After you have created your custom policy you will be able to search it using the name that you gave to your policy
- For demo purpose I have created a custom policy with a name called ```test_policy```
![search_custom_policy](images/IAM/search_custom_policy.png)

**NOTICE :** I have created a custom policy named ```AWSGlueCrawlerPermissionPolicy``` this is one of the policy that will be requiring by the IAM role that I will be attaching to my lambda function for it work properly and trigger my crawler when new files arrive in my S3 bucket.

**STEP 5:** 
- Now I am going to create a new IAM role.
- Go to the side panel > Roles > create role
![IAM_role_create](images/IAM/IAM_role_create.png)

**STEP 6:**
- In Trusted entity type section here I will select AWS service 
- Under use case section I will select Lambda option and then hit next
![select_trusted_entity](images/IAM/select_trusted_entity.png)

**STEP 7:** 
- In Add permissions page under permission policies section search for you custom policy that you created earlier in my case my custom policy name is ```AWSGlueCrawlerPermissionPolicy``` and then select that policy to be included in the IAM role that you are creating.
![search_and_select_custom_policy](images/IAM/search_and_select_custom_policy.png)
- Now you will have to search for ```s3``` You will have to add this ```AmazonS3FullAccess``` policy to your role
- After adding these two policies to your IAM role you can hit next
![s3_bucket_policy](images/IAM/s3_bucket_policy.png)

**STEP 8:**
- After hitting next you will be greeted to a page where you will have to provide the name and description of what this role does.
![name_review](images/IAM/name_review.png)

**NOTICE :** In my case I have already created a role with a name ```TriggerGlueCrawler``` for my lambda function where I have attached my custom policy and AmazonS3FullAccess policy and made sure that my lambda function has approprioate permissions to function as intended.

### Create a Lambda function
- After the IAM role for this Lambda function is created successfully I will now create the lambda function itself.
- This lambda function will be responsible for triggering my AWS glue crawler programatically using a python script when a new file arrives in the S3 bucket.

**STEP 1:**
- Go to Lambda function page my searching it in the search box
- Go to the side panel > Functions > Create functions page.
![create_lambda_function](images/Lambda_function/create_lambda_function.png)

**STEP 2:**
- In this page provide function name 
- Select I will be selecting python as a runtime
- Make sure you don't select durable execution option otherwise your lambda function won't work without setting the destination.
- Under the change default execution role select the option ```Use existing role```
    - From the drop down menu select the role that you created earlier in my case the role that I created was ```TriggerGlueCrawler``` hence I will be selecting that.
- After this click on create function 
![create_lambda_function_page](images/Lambda_function/create_lambda_function_page.png)

**STEP 3:**
- Write Lambda function in this page and click on deploy button after you have written your code to trigger the crawler service 
![write_lambda_code](images/Lambda_function/write_lambda_code.png)
- This code below will start AWS glue crawler when this executes in Lambda function.
- After this click on create function button and your lambda function will be created
```python

import boto3

def lambda_handler(event, context):
    
    crawler_name = "customers_data_silver_table"
    glue = boto3.client('glue')
    
    crawler = glue.get_crawler(Name=crawler_name)
    
    state = crawler['Crawler']['State']
    
    if state == 'READY':
        glue.start_crawler(Name=crawler_name)
        return {
            'statusCode': 200,
            'body': f"Crawler {crawler_name} started."
        }
    else:
        return {
            'statusCode': 200,
            'body': f"Crawler already running."
        }
```

**STEP 4:** **Create an event Listner for S3 bucket**
- Now you will have to set the trigger for your lambda function 
- For this there are two options either you set the trigger by using the menu and options provided inside the lambda function itself or you can do it from S3 bucket
- I chose to attach the lambda function to an event (trigger) after creating it in the S3 bucket properties 
- Go to Amazon S3 > Buckets > properties
- Under the Event notifications menu you will see create event notification button click it to create a new event notification.
![Event_notifications](images/Lambda_function/Event_notifications.png)

**STEP 5:** **Attach the previously created Lambda function to that S3 bucket event**
- After clicking the create event notification button you will be greeted with this page.
- In this page you need to provide your event name , set and configure the event types, and last but not least you will have to set the lambda function that you created just now to this s3 bucket event essentially tie this as a trigger to the lambda function
![create_event_s3_page](images/Lambda_function/create_event_s3_page.png)
![create_event_s3_page2](images/Lambda_function/create_event_s3_page2.png)

 **So how this works ?** <br>
- The moment my ETL pipeline reads the csv file from the ingestion s3 directory it processes the data and then creates parquet files in this S3 bucket. 
- The moment new parquet files arrive after a successfull ETL execution the S3 put event triggers the lambda function which has the python script to trigger the AWS glue crawler. 
- Which in turns creates metadata in AWS glue catalog using those parquet files and now the data in those files can be queried using AWS Athena using sql.

### Create an AWS glue crawler
**STEP 1:**
- Create a new crawler
- This crawler must crawl through a bunch of parquet files in AWS S3 bucket and register their metadata and create a table in AWS glue catalog. 
- This will give us the ability to use AWS Athena to query the data from the parquet file present in S3 bucket.
- Go to AWS glue > side bar > crawlers > create crawler
![create_crawler](images/aws_glue/crawler/create_crawler.png)

**STEP 2:**
- After you click create crawler button you will be greeted with this page.
- Here you will set the name and description for your crawler
- After that hit next
![set_crawler_properties](images/aws_glue/crawler/set_crawler_properties.png)

**STEP 3:**
- After you hit next you will be greeted with this page.
- Here in this page you will add the source where you will have to provide the details regarding your Source s3 bucket location and the folders inside it where the files exists.
- If you are registering the metadata of a table where the data is in csv file then you will have to compulsory create a classifier if you haven't already created one
- But here in this case since the files are in parquet format we don't have to set any classifier.
![set_source_classifier](images/aws_glue/crawler/set_source_classifier.png)

**STEP 4:**
- Here in this page you will have to attach an IAM role that will have appropriate policies attached to it and will allow the crawler to access S3 bucket and AWS glue catalog for it to be able to read the parquet file and register the metadata related to those files in AWS glue catalog so that Athena can use it to query data from those files using SQL.
- It is a best practice that you create your IAM role before hand 
- This IAM role must have ```AWSGlueServiceRole``` and ```AmazonS3FullAccess``` policies attached to it. 
![configure_security](images/aws_glue/crawler/configure_security.png)

**STEP 5:**
- Here you will have to set the output and scheduling 
- Select the database that you created in your AWS glue catalog
- Inside the advanced options set how AWS must handle the table updates if the changes in schema occurs (schema evolution handling)
- Last but not least set the crawler shcedule I have set it to on demand.

## NOTE :
Here are some of the custom policies I experimented with and it worked like a charm hence I am including it here just in case if you need it

- **Create a policy that gives permission to a service to start and get glue crawler**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:StartCrawler",
                "glue:GetCrawler"
            ],
            "Resource": "arn:aws:glue:ap-south-1:406868976171:crawler/RegisterParquetFiles"
        }
    ]
}
```
- **Create a policy to allow services to access glue visual ETL**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "glue:StartJobRun",
            "Resource": "arn:aws:glue:ap-south-1:406868976171:job/Process_Sales_data"
        }
    ]
}
```
- **Create SQS policy to allow services to access SQS messages (This is not needed but I am mentioning it here just in case you want to go this route)**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement1",
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:ChangeMessageVisibility"
            ],
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:S3FileArrivalQueue"
        }
    ]
}
```

## Incremental Load
- Suppose I have csv file named ```day1.csv``` file and on day 1 my glue pipeline ingested data from it.
- Then came day2 now there is another file ```day2.csv``` file and on day 2 my glue pipeline ran to ingest data from it but now along with the data in ```day2.csv``` file ```day1.csv``` file is also ingested.
- This is the default behaviour of AWS glue.
- This is not a desirable behaviour because this will cause unecessary data redundancy issue and will cause data management issues down in the long run as our data grows in size.
- In order to mitigate this issue we can use the option called Idempotency.
- Idempotancy means exactly once behaviour. We can achieve it using bookmarks.
- What a bookmark does is it keeps track of the data that has already been ingested by AWS glue visual ETL pipeline and makes sures that the ingestion process only runs for new files only.

### Implementing Incremental Load (Version 1)
- Things that I want to implement here:
    - Implement automatic visual ETL pipeline trigger when a new file arrives in S3 bucket automatically using Lambda function.
    - Then after ingestion again use Lambda funtion to register the data in AWS glue catalog so that we can use Athena to use SQL to query the data in parquet files prsent in the S3 bucket.
### Architectural diagram
```bash
                ┌─────────────────────────┐
                │     Upstream System     │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │   S3 Source Bucket      │
                │   (CSV Files)           │
                └────────────┬────────────┘
                             │  (PUT Event)
                             ▼
                ┌─────────────────────────┐
                │      Lambda Function    │
                │  (Start Glue Job)       │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │     Glue Visual ETL     │
                │  - Bookmark Enabled     │
                │  - Clean + Transform    │
                │  - Convert Types        │
                │  - Write Parquet        │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │   S3 Data Sink Bucket   │
                │     (Parquet Files)     │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │   Glue Data Catalog     │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │        Athena           │
                │      (SQL Queries)      │
                └─────────────────────────┘
```
### Preparing IAM roles for the services
**STEP 1:** Create an IAM role for Lambda function that starts AWS glue visual ETL pipeline to make sure that it has proper permissions to trigger AWS visual ETL pipeline when put event is triggered in source AWS S3 bucket.
- Create a custom policy for this IAM role. You are free to use the code below to attach it to the Role that you will be creating for this Lambda function.
- It gives appropriate permissions to allow the lambda function to trigger AWS glue visual ETL pipeline.
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "glue:StartJobRun",
      "Resource": "arn:aws:glue:REGION:ACCOUNT_ID:job/YOUR_GLUE_JOB_NAME"
    }
  ]
}
```
- Now attach this policy to the IAM role which will then be used by the lambda function that is going to trigger this AWS glue visual ETL when new files arrives in the source S3 bucket.

**STEP 2:** Create an IAM role for AWS glue 
- This role allows AWS glue to access S3 bucket and It also allows AWS Glue to run jobs, crawlers, and workflows.
- Here in this case we don't need to create a custom policy we can simply use AWS managed policies ```AWSGlueServiceRole``` 
    - For AWS access policy use this json instead to create a custom policy to allow glue to access source S3 securely
    ```json
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
              ],
              "Resource": [
                "arn:aws:s3:::aws-glue-s3-bucket-one",
                "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                "arn:aws:s3:::data-sink-one",
                "arn:aws:s3:::data-sink-one/*"
              ]
            }
          ]
        }
    ```
- Once this role is created we have to attach it to our AWS glue visual ETL job that we have created so that it has appropriate permissions to carry out its job properly.
### Create AWS glue visual ETL job
- This AWS glue visual ETL job is responsible for : 
    - Ingesting data from csv file
    - Cleaning data and converting columns that have numbers into double or integers from string data types accordingly, removing un-necessary symbols.
    - Saving the file in the parquet file in an output S3 bucket.
    - Last but not least this ETL must only read new files that arrive in S3 bucket and not the old ones that have already been processed.
- When saving the job 
    - It will ask you to attach the IAM role that you created without this it will not allow you to save your AWS glue visual ETL job so this is mandatory.
    - ![AWS_glue_creation1](images/aws_glue/visualETL/AWS_glue_creation1.png)
    - You must also set job bookmarks to enable so that ETL only reads new files and ignores older files in S3 that have already been processed when it gets triggered.
    - ![enable_bookmarks](images/aws_glue/visualETL/enable_bookmarks.png)
    - We are doing this to save us from un-necessary costing that happens when the files are re-processed even when it is not required. Its a waste of precious compute resources.
- Your AWS glue visual ETL pipeline should look something like this.
![visual_ETL_pipeline](images/aws_glue/visualETL/visual_ETL_pipeline.png)
- Set data source S3 bucket (Setting up data ingestion node)
![set_data_source_s3](images/aws_glue/visualETL/set_data_source_s3.png)
- Here I have used SQL node to clean data (Setting up data cleaning node)
```sql
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
```
- Use Drop Null Fields node to drop the rows that have all the columns as null (This is a pre-build node provided by AWS)
![drop_null_field_node](images/aws_glue/visualETL/drop_null_field_node.png)
- Set the target to be an S3 bucket using data target S3 bucket node 
    - Here you will have to set the file format in which you want to get output into. The best option to go with is parquet format since it has the actual data along with the table's metadata inside of it. 
    - In the data catalog options if you want your visual ETL pipeline to automatically register the table in AWS glue catalog so that you can use athena to query the table then you will have to go with this option ```Create a table in the Data Catalog and on subsequent runs, keep existing schema and add new partitions```
        - We want to prevent schema changing on its own. We would rather want our pipeline to fail than to cause issues as listed below:
            - Senario 1 the data type of a column changes then 
                - Athena queries may fail
                - Downstream dashboards break
                - BI tools complain
                - Historical partitions may mismatch types
            - Senario 2 Column is removed 
                - Older partitions still have that column but the new one's won't hence again breaking the downstream services that depeneds on that particular column
                - Athena behaves inconsistently
            - Senario 3 A new column is added
                - This one is usually safe — but even then, you should manage schema intentionally.
    - ![target_s3_set_format](images/aws_glue/visualETL/target_s3_set_format.png)
    - Next you will have to set the database that you must have created in AWS glue data catalog
    - Then you will have to set the table name 
    - Add partition key if you think its needed
    - ![target_s3_set_database_table_name_and_partition](images/aws_glue/visualETL/target_s3_set_database_table_name_and_partition.png)
- This is the generated script from the visual ETL pipeline 
```python
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.gluetypes import *
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue import DynamicFrame

    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, ArrayType):
            if isinstance(schema.elementType, StructType):
                output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, NullType):
            output.append(path)
        else:
            x, distinct_set = frame.toDF(), set()
            for i in x.select(path).distinct().collect():
                distinct_ = i[path.split('.')[-1]]
                if isinstance(distinct_, list):
                    distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
                elif isinstance(distinct_, str) :
                    distinct_set.add(distinct_.strip())
                else:
                    distinct_set.add(distinct_)
            if isinstance(schema, StringType):
                if distinct_set.issubset(nullStringSet):
                    output.append(path)
            elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
                if distinct_set.issubset(nullIntegerSet):
                    output.append(path)
        return output

    def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
        nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
        return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Default ruleset used by all target nodes with data quality enabled
    DEFAULT_DATA_QUALITY_RULESET = """
        Rules = [
            ColumnCount > 0
        ]
    """

    # Script generated for node Raw data source S3
    RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", 
        connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
        transformation_ctx="RawdatasourceS3_node1772431168408")

    # Script generated for node SQL Query
    SqlQuery61 = '''
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
    '''
    SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")

    # Script generated for node Drop Null Fields
    DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

    # Script generated for node Silver layer data sink S3
    EvaluateDataQuality().process_rows(frame=DropNullFields_node1772431857999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
    SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
    SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
    SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
    SilverlayerdatasinkS3_node1772432020219.writeFrame(DropNullFields_node1772431857999)
    job.commit()
```

### NOTE:
- During testing you may want to ingest the same file again and again but since you have set the bookmark checkbox to be true in your AWS glue visual ETL pipeline it will not read the same file twice 
- In order to solve this problem you have the capability to reset bookmark for your visual ETL pipeline 
![reset_bookmark](images/aws_glue/visualETL/reset_bookmark.png)

### Start AWS glue visual ETL automatically
- Right now I have to manually run AWS glue visual ETL pipeline 
- I want to automate this process 
    - I want the pipeline to only run when a new csv file arrives in the source S3 bucket.
- One of the ways we can do this is utilizing a Lambda function that triggers this pipeline when a new file arrives in the source S3 bucket

**STEP 1:**
- First we have to create a role for our lambda function with appropirate policies attached to it
- ![create_role_for_lambda_function](images/aws_glue/Lambda_function/create_role_for_lambda_function.png)
- Create a policy with this rule
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "glue:StartJobRun",
            "Resource": "arn:aws:glue:ap-south-1:406868976171:job/ingest_sales_data"
        }
    ]
}
```
- Attach that policy to this IAM role 
- You will also need to attach this ```AWSLambdaBasicExecutionRole``` policy so that lambda function could actually write logs to cloudwatch service.

**STEP 2:** 
- Create a lambda function to trigger AWS glue visual ETL pipeline 
- First we have to set the name for our Lambda function
- Choose the runtime (your preferred programming language) 
- Select the architecture you want to use on which your lambda function will run on
- ![set_lambda_function_name](images/aws_glue/Lambda_function/set_lambda_function_name.png)
- Attach IAM role to this Lambda function in order to make sure that it has permission to actually start AWS glue visual ETL pipeline 
- ![attach_IAM_role_to_lambda](images/aws_glue/Lambda_function/attach_IAM_role_to_lambda.png)
- Write the code to trigger ETL pipeline in glue for your lambda function here in this page
- ![write_code_to_trigger_aws_glue_ETL](images/aws_glue/Lambda_function/write_code_to_trigger_aws_glue_ETL.png)
- Set Lambda Environment Variable
- ![add_lambda_function_env_vars](images/aws_glue/Lambda_function/add_lambda_function_env_vars.png)
- here is the lambda function code 
```python
    import json
    import boto3
    import os

    glue_client = boto3.client("glue")

    GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

    def lambda_handler(event, context):
        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME
            )

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Glue job started successfully",
                    "jobRunId": response["JobRunId"]
                })
            }

        except Exception as e:
            return {
                "statusCode": 500,
                "body": str(e)
            }
```


**STEP 3:** 
- Create an event notification in the source S3 bucket where the files are dumped by the upstream applications/services and attached the lambda function that you created earlier to this event
- ![create_source_s3_event_notification](images/aws_glue/automate_visual_ETL/create_source_s3_event_notification.png)
- Set the event name and event type 
    - Here I have set the event type to be of PUT type
    - The PUT type event detects if a new object is created in S3 bucket if yes then it will trigger the lambda function which has the code to start the AWS glue visual ETL pipeline 
- ![FileArrivalEvent](images/aws_glue/automate_visual_ETL/FileArrivalEvent.png)
- In the same page if you scroll down you will find the options to attach a lambda function that you created earlier to this event 
- ![set_lambda_function_here](images/aws_glue/automate_visual_ETL/set_lambda_function_here.png)

### Implementing Incremental Load (make this implementation production ready)
In order to make incremental load pipeline production ready there few things I need to implement

#### Proposed architecture for this implementation
```bash
                              ┌──────────────────────────┐
                              │     Upstream System      │
                              │  (Apps / APIs / Batch)   │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   S3 RAW Landing Zone    │
                              │  (Versioning Enabled)    │
                              └──────────────┬───────────┘
                                             │
                                (S3 PUT Event Notification)
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │     EventBridge Rule     │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │        Lambda Trigger    │
                              │ - Extract bucket/key     │
                              │ - Validate event         │
                              │ - Idempotency check      │
                              │ - Write metadata record  │
                              └──────────────┬───────────┘
                                             │
                           ┌─────────────────┴──────────────────┐
                           ▼                                    ▼
              ┌──────────────────────┐              ┌──────────────────────┐
              │ DynamoDB Metadata    │              │    SQS (DLQ)         │
              │ - File processed?    │              │ - Failed events      │
              │ - Status tracking    │              │ - Replay capability  │
              └──────────────────────┘              └──────────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   Glue Visual ETL Job    │
                              │ - Bookmark Enabled       │
                              │ - File-level ingestion   │
                              │ - Schema enforcement     │
                              │ - Data quality rules     │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   S3 SILVER Layer        │
                              │  (Partitioned Parquet)   │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   Glue Data Catalog      │
                              │  (Schema Controlled)     │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │        Athena            │
                              │   (Analytics / BI)       │
                              └──────────────────────────┘
```

- **Right now I have attached the lambda function that starts visual ETL pipeline is directly attached to the event notification of the source S3 bucket**
    - This is not a reliable way to trigger Lambda functions 
    - S3 events notifications are:
        - At least once delivery
        - Can produce duplicate events
        - Can occasionally miss events (rare, but possible)
        - No ordering guarantees
    - So if 
        - 10 files at once or the same file is retired 
    - You might
        - Trigger glue twice for the same file
        - Process duplicate data
        - Overwrite partitions incorrectly
    - For small hobby pipelines --> Fine 
    - For production ingestion --> Risky
    - If 500 files arrive in 1 minute then S3 will invoke lambda function 500 times and Lambda function will try to start 500 glue jobs , Now you hit:
        - Glue concurrency limit
        - API throttling
        - Random failures
        - Partial ingestion
    - No replay capability:
        - You cannot easily replay missed events 
        - S3 does not perists event history for you 
    - Tight coupling
        - Right now bucket is tightly coupled to 
            - One Lambda function
            - One Glue job
        - If tomorrow 
            - I want to send the same event to another system
            - Or introduce validation
            - Or introduce duplication
            - I must rewrite everything and that's a brittle architecture
- What production Data platform do instead 
    - You can either use EventBridge
    - ```bash
        S3 → EventBridge → SQS → Lambda → Glue
        ```
    - Or your can use SQS directly removing EventBridge
    - ```bash
        S3
        ↓
        SQS (queue)
        ↓
        Lambda (polls queue)
        ↓
        Glue
        ```
    - I went with the EventBridge method 
    - Pros of using this architecture
        - Each layer is independent.
        - If tomorrow you want 
            - Another Lambda 
            - A step function
            - A montioring pipeline 
            - A metadata tracker 
        - You just add another EventBridge rule.
        - You don't need to touch S3 
    - Using SQS as the buffer
        - Amazon SQS gives you:
            - Persistent message storage.
            - Visibility timeout
            - Controlled retries 
            - Dead-Letter queue
        - If lambda fails:
            - Message becomes visible again
            - Gets retried 
            - After N retries --> Moves to DLQ
    - Replay capability
        - If something breaks 
            - Bad glue transformation
            - Schema issue 
            - Partition error
        - You can:
            - Reprocess DLQ messages 
            - Re-drive message from SQS
            - Manually replay failed ingestion
        - This is critical in production data systems . Without a queue replay is painful.
    - Backpressure & Concurrency Control
        - If 500 files arrive:
            - With SQS
                - Message wait safely
                - Lambda scales gradually
                - You can control:
                    - Batch size 
                    - max concurrency
                    - parallelism
                - You just introduced flow control into your ingestion
    - Event Filtering at the Platform Layer
        - Because I used:
            - I can filter events before they even hit SQS:
                - Only .csv files
                - Only raw/sales/ prefix
                - Ignore temp files
                - Ignore test uploads
            - That reduces noise and cost.
    - Clean Failure Isolation
        - If:
            - Glue API throttles 
            - Lambda fails 
            - Network hiccup happens
        - The event is safe inside SQS
    - Horizontal scalability
        - Add muliple Lambda consumers
        - Scale out ingestion
        - Process different datasets in parallel
    - This architecture scales horizontally
    - Observability & Monitoring
        - Now you can monitor 
            - SQS queue depth 
            - DLQ message count 
            - Lambda error rate
            - Glue job failures
        - If queue depth spikes:
            - Ingestion bottleneck
        - If DLQ grows 
            - Transformation bug

### Implementation steps (Version 2)
#### Create a DLQ
- A Dead Letter Queue is a special queue where messages are sent after they fail processing multiple times. It’s a safety net for failed events.
- Storing events in case of failure is necessary so that we can replay the ingestion pipeline after fixing the errors. Hence making sure that data is not lost silently

**STEP 1:** 
- Give your SQS a name and enable encryption
![create_DLQ_1](images/production_grade_glue_implementation/sqs_dlq_implementation/create_DLQ_1.png)
![create_DLQ_2](images/production_grade_glue_implementation/sqs_dlq_implementation/create_DLQ_2.png)
![create_DLQ_3](images/production_grade_glue_implementation/sqs_dlq_implementation/create_DLQ_3.png)
- This is the access policy I used:
```json
{
  "Version": "2012-10-17",
  "Id": "__default_policy_ID",
  "Statement": [
    {
      "Sid": "__owner_statement",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::aws_account_id:root"
      },
      "Action": "SQS:*",
      "Resource": "arn:aws:sqs:ap-south-1:aws_account_id:sales-ingestion-dlq"
    },
    {
      "Sid": "AllowEventBridgeToSendMessage",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:aws_account_id:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:aws_account_id:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    },
    {
      "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:aws_account_id:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:aws_account_id:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    }
  ]
}
```
- Now in the event of failure the event messages won't be lost instead it will saved here in this SQS named ```sales-ingestion-dlq``` 
![failure_DLQ_message_polling](images/production_grade_glue_implementation/sqs_dlq_implementation/failure_DLQ_message_polling.png)

#### Configuring S3 bucket for EventBridge
- Here If you have any pre-configured Event notification in your S3 bucket then delete it We don't need it anymore. 
- Instead turn on the Amazon event bridge option, this is present just below the event notification option in the properties tab of the S3 bucket.
![set_s3_bucket_event_bridge](images/production_grade_glue_implementation/S3_bucket_configurations/set_s3_bucket_event_bridge.png)

#### Create an ETL pipeline

**STEP 1:**
- Create a role that has policies with appropriate permissions for AWS glue catalog so that it can perform its actions properly without any security risk
- Create a custom policy named ```LimitedS3PermissionPolicy```
```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::aws-glue-s3-bucket-one",
                    "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                    "arn:aws:s3:::data-sink-one",
                    "arn:aws:s3:::data-sink-one/*"
                ]
            }
        ]
    }
```
- Create a Role named ```AWSGlueRole```
- Attach policies to this role 
    - attach ```LimitedS3PermissionPolicy``` custom policy
    - attach aws managed policy named ```AWSGlueServiceRole```

**STEP 2:**
- In this you will have to attach a role to your Glue visual ETL pipeline 
![create_ETL_1](images/production_grade_glue_implementation/ETL/create_ETL_1.png)
- Now you will have to set here 
    - Enable bookmarks to make sure only new files are processed and old files are ignored
    - Enable job queue
    - Enable job insights
![create_ETL_2](images/production_grade_glue_implementation/ETL/create_ETL_2.png)


**STEP 3:**
- This is the code for the ETL pipeline 
- This ETL pipeline ingest data in form of csv file from a source S3 bucket
- It removes the rows that has all the values in its columns set as null
- It then transforms the data in a column into its appropriate datatype. The reason I had to do this is because in csv file all the columns wheather it has data in numbers are of string datatype.
- It then register the metadata in the AWS glue catalog after the actual data is stored in parquet files in the target S3. It is to make sure that the data can be queried from those output parquet files using Athena.
- ```python
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'source_key'
    ])

    source_bucket = args['source_bucket']
    source_key = args['source_key']
    ```
    - This code accepts the arguments related to bucket and the recently PUT object key that is passed on to AWS glue ETL pipeline by Lambda function that has invoked it when the PUT event happened
- ```python
    connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
    ```
    - This line is very important when it comes to optimization 
    - In the earlier version my ETL pipeline was scanning the whole folder everytime it was invoked by lambda function but now it only scans the file that has arrived in the S3 hence saving precious compute. As a result we are able to save cost.
    - If three files are uploaded to S3 then for each file three events will be generated then those three events will actiavte three lambda functions which will be invoking three separate glue visual ETL pipelines. These pipelines will then execute one by one 
- Below is the complete ETL pipeline python code 
- ```python
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.gluetypes import *
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue import DynamicFrame

    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, ArrayType):
            if isinstance(schema.elementType, StructType):
                output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, NullType):
            output.append(path)
        else:
            x, distinct_set = frame.toDF(), set()
            for i in x.select(path).distinct().collect():
                distinct_ = i[path.split('.')[-1]]
                if isinstance(distinct_, list):
                    distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
                elif isinstance(distinct_, str) :
                    distinct_set.add(distinct_.strip())
                else:
                    distinct_set.add(distinct_)
            if isinstance(schema, StringType):
                if distinct_set.issubset(nullStringSet):
                    output.append(path)
            elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
                if distinct_set.issubset(nullIntegerSet):
                    output.append(path)
        return output

    def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
        nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
        return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'source_key'
    ])

    source_bucket = args['source_bucket']
    source_key = args['source_key']

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Default ruleset used by all target nodes with data quality enabled
    DEFAULT_DATA_QUALITY_RULESET = """
        Rules = [
            ColumnCount > 0
        ]
    """

    # Script generated for node Raw data source S3
    RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", 
        # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
        connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
        transformation_ctx="RawdatasourceS3_node1772431168408")
    # for debugging only
    print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

    # Script generated for node SQL Query
    SqlQuery61 = '''
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
    '''
    SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")

    # Script generated for node Drop Null Fields
    DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

    # Script generated for node Silver layer data sink S3
    EvaluateDataQuality().process_rows(frame=DropNullFields_node1772431857999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
    SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
    SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
    SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
    SilverlayerdatasinkS3_node1772432020219.writeFrame(DropNullFields_node1772431857999)
    job.commit()
    ```

#### Create a Lambda function 
- This Lambda function will trigger the Visual ETL pipeline

**STEP 1:**
- Create a role that gives appropirate access rights of the services to make sure that the lambda function works as intended.
- Create a custom policy named ```AWSGlueStartJobAccessPolicy```
    - Use this policy json 
    - ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "glue:StartJobRun",
                        "Resource": "arn:aws:glue:ap-south-1:406868976171:job/ingest_sales_data"
                    }
                ]
            }
        ```

- Create a role named ```TriggerAWSGlueVisualETLPipeline```
    - Attach this aws managed policy to it ```AWSLambdaBasicExecutionRole```
    - Attach this ```AWSGlueStartJobAccessPolicy``` custom policy that you made earlier to it 

**STEP 2:**
- Write the code 
- ```python
    # VERSION 2 : Works with Event Bridge and is Production READY
    import json
    import boto3
    import os

    glue_client = boto3.client("glue")

    GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

    def lambda_handler(event, context):
        try:
            # event recieved from event bridge
            print(f"Lambda function | Event recieved from event bridge | {event}") 
            # Extract bucket and key from EventBridge event
            bucket = event["detail"]["bucket"]["name"]
            key = event["detail"]["object"]["key"]

            print(f"Lambda function | Received new object: | s3://{bucket}/{key}")

            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--source_bucket": bucket,
                    "--source_key": key
                }
            )

            print(f"Lambda function | Started Glue job | {response['JobRunId']}")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Glue job started successfully",
                    "jobRunId": response["JobRunId"]
                })
            }

        except Exception as e:
            print(f"Error: {str(e)}")
            raise e    except Exception as e:
            print(f"Error: {str(e)}")
            raise e
    ```

- Why this is a better code than the previous version?
    - Previous code 
        - ```python
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME
            )
            ```
        - It was only taking GLUE_JOB_NAME as an input argument 
        - Hence the lambda function have no idea which file triggered it.
        - In the previous version Glue used to scan for folders (Glue was setup to be a folder-based job)
        - Cannot isolate failures
        - Not idempotent
        - Hard to debug
        - Wastes compute
        - What the previous code was doing "Something happened in S3 run glue visual ETL blindly"
        - That was not event based ingestion that was reactive batch processing.
    - New version
        - ```python
                bucket = event["detail"]["bucket"]["name"]
                key = event["detail"]["object"]["key"]

                print(f"Received new object: s3://{bucket}/{key}")

                response = glue_client.start_job_run(
                    JobName=GLUE_JOB_NAME,
                    Arguments={
                        "--source_bucket": bucket,
                        "--source_key": key
                    }
                )
            ```

        - The event argument is the one recieving the event related data from the event bridge 
            - This data contains the source_bucket and object_key.
            - The lambda function then passes this data to AWS glue visual ETL pipeline so that it can use this information to only scan for the file that has arrived in the S3 bucket instead of scanning the whole folder, hence saving DBU compute and ultimately saving cost.  
        - This code allows me to implement File level ingestion logic when developing glue visual ETL pipeline.
        - It Becomes Truly Event-Driven
        - Instead of 
            - ```bash
                    File arrives → Run Glue
                ```
        - This version of code does this
            - ```bash
                    File arrives → Extract exact object key → Pass to Glue
                ```
        - Glue will now be able to process 
            - ```bash
                    s3://bucket/specific_file.csv
                ```
        - This eleminates:
            - Full folder scans 
            - Reprocessing old files
            - Hidden duplicates
        - Perfect Failure Isolation
            - ```bash
                    file_1.csv → corrupt
                    file_2.csv → good
                ```
            - With Version 1:
                - Entire folder ingestion may fail.
            - With version 2:
                - Only ```file_1.csv``` fails.
                - ```file_2.csv``` processes normally.
        - Enables DLQ + Replay
            - Since I am extracting 
                - ```bash
                        bucket
                        key
                    ```
            - That information is preserved in Amazon SQS (DLQ)
                - If Glue fails:
                    - The exact file path remains inside the message.
                    - You can replay exactly that file.
                - Version 1 cannot replay specific files because it doesn't know which file caused failure.
        - Cost Efficiency
            - Version 1 causes:
                - Folder scans
                - Reading multiple files
                - Unnecessary spark overhead
            - Version 2:
                - Reads exactly one file
                - Lower DPU usage
                - Faster execution
                - Better cost control
        - Debuggability:
            - Version 2 prints:
                - ```python
                        print(f"Received new object: s3://{bucket}/{key}")
                    ```
                - Now your cloud watch logs show:
                - ```bash
                        Received new object: s3://raw/sales/file123.csv
                    ```
            - This allows us to immediately know
                - Which file triggered
                - Which file failed
                - Which file succeeded
        - Enables Idempotency
            - Before starting Glue:
                - Check DynamoDB
                - Check metadata store
                - Check processed files table
        - Horizontal Scalability
            - Version 2:
                - 100 Lambda invocations
                - 100 independent glue runs
                - Fully parallel
                - Isolated failures
            - Version 1:
                - Possibly 1 glue run scanning entire folder
                - Bottleneck risk
                - Partial processing
        - Architecturally Clean
            - Version 1 = "Batch job trigger"
            - Version 2 = "Event payload processor"

**STEP 3:**
- Attach the role that you created for this lambda function here in this page.
- Attaching role with appropriate permission is necessary for security purposes.
- We want this lambda function to access services in AWS securely and give minimum possible permission for it to be able to function properly.
![lambda_function_attach_role](images/production_grade_glue_implementation/lambda_setup/lambda_function_attach_role.png)
![add_role_to_lambda](images/production_grade_glue_implementation/lambda_setup/add_role_to_lambda.png)

**STEP 4:**
- Add environment variables that is required by this lambda function here 
![environment_variable_lambdafunction](images/production_grade_glue_implementation/lambda_setup/environment_variable_lambdafunction.png)

**NOTE :**
- If you have set everything correctly then you will see the logs in AWS cloudwatch for your lambda function
![cloudwatch_logs_for_lambda](images/production_grade_glue_implementation/lambda_setup/cloudwatch_logs_for_lambda.png)
- Here all your lambda function logs will be dumped
![cloudwatch_logs_for_lambda2](images/production_grade_glue_implementation/lambda_setup/cloudwatch_logs_for_lambda2.png)


#### Create an EventBridge
**STEP 1:**
- Setup trigger events
![create_event_bridge_1](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_1.png)
- Select the S3 simple storage service for trigger event 
- Once thats done now use this event pattern filter 
    - ```json
            {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {
                "name": ["aws-glue-s3-bucket-one"]
                },
                "object": {
                "key": [{
                    "prefix": "raw_data/sales_data/"
                }]
                }
            }
            }
        ```
    - Here ```"raw_data/sales_data/"``` is the directory inside this S3 where your files are arriving.
- ![create_event_bridge_2](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_2.png)

**STEP 2:**
- When configuring the targets for the EventBridge make sure that you have disabled the input transformation configuration
- What this does ? --> Use Target input transformer to customize the text from an event before EventBridge passes the information to the target. When target input transformer is not defined, the original event will be sent to a target.
- We don't need it here
![create_event_bridge_3](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_3.png)

**STEP 3:**
- Select target this account since our Lambda function is present in this account only
![create_event_bridge_4](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_4.png)
- Here make sure that refrain from using your own custom role for your EventBridge instead let AWS create a new Role for your EventBridge on your behalf
![create_event_bridge_5](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_5.png)
- Setup retry policy for your lambda function that triggers visual ETL when this file arrival PUT event happens in S3 bucket
- Setup the age of events in hours 
    - This will set the number of hours an unprocessed event can be kept
- Here attach your SQS which will serve as DLQ to store messages in case of a failure after the fix is completed the stored messages will be re-processed.
    - This prevent data loss in case of a failure.

![create_event_bridge_6](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_6.png)

#### Room for further improvements in this 
Current setup works because:
- I enabled Glue Job Bookmarks
- I trigger Glue per file
- I are not doing complex orchestration
- I are not handling replay or partial failure logic

Right now my incremental guarantee depends entirely on:
- Glue bookmarks

That works for:
- Simple pipelines
- Single-account setups
- No replay requirements
- No external state management
- No manual backfills

But bookmarks are:
- Internal to Glue
- Opaque
- Not easily inspectable
- Hard to coordinate across multiple services

Current implementation have the infrastructure for replayability, but I have NOT fully implemented replayability yet.
- GAP 1 — Current system Don’t Track Processing State (BIGGEST ISSUE)
    - REQUIRED FIX → Add Idempotency Store (DynamoDB)
    - This is mandatory for replayability.
- GAP 2 — Current System Doesn't Detect Glue Failures Properly
    - Step Functions (BEST PRACTICE)
        - waits for completion
        - retries
        - catches failure
        - triggers DLQ
- GAP 3 — Current system Don’t Have Replay Workflow Yet
    - Right now DLQ exists but:
    - user replay manually (not production-grade).

## Improvement implementation on current system (Version 3)
- In this version I am going to implement Step function and replace the lambda function that I was using earlier to trigger the ETL pipeline.
- Reasons : 
    - Since (Version 2) implementation of this ETL pipeline where I was using lambda function instead of a step function I was not able to reliably detect if the ETL pipeline failed and hence was not able to send the event messages sent by S3 to DLQ reliably in case of a failure.
### Implementation 
#### Step 1 : Source S3 bucket setup
- Enable events to be sent to EventBridge in your S3 bucket 
- ![enable_event_bridge](images/production_grade_glue_version2_dlq_/S3_settings/enable_event_bridge.png)

#### Step 2 : EventBridge setup 
- The name of the event bridge is ```ActivateLambdaFuncEventBridgeRules```
- Setup Trigger events for your EventBridge
- ![setting_up_trigger_event](images/production_grade_glue_version2_dlq_/EventBridge/setting_up_trigger_event.png)

- Event pattern (filter) that I set when setting up the Event Bridge
```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["aws-glue-s3-bucket-one"]
    },
    "object": {
      "key": [{
        "prefix": "raw_data/sales_data/"
      }]
    }
  }
}
```

- Setting up the targets that must be invoked if file is put inside the source S3 bucket
- ![step_function_target_setup](images/production_grade_glue_version2_dlq_/EventBridge/step_function_target_setup.png)
- One thing to note is that event if the image shows that I have used an existing role you must always select ```create a new role for this specific resource``` 
- The reason the image shows the other option is because I have taken a screenshot of already existing EventBridge that's why.
- You don't need to create role for this service manually AWS automatically creates a role with appropriate permissions for event bridge to use.
- ![retry_policy_of_event_bridge](images/production_grade_glue_version2_dlq_/EventBridge/retry_policy_of_event_bridge.png)

#### Step 3 : Setup a DLQ (Dead-Letter-Queue)
- The name of this DLQ is ```sales-ingestion-dlq```
- ![dlq_conf](images/production_grade_glue_version2_dlq_/sqs/dlq_conf.png)
- ![encryption_dlq](images/production_grade_glue_version2_dlq_/sqs/encryption_dlq.png)
- Access policy of DLQ
```json
{
  "Version": "2012-10-17",
  "Id": "__default_policy_ID",
  "Statement": [
    {
      "Sid": "__owner_statement",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::406868912345:root"
      },
      "Action": "SQS:*",
      "Resource": "arn:aws:sqs:ap-south-1:406868912345:sales-ingestion-dlq"
    },
    {
      "Sid": "AllowEventBridgeToSendMessage",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:406868971234:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:406868971234:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    },
    {
      "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:406868971234:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:406868912345:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    }
  ]
}
```
- ![redrive_policy_dlq](images/production_grade_glue_version2_dlq_/sqs/redrive_policy_dlq.png)

#### Step 4 : Create a step function
- This step function will be used to not only start a Glue function but it will also be used to monitor if the glue job was a sucess or a failure 
```bash
RunGlueJob → Success
          ↘ SendToDLQ → FailState
```
- How this state function works ?
    - When a file lands in S3
    - EventBridge triggers this Step function
    - Step function runs glue job
    - If the glue job fails then the event is send to DLQ

```json
{
  "Comment": "Glue ETL Orchestration with DLQ",
  "StartAt": "RunGlueJob",
  "States": {
    "RunGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "ingest_sales_data",
        "Arguments": {
          "--source_bucket.$": "$.detail.bucket.name",
          "--source_key.$": "$.detail.object.key"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "SendToDLQ"
        }
      ],
      "Next": "Success"
    },
    "SendToDLQ": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sqs:sendMessage",
      "Parameters": {
        "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868971234/sales-ingestion-dlq",
        "MessageBody.$": "$"
      },
      "Next": "FailState"
    },
    "FailState": {
      "Type": "Fail"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
```

- ```"arn:aws:states:::glue:startJobRun.sync"```
    - Start Glue job
    - WAIT until it finishes
- If ```.sync``` wasn’t used: Step Function would move immediately.

```json
"Arguments": {
          "--source_bucket.$": "$.detail.bucket.name",
          "--source_key.$": "$.detail.object.key"
        }
```
- Here the step function is extracting the bucket name and object key from the event bridge and then passing it on to AWS glue ETL pipeline where we will be able to access it using ```source_bucket``` and ```source_key``` params.
- In Glue ETL ```getResolvedOptions(sys.argv, ...)``` this code will catch all the params passed on by this step function

```json
"Retry": [{
  "ErrorEquals": ["States.TaskFailed"],
  "IntervalSeconds": 30,
  "MaxAttempts": 2,
  "BackoffRate": 2
}]
```
- Here I have decided to put the retry logic in the step function instead of eventBridge because it does makes more sense since step function is not only the one startiing the glue job but it is also the one waiting if the job was a success or a failure.
- Why this is done ?
    - transient cluster issues
    - network blips 
    - service throttling

```json
"Catch": [{
  "ErrorEquals": ["States.ALL"],
  "Next": "SendToDLQ"
}]
```
Meaning:
- If STILL fails after retries
- Send to DLQ

- You will have to add an AWS managed policy ```AmazonSQSFullAccess``` to the generated IAM role when creating a Step function.

**NOTICE :**

**There was an issue related to where when ETL fails the step function was not sending the message to DLQ**
- This happened only when the ETL recieved a corrupt csv file.
- This happened because of the code below
```json
"Retry": [{
  "ErrorEquals": ["States.TaskFailed"],
  "IntervalSeconds": 30,
  "MaxAttempts": 2,
  "BackoffRate": 2
}]
```
- ```"ErrorEquals": ["States.TaskFailed"],``` Glue ETL can return not only TaskFailed but other types of failure states as well
- solution is to use ```"States.ALL"``` instead of ```"States.TaskFailed"``` take a look at the code below and replace the above one with this
```json
"Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
```

#### Step 5 : Create another step function to handle manual replays in case of a failure
- The name of this step function is ```replay_failed_ingestion```
- This is the second step function : ```DLQ → StepFn → Glue (retry) → Success```
- It replays job safely
    - This step function reads the message from DLQ ```sales-ingestion-dlq```
    - Extracts original payload
    - Re-runs Glue
    - Deletes message if success
- ```"MaxNumberOfMessages": 1```
    - Pulls only one message per execution this prevents overload + ensures a controlled replay.
- ```"Next": "FailState"```
    - If replay still fails then stop here
    - In this case DLQ message will not be deleted
- ```"Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage"```
    - Deletes message only if: 
        - Replay is successfull
- Here is the full code for this step function
```json
{
  "Comment": "Replay ETL from DLQ with batching, tracking, and safeguards",
  "StartAt": "ReceiveFromDLQ",
  "States": {
    "ReceiveFromDLQ": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
      "Parameters": {
        "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
        "MaxNumberOfMessages": 10
      },
      "ResultPath": "$.dlq",
      "Next": "CheckIfEmpty"
    },
    "CheckIfEmpty": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.dlq.Messages",
          "IsPresent": false,
          "Next": "Success"
        }
      ],
      "Default": "ReplayBatch"
    },
    "ReplayBatch": {
      "Type": "Map",
      "ItemsPath": "$.dlq.Messages",
      "MaxConcurrency": 2,
      "Iterator": {
        "StartAt": "ExtractPayload",
        "States": {
          "ExtractPayload": {
            "Type": "Pass",
            "Parameters": {
              "receiptHandle.$": "$.ReceiptHandle",
              "body.$": "States.StringToJson($.Body)"
            },
            "ResultPath": "$.parsed",
            "Next": "ParseCause"
          },
          "ParseCause": {
            "Type": "Pass",
            "Parameters": {
              "receiptHandle.$": "$.parsed.receiptHandle",
              "cause.$": "States.StringToJson($.parsed.body.Cause)"
            },
            "ResultPath": "$.final",
            "Next": "CheckReplayCount"
          },
          "CheckReplayCount": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:getItem",
            "Parameters": {
              "TableName": "dlq_replay_tracking",
              "Key": {
                "object_key": {
                  "S.$": "$.final.cause.Arguments['--source_key']"
                }
              }
            },
            "ResultPath": "$.ddb",
            "Next": "ReplayLimitCheck"
          },
          "ReplayLimitCheck": {
            "Type": "Choice",
            "Choices": [
              {
                "Variable": "$.ddb.Item.retry_count.N",
                "NumericGreaterThanEquals": 3,
                "Next": "SkipPoisonMessage"
              }
            ],
            "Default": "RunGlueJob"
          },
          "RunGlueJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "ingest_sales_data",
              "Arguments": {
                "--source_bucket.$": "$.final.cause.Arguments['--source_bucket']",
                "--source_key.$": "$.final.cause.Arguments['--source_key']"
              }
            },
            "Retry": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "IntervalSeconds": 30,
                "MaxAttempts": 2,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "UpdateRetryCount"
              }
            ],
            "Next": "DeleteMessage"
          },
          "UpdateRetryCount": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:updateItem",
            "Parameters": {
              "TableName": "dlq_replay_tracking",
              "Key": {
                "object_key": {
                  "S.$": "$.final.cause.Arguments['--source_key']"
                }
              },
              "UpdateExpression": "ADD retry_count :inc",
              "ExpressionAttributeValues": {
                ":inc": {
                  "N": "1"
                }
              }
            },
            "Next": "FailState"
          },
          "DeleteMessage": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
            "Parameters": {
              "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
              "ReceiptHandle.$": "$.final.receiptHandle"
            },
            "Next": "SuccessState"
          },
          "SkipPoisonMessage": {
            "Type": "Pass",
            "Next": "SuccessState"
          },
          "FailState": {
            "Type": "Fail"
          },
          "SuccessState": {
            "Type": "Succeed"
          }
        }
      },
      "Next": "Success"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
```

#### Step 6 : Create a glue ETL job
- Before you create this ELT you will have to create an IAM role that give appropriate permission to this ETL for it to be able to utilize the S3 and Glue catalog so that it can register the data in the table so that the users can use Athena to query the data using sql.
- The IAM role that is used by this ETL job is named ```AWSGlueRole```
    - In this role you will have to attach an AWS managed policy called ```AWSGlueServiceRole```
    - Then one more custom policy you will have to attach is a custom policy named ```LimitedS3PermissionPolicy```
    ```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::aws-glue-s3-bucket-one",
                    "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                    "arn:aws:s3:::data-sink-one",
                    "arn:aws:s3:::data-sink-one/*"
                ]
            }
        ]
    }
    ```
- Here is the complete code
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

from functools import reduce
from pyspark.sql import functions as F

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_key'
])

source_bucket = args['source_bucket']
source_key = args['source_key']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """Rules = [
    ColumnCount > 0,

    IsComplete "rating",
    IsComplete "rating_count",
    IsComplete "discounted_price",
    IsComplete "actual_price",

    ColumnValues "rating" >= 0,
    ColumnValues "rating_count" >= 0,
    ColumnValues "discounted_price" >= 0,
    ColumnValues "actual_price" >= 0,
    ColumnValues "discount_percentage" >= 0,
    
    IsUnique "product_id"
]"""

# -------- DELIMITER CORRUPTION CHECK -------- #
import csv
import io

raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").collect()

if len(raw_lines) < 2:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (file has no data rows)")

def count_csv_columns(line: str) -> int:
    """Count columns correctly, respecting quoted fields containing commas."""
    try:
        return len(next(csv.reader(io.StringIO(line))))
    except StopIteration:
        return 0

header_line = raw_lines[0][0]
expected_col_count = count_csv_columns(header_line)

bad_row_indices = []
for i, row in enumerate(raw_lines[1:], start=2):
    line = row[0]
    actual_col_count = count_csv_columns(line)
    if actual_col_count != expected_col_count:
        bad_row_indices.append((i, actual_col_count, line[:80]))

if bad_row_indices:
    details = "\n".join(
        [f"  Line {idx}: expected {expected_col_count} cols, got {actual} → {preview}..."
         for idx, actual, preview in bad_row_indices]
    )
    raise Exception(
        f"Glue Visual ETL | Corrupted CSV detected — {len(bad_row_indices)} row(s) have wrong column count "
        f"(likely semicolons used as delimiters instead of commas):\n{details}"
    )

print(f"Glue Visual ETL | Column count validation passed: all rows have {expected_col_count} columns")
# -------- DELIMITER CORRUPTION CHECK -------- #

# Script generated for node Raw data source S3
RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE", "optimizePerformance": False}, connection_type="s3", format="csv", 
    # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
    connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
    transformation_ctx="RawdatasourceS3_node1772431168408")
# for debugging only
print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

df = RawdatasourceS3_node1772431168408.toDF()

print("Glue Visual ETL | DEBUG bucket:", source_bucket)
print("Glue Visual ETL | DEBUG key:", source_key)
print("Glue Visual ETL | DEBUG columns:", df.columns)
print("Glue Visual ETL | DEBUG row count:", df.count())
print("Glue Visual ETL | DEBUG schema:", df.schema)
print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())

# -------- CORRUPTION CHECK (SAFE VERSION) -------- #

# If Spark inferred no columns → corrupted file
if len(df.columns) == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (no columns inferred)")

expected_cols = len(df.columns)

# If dataframe empty → corrupted file
if df.limit(1).count() == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (empty dataframe)")

# Row-level corruption detection
null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]

bad_rows = df.filter(
    reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7
)

if bad_rows.limit(1).count() > 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (null-heavy rows)")
# -------- CORRUPTION CHECK (SAFE VERSION) -------- #

# Script generated for node SQL Query
SqlQuery61 = '''
SELECT
    product_id,
    product_name,
    category,
    about_product,
    user_id,
    user_name,
    review_id,
    review_title,
    review_content,
    img_link,
    product_link,

    -- discounted_price: ₹149 → 149.0
    CAST(
        REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS discounted_price,

    -- actual_price: ₹1,000 → 1000.0
    CAST(
        REGEXP_REPLACE(actual_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS actual_price,

    -- discount_percentage: 85% → 85.0
    CAST(
        REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
        AS DOUBLE
    ) AS discount_percentage,

    -- rating: 3.9 → 3.9
    CAST(
        REGEXP_REPLACE(rating, '[^0-9.]', '')
        AS DOUBLE
    ) AS rating,

    -- rating_count: 24,871 → 24871
    CAST(
        REGEXP_REPLACE(rating_count, '[^0-9]', '')
        AS INT
    ) AS rating_count

FROM myDataSource;
'''
SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")

# Script generated for node Drop Null Fields
DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

# Script generated for node Silver layer data sink S3
EvaluateDataQuality().process_rows(frame=DropNullFields_node1772431857999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
SilverlayerdatasinkS3_node1772432020219.writeFrame(DropNullFields_node1772431857999)
job.commit()
```

### CSV CORRUPTION CHECK (DEEP EXPLANATION)
- ```spark.read.text(...).collect()```
    - Reads raw file line-by-line
    - Returns list to driver
- Why check < 2 rows?
    - ```if len(raw_lines) < 2:```
    - Means:
        - 1 row → only header
        - 0 row → empty file
    - Hence invalid dataset
- ```count_csv_columns()```
    - ```csv.reader(io.StringIO(line))```
    - Why?
        - Handles quoted commas correctly
    - Example : ```"a,b",c```
    - correctly counted as 2 columns.
- Row validation logic
    - ```for i, row in enumerate(raw_lines[1:], start=2):```
    - Starts from row 2 because: row1= header
    - Then : ```actual_col_count != expected_col_count```
    - If mismatch then corrupted row
- Error reporting
    - ```line[:80]```
    - Show preview only (Safe logging)
- Final check
    - ```if bad_row_indices:```
    - If any corrupted now --> fail job

### HOW EVENTBRIDGE + STEP FUNCTION WORKS
#### Step 1 : File arrives
- EventBridge is enabled on bucket
- S3 sends events to EventBridge automatically.

#### Step 2 : EventBridge rule triggers
Event pattern (Filter)

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["aws-glue-s3-bucket-one"]
    },
    "object": {
      "key": [{
        "prefix": "raw_data/sales_data/"
      }]
    }
  }
}
```

Matches:
- PutObject
- Multipart upload complete

The code below is the rules defined in pyspark script in AWS glue catalog 
```python
# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """Rules = [
    ColumnCount > 0,

    IsComplete "rating",
    IsComplete "rating_count",
    IsComplete "discounted_price",
    IsComplete "actual_price",

    ColumnValues "rating" >= 0,
    ColumnValues "rating_count" >= 0,
    ColumnValues "discounted_price" >= 0,
    ColumnValues "actual_price" >= 0,
    ColumnValues "discount_percentage" >= 0,
    
    IsUnique "product_id"
]"""
```
- This is an AWS Glue data quality ruleset.
- This tells glue 
    - Check if my dataset is structurally valid before writing.
- What it does ? 
    - runs all rules
    - measures pass/fail
    - logs results
    - publishes metrics
    - (optionally) fails job
- ```ColumnCount = 16,```
    - In my case the csv files had 16 columns so I have made sure that the csv that is about to be processed must have 16 columns exactly.
    - This prevents data from silent schema drift in case the upstream service/application removes a column in the csv files.
    - Cannot allow schema drift at all cost because it will break all the down stream applications.
- ```python
        ColumnValues "rating" >= 0,
        ColumnValues "rating_count" >= 0,
        ColumnValues "discounted_price" >= 0,
        ColumnValues "actual_price" >= 0,
        ColumnValues "discount_percentage" >= 0,
    ```
    - It makes sure that ```rating, rating_count, discounted_price, actual_price, discount_percentage``` columns does not have any negative data values in them.
- ```IsUnique "product_id"```
    - This makes sure that the product_id is unique and prevents data duplication
- I have set AWS glue ETL pipeline to have maximum concurrency of 10 
![setting_up_max_concurrency](images/production_grade_glue_version2_dlq_/glue_ETL/setting_up_max_concurrency.png)

- **Issue I faced :**
    - Data quality related validation issue
        - I accidently used this rule ```ColumnCount == 16,``` instead of this ```ColumnCount = 16,``` 
        - Because of this my AWS glue ETL pipeline was failing which is to be expected 
        - **The bigger concern for me was this that it was not sending message to DLQ as it should.**
        - Reason for this happening:
            - The step function checks for the final job state and not the logs that is the reason the flow in the step function never reaches to the point where it sends the message to DLQ hence no message is found in the DLQ even when the ETL job fails.
        - Solution 
            - All you have to do is wrap the code where you are evaluating the data quality rules in try except. This should send message to DLQ in case the ETL pipeline fails.
            - In order to test it I am deliberately keeping the wrong rule in the section where I have defined all of my data quality rules ```ColumnCount == 16,```. The correct code looke like this ```ColumnCount == 16,```.

#### Step 3 : Filter applied
The filter set in the Event pattern 
```bash
"prefix": "raw_data/sales_data/"
```

Only those files trigger the Step function.

#### Step 4 : Step Function starts
EventBridge sends full event payload:
```bash
detail.bucket.name
detail.object.key
```

#### Step 5 : Step Function → Glue
Passes params via:
```bash
--source_bucket
--source_key
```

#### Architecture 
Current : ```S3 → EventBridge → StepFunction → Glue → Silver S3```

### Problems with this architecture 
#### max concurrency error thrown by the ETL pipeline the moment I try to ingest more than 1 file 
- This problem happens when you go with this architecture : ```S3 → EventBridge → StepFunction → Glue → Silver S3```
- If you set the max concurrency to 10 of an AWS glue ETL then you may face the issue related to **ETL bookmark conflict**.
    - Explaination
        - 2 files arrive
        - 2 StepFn executions start
        - 2 Glue jobs run simultaneously
        - Bookmark system = SHARED STATE
        - Long story short you CANNOT run parallel Glue jobs with bookmarks enabled.
    - Solution I have to disable the bookmark for glue ETL pipeline. 
        - Reason : 
            - My event passes the which bucket recieved what file to state function which then passes that information to Glue ETL pipeline so my pipeline knows which file to ingest and from where hence bookmark is not needed here 
            - It was required when instead of implementing file level scanning I would have implemented folder level scanning then it would have been necessary 
- AWS glue ETL hitting its max concurrency limit and throwing errors
    - I have set the max concurrency of this ETL pipeline to be 10. This means the moment the number of files recieved in S3 exceeds 10 AWS glue ETL will throw errors in case of the current architecture ```S3 → EventBridge → StepFunction → Glue → Silver S3```
    - Reason:
        - 40 files arrives in S3 bucket
        - All trigger the step function and the step function trigger AWS glue ETL parallely but since I have set max parallel process to be 10 it will not allow more than that hence the error.
        - Hence we can say that this is a throttling control problem.
    - Solution : 
        - We have to improve our architecture and use some kind of queue to store all the events coming in hot right from the source S3 bucket
        - The missing piece in my current architecture was Buffering + Controlled Dispatch
        - Correction:
            - During my reseach I found that Step function can only be triggered only by
                - EventBridge
                - API
                - SDK
                - Lambda
            - Which means I cannot use SQS directly to trigger my step function which is a bummer but I can use Lamdba function to trigger my step function and I know that SQS can trigger Lambda function directly. 
            - So I have to make changes in the architecture accordingly I have to put a lambda function between SQS and Step function ```S3 → EventBridge → SQS BUFFER → Lambda (trigger) → StepFn → Glue → Silver S3```

#### **Implementation : (Version 4)**
- As discussed above I am going to implement this architecture ---> ```S3 → EventBridge → SQS BUFFER → Lambda (trigger) → StepFn → Glue → Silver S3```
- **First configure you source S3 bucket to use event bridge:**
    - ![send_events_to_event_bridge_s3](images/production_grade_glue_version3_handled_concurrency/s3/send_events_to_event_bridge_s3.png)
    - Set the source s3 in a way that it sends the file arrival event message straight to eventBridge.
- **Setup the EventBridge:**
    - ![event_bridge_diagram](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram.png)
    - ![event_bridge_diagram_2](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram_2.png)
    - ![event_bridge_diagram_2_permissions](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram_2_permissions.png)
    - ![event_bridge_diagram_2_DLQ](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram_2_DLQ.png)
    - Here is the event pattern filter.
        - ```json
            {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {
                "name": ["aws-glue-s3-bucket-one"]
                },
                "object": {
                "key": [{
                    "prefix": "raw_data/sales_data/"
                }]
                }
            }
            }
            ```
    - Set the event bridge in a way that it recieves the event message of file arrival directly from S3 bucket and then send that event message to SQS queue named ```FileProcessingQueue.fifo```
- **SQS for handling file arrival event message from S3 setup:**
    - **STEP 1:**
        - ![FileProcessingQueue.fifo](images/production_grade_glue_version3_handled_concurrency/sqs/FileProcessingQueue.fifo.png)
        - Set the default visibility time of messages in this ```FileProcessingQueue.fifo``` queue to be 20 minutes. 
        - Don't forget to setup FIFO in this queue if you want your messages to execute in an orderly fashon
        - This is the access policy that I used for this queue 
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                "Sid": "AllowEventBridgeToSend",
                "Effect": "Allow",
                "Principal": {
                    "Service": "events.amazonaws.com"
                },
                "Action": "sqs:SendMessage",
                "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue",
                "Condition": {
                    "ArnEquals": {
                    "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                    }
                }
                }
            ]
        }
        ```
- **Setup a Lambda function**
    - This lambda function will be triggered via SQS named ```FileProcessingQueue.fifo``` and then this lambda function in return will trigger a state function called ```orchestrate_data_ingestion```
    - Why this is needed because SQS cannot directly trigger Step function it can only trigger a lambda function 
    - Here is the lambda function code that I am using 
    ```python
    import json
    import boto3
    import uuid
    from datetime import datetime, timezone, timedelta

    sf = boto3.client("stepfunctions")

    STATE_MACHINE_ARN = "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"

    def lambda_handler(event, context):

        execution_id = str(uuid.uuid4())[:8]

        IST = timezone(timedelta(hours=5, minutes=30))
        timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")

        print(f"\n===== Lambda Invocation START =====")
        print(f"ExecutionID: {execution_id}")
        print(f"Timestamp: {timestamp}")
        print("Full Event From SQS:", json.dumps(event))
        print("===================================\n")

        files = []

        for record in event["Records"]:
            body = json.loads(record["body"])

            files.append({
                "bucket": body["detail"]["bucket"]["name"],
                "key": body["detail"]["object"]["key"]
            })

        print("Files collected:", files)

        sf.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps({"files": files})
        )

        return {"status": "ok"}
    ```
    - Here is the lambda function trigger configuration
    ![lambda_function_trigger_conf](images/production_grade_glue_version3_handled_concurrency/lambda_function/lambda_function_trigger_conf.png)
    - When configuring batch size to 5 and set up max concurrency for lambda function to 2 
    - In order for your lambda function to work properly you need to create a new role that has permission like this 
    ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "states:StartExecution"
                    ],
                    "Resource": "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes"
                    ],
                    "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue.fifo"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "*"
                }
            ]
        }
    ```
- **Setup AWS Step function (Orchestrate AWS ETL pipeline)**
    - ```orchestrate_data_ingestion``` step function will trigger the AWS glue ETL pipeline
    - ```json
        {
        "Comment": "Glue ETL Orchestration with Controlled Concurrency",
        "StartAt": "ProcessFiles",
        "States": {
            "ProcessFiles": {
            "Type": "Map",
            "ItemsPath": "$.files",
            "MaxConcurrency": 1,
            "Iterator": {
                "StartAt": "RunGlueJob",
                "States": {
                "RunGlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                    "JobName": "ingest_sales_data",
                    "Arguments": {
                        "--source_bucket.$": "$.bucket",
                        "--source_key.$": "$.key"
                    }
                    },
                    "Retry": [
                    {
                        "ErrorEquals": [
                        "Glue.ConcurrentRunsExceededException"
                        ],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 10,
                        "BackoffRate": 1.5
                    },
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 2,
                        "BackoffRate": 2
                    }
                    ],
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "Next": "SendToDLQ"
                    }
                    ],
                    "End": true
                },
                "SendToDLQ": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                    "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                    "MessageBody.$": "$"
                    },
                    "End": true
                }
                }
            },
            "End": true
            }
        }
        }
      ```
- **Setup DLQ**
    - This DLQ will be used by the step function that is responsible for replaying ETL pipeline.
    - This DLQ saves the event message sent from source s3 in the event when the ETL fails to ingest a particular file.
    - ![DLQ](images/production_grade_glue_version3_handled_concurrency/sqs/DLQ.png)
    - This is the sqs policy I used 
    ```json
    {
        "Version": "2012-10-17",
        "Id": "__default_policy_ID",
        "Statement": [
            {
            "Sid": "__owner_statement",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::406868976171:root"
            },
            "Action": "SQS:*",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq"
            },
            {
            "Sid": "AllowEventBridgeToSendMessage",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            },
            {
            "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            }
        ]
    }
    ```
- **Setup AWS Step function (Replay AWS ETL pipeline)**
    - ```replay_failed_ingestion``` This step function will allow us to re-invoke the ETL pipeline manually in the event where our pipeline failed and even retry failed.
    - SOP is that :
        - suppose you recieve a corrupted csv file 
        - you pipeline retries 3 times and then fails 
        - you get a mail telling you that the ETL failed to ingest this file along with the file name 
        - The event message from S3 gets stored in the DLQ
        - after fixing the issue I can simple use this state function to manualy run the ETL pipeline 
        - since this state function reads the event message from the DLQ the ETL knows which file to reprocess.
        - This is why this replay step function is essential.
    - ```json
        {
        "Comment": "Replay ETL from DLQ (Fixed Version)",
        "StartAt": "ReceiveFromDLQ",
        "States": {
            "ReceiveFromDLQ": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
            "Parameters": {
                "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                "MaxNumberOfMessages": 10
            },
            "ResultPath": "$.dlq",
            "Next": "CheckIfEmpty"
            },
            "CheckIfEmpty": {
            "Type": "Choice",
            "Choices": [
                {
                "Variable": "$.dlq.Messages",
                "IsPresent": false,
                "Next": "Success"
                }
            ],
            "Default": "ReplayBatch"
            },
            "ReplayBatch": {
            "Type": "Map",
            "ItemsPath": "$.dlq.Messages",
            "MaxConcurrency": 1,
            "Iterator": {
                "StartAt": "ExtractPayload",
                "States": {
                "ExtractPayload": {
                    "Type": "Pass",
                    "Parameters": {
                    "receiptHandle.$": "$.ReceiptHandle",
                    "body.$": "States.StringToJson($.Body)"
                    },
                    "ResultPath": "$.parsed",
                    "Next": "ParseCause"
                },
                "ParseCause": {
                    "Type": "Pass",
                    "Parameters": {
                    "receiptHandle.$": "$.parsed.receiptHandle",
                    "cause.$": "States.StringToJson($.parsed.body.Cause)"
                    },
                    "ResultPath": "$.payload",
                    "Next": "RunGlueJob"
                },
                "RunGlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "ResultPath": "$.glueResult",
                    "Parameters": {
                    "JobName": "ingest_sales_data",
                    "Arguments": {
                        "--source_bucket.$": "$.payload.cause.Arguments['--source_bucket']",
                        "--source_key.$": "$.payload.cause.Arguments['--source_key']"
                    }
                    },
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "Next": "FailState"
                    }
                    ],
                    "Next": "DeleteMessage"
                },
                "DeleteMessage": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
                    "Parameters": {
                    "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                    "ReceiptHandle.$": "$.payload.receiptHandle"
                    },
                    "Next": "SuccessState"
                },
                "FailState": {
                    "Type": "Fail"
                },
                "SuccessState": {
                    "Type": "Succeed"
                }
                }
            },
            "Next": "Success"
            },
            "Success": {
            "Type": "Succeed"
            }
        }
        }
      ```
      
- **Setup AWS glue ETL pipeline:**
    - Here is the python code for the ETL pipeline 
    ```python
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.gluetypes import *
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue import DynamicFrame

    from functools import reduce
    from pyspark.sql import functions as F

    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, ArrayType):
            if isinstance(schema.elementType, StructType):
                output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, NullType):
            output.append(path)
        else:
            x, distinct_set = frame.toDF(), set()
            for i in x.select(path).distinct().collect():
                distinct_ = i[path.split('.')[-1]]
                if isinstance(distinct_, list):
                    distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
                elif isinstance(distinct_, str) :
                    distinct_set.add(distinct_.strip())
                else:
                    distinct_set.add(distinct_)
            if isinstance(schema, StringType):
                if distinct_set.issubset(nullStringSet):
                    output.append(path)
            elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
                if distinct_set.issubset(nullIntegerSet):
                    output.append(path)
        return output

    def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
        nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
        return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'source_key'
    ])

    source_bucket = args['source_bucket']
    source_key = args['source_key']

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Default ruleset used by all target nodes with data quality enabled
    DEFAULT_DATA_QUALITY_RULESET = """Rules = [
        ColumnCount = 16,

        IsComplete "rating",
        IsComplete "rating_count",
        IsComplete "discounted_price",
        IsComplete "actual_price",

        ColumnValues "rating" >= 0,
        ColumnValues "rating_count" >= 0,
        ColumnValues "discounted_price" >= 0,
        ColumnValues "actual_price" >= 0,
        ColumnValues "discount_percentage" >= 0,
        
        IsUnique "product_id"
    ]"""

    # -------- DELIMITER CORRUPTION CHECK -------- #
    # It checks for delimiter / structural corruption STARTS
    import csv
    import io

    raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").collect()

    if len(raw_lines) < 2:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (file has no data rows)")

    def count_csv_columns(line: str) -> int:
        """Count columns correctly, respecting quoted fields containing commas."""
        try:
            return len(next(csv.reader(io.StringIO(line))))
        except StopIteration:
            return 0

    header_line = raw_lines[0][0]
    expected_col_count = count_csv_columns(header_line)

    bad_row_indices = []
    for i, row in enumerate(raw_lines[1:], start=2):
        line = row[0]
        actual_col_count = count_csv_columns(line)
        if actual_col_count != expected_col_count:
            bad_row_indices.append((i, actual_col_count, line[:80]))

    if bad_row_indices:
        details = "\n".join(
            [f"  Line {idx}: expected {expected_col_count} cols, got {actual} → {preview}..."
            for idx, actual, preview in bad_row_indices]
        )
        raise Exception(
            f"Glue Visual ETL | Corrupted CSV detected — {len(bad_row_indices)} row(s) have wrong column count "
            f"(likely semicolons used as delimiters instead of commas):\n{details}"
        )

    print(f"Glue Visual ETL | Column count validation passed: all rows have {expected_col_count} columns")
    # It checks for delimiter / structural corruption ENDS
    # -------- DELIMITER CORRUPTION CHECK -------- #

    # Script generated for node Raw data source S3
    RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE", "optimizePerformance": False}, connection_type="s3", format="csv", 
        # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
        connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
        transformation_ctx="RawdatasourceS3_node1772431168408")
    # for debugging only
    print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

    df = RawdatasourceS3_node1772431168408.toDF()

    print("Glue Visual ETL | DEBUG bucket:", source_bucket)
    print("Glue Visual ETL | DEBUG key:", source_key)
    print("Glue Visual ETL | DEBUG columns:", df.columns)
    print("Glue Visual ETL | DEBUG row count:", df.count())
    print("Glue Visual ETL | DEBUG schema:", df.schema)
    print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())

    # -------- CORRUPTION CHECK -------- #
    # checks Spark-level corruption after parsing STARTS

    # If Spark inferred no columns → corrupted file
    if len(df.columns) == 0:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (no columns inferred)")

    expected_cols = len(df.columns)

    # If dataframe empty → corrupted file
    if df.limit(1).count() == 0:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (empty dataframe)")

    # Row-level corruption detection
    null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]

    bad_rows = df.filter(
        reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7
    )

    if bad_rows.limit(1).count() > 0:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (null-heavy rows)")

    # checks Spark-level corruption after parsing ENDS
    # -------- CORRUPTION CHECK -------- #

    #--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#
    # Script generated for node SQL Query
    SqlQuery61 = '''
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
    '''

    try:
        SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")
    except Exception as e:
        print(f"Glue Visual ETL | SQL query execution failed | {e}")
        raise RuntimeError("Failing Glue job explicitly | Failed to execute sql query")
    #--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#

    # Script generated for node Drop Null Fields
    DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

    # -------- DEDUPLICATION STEP -------- #
    # This will check if there exists a row with a duplicate review_id if yes then it will drop one of the row and make sure that the row is unique
    # Doing this prevents any data duplication 
    df_clean = DropNullFields_node1772431857999.toDF()

    deduped_df = df_clean.dropDuplicates()

    print("Glue Visual ETL | Before dedup:", df_clean.count())
    print("Glue Visual ETL | After dedup:", deduped_df.count())

    deduped_dynamic_frame = DynamicFrame.fromDF(
        deduped_df, glueContext, "deduped_dynamic_frame"
    )
    # -------- DEDUPLICATION STEP -------- #

    # Script generated for node Silver layer data sink S3
    try:
        EvaluateDataQuality().process_rows(frame=deduped_dynamic_frame, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
    except Exception as e:
        print("Glue Visual ETL | DQ failed:", e)
        raise RuntimeError("Failing Glue job explicitly | Failed to run data quality rules")

    SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
    SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
    SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
    SilverlayerdatasinkS3_node1772432020219.writeFrame(deduped_dynamic_frame)
    job.commit()
    ```
    - Here is the configuration related to AWS glue ETL pipeline 
    - ![ETL_conf_1](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_1.png)
    - ![ETL_conf_2](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_2.png)
    - ![ETL_conf_3](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_3.png)
    - ![ETL_conf_4](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_4.png)
    - This ETL pipeline will recieve the params from lambda function using which it will know what files to process from the S3 bucket.
    - One thing to note make sure that you set max concurrency to 2 for glue ETL pipeline 
    - This is done to save cost 
    - This ETL pipeline will also prevent duplicated data in the table from ingestion.
        - ```python
            # -------- DEDUPLICATION STEP -------- #
            # This will check if there exists a row with a duplicate review_id if yes then it will drop one of the row and make sure that the row is unique
            # Doing this prevents any data duplication 
            df_clean = DropNullFields_node1772431857999.toDF()

            deduped_df = df_clean.dropDuplicates(["review_id"])

            print("Glue Visual ETL | Before dedup:", df_clean.count())
            print("Glue Visual ETL | After dedup:", deduped_df.count())

            deduped_dynamic_frame = DynamicFrame.fromDF(
                deduped_df, glueContext, "deduped_dynamic_frame"
            )
            # -------- DEDUPLICATION STEP -------- #
            ```
            - This code checks for duplicate ```review_id``` in the rows and will only allow one of them to pass so that it can be ingested.
### There are still gaps in this Implementation : (Version 4)
#### No true idempotency 
- In current implementation I will get duplicates
- Meaning the same file can be processed multiple times
- Real problem is :
    - If glue has to run twice to process the same file that has already been ingested then its not only a waste of compute but also waste of money in AWS
    - Step function runs twice again its a waste of compute and money specially when the file has already been ingested.
    - DLQ noise increases
    - Observability becomes messy
#### Solution : 
- Create a file processing repository (DynamoDB)
- Keys:
    - ```bash 
        PK: file_key (S3 object key)
    ```
- Attributes:
    - ```bash
        status: IN_PROGRESS | SUCCESS | FAILED
        row_count
        error_message
        event_payload
        updated_at
      ```

### NOTE : Common Data Quality rules in AWS Visual ETL pipeline
Common DQ rules
You can use:
- IsComplete
- IsUnique
- RowCount
- ColumnCount
- ColumnValues BETWEEN
- ColumnLength
- MatchesRegex
- ReferentialIntegrity

## Implementation : (Version 5)
- In this phase of implementation I am going to implement dynamo db to keep track of files that have been ingested successfully or failed or in progress

### Theory : Implementation (Version 5)
#### DynamoDB vs RDS
-  **DynamoDB (NoSQL, key-value)**
    - Schema: flexible (no rigid schema)
    - Access pattern: key-based lookups (O(1))
    - Scaling: automatic, virtually unlimited
    - Concurrency: built-in atomic conditional writes
    - Latency: single-digit milliseconds
    - Operations: simple (get/put/update)
    - Cost model: per request
    - atomic conditional writes at scale
- **RDS (Relational DB)**
    - Schema: strict (tables, joins, constraints)
    - Access pattern: complex queries, joins
    - Scaling: vertical (or complex horizontal)
    - Concurrency: transactional (ACID), but heavier
    - Latency: higher than DynamoDB
    - Operations: SQL, joins, aggregations
    - Cost model: instance-based (always on)
### Proposed architecture 
```bash
                ┌─────────────────────────┐
                │   S3 Source Bucket      │
                │   (CSV Files)           │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │      EventBridge        │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │   SQS BUFFER (FIFO)     │
                │ FileProcessingQueue     │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │        Lambda           │
                │ (Trigger StepFn)        │
                └────────────┬────────────┘
                             │
                ┌────────────▼────────────┐
                │     DynamoDB (NEW)      │
                │  File Processing Table  │
                │-------------------------│
                │ PK: file_key            │
                │ status                  │
                │ row_count               │
                │ error_message           │
                │ event_payload           │
                │ retry_count             │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │      Step Function      │
                │ orchestrate_ingestion   │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │        Glue ETL         │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │     Silver S3 Layer     │
                └─────────────────────────┘
```

## Implementation phase for version 5
#### S3 bucket related settings 
- Set the setting related to sending the events to the event bridge in this source S3 bucket like as shown below 
![source_bucket](images/production_grade_glue_version5_dynamo_db/S3/source_bucket.png)
    
#### SQS (sales-ingestion-dlq)
- Access policy for this sqs ```sales-ingestion-dlq```
    - ```json
        {
        "Version": "2012-10-17",
        "Id": "__default_policy_ID",
        "Statement": [
            {
            "Sid": "__owner_statement",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::406868976171:root"
            },
            "Action": "SQS:*",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq"
            },
            {
            "Sid": "AllowEventBridgeToSendMessage",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            },
            {
            "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            }
        ]
        }
      ```
- ![sales-ingestion-dlq_1](images/production_grade_glue_version5_dynamo_db/SQS/sales-ingestion-dlq_1.png)
- ![sales-ingestion-dlq_2](images/production_grade_glue_version5_dynamo_db/SQS/sales-ingestion-dlq_2.png)
- ![sales-ingestion-dlq_3](images/production_grade_glue_version5_dynamo_db/SQS/sales-ingestion-dlq_3.png)
- This sqs queue stores the event messages in a queue in the event of ETL pipeline failure so that we can use ```replay_failed_ingestion``` step function to manually replay all the events and ingest the data from the files that have failed to be ingested due to errors after resolving the cause for error.

#### Step function (replay_failed_ingestion)
- Attach this permissions to IAM role 
    - ![replay_failed_ingestion_2](images/production_grade_glue_version5_dynamo_db/step_functions/replay_failed_ingestion_2.png)
    - Let AWS create IAM role for your step functions and then add more permissions to this IAM role as needed by your use case.
    - Attach one AWS managed policy called ```AmazonSQSFullAccess```
    - Attach one custom policy for dynamoDB related permissions named ```DynamoDBStepFunctionAccessPolicy```
        - ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DynamoDBStepFunctionAccessPolicy",
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:UpdateItem",
                            "dynamodb:GetItem"
                        ],
                        "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                    }
                ]
            }
          ```
```json
{
  "Comment": "Replay ETL from DLQ (Fixed - State Preservation + Safe Errors)",
  "StartAt": "ReceiveFromDLQ",
  "States": {
    "ReceiveFromDLQ": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
      "Parameters": {
        "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
        "MaxNumberOfMessages": 10
      },
      "ResultPath": "$.dlq",
      "Next": "CheckIfEmpty"
    },
    "CheckIfEmpty": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.dlq.Messages",
          "IsPresent": false,
          "Next": "Success"
        }
      ],
      "Default": "ReplayBatch"
    },
    "ReplayBatch": {
      "Type": "Map",
      "ItemsPath": "$.dlq.Messages",
      "MaxConcurrency": 2,
      "Iterator": {
        "StartAt": "ExtractPayload",
        "States": {
          "ExtractPayload": {
            "Type": "Pass",
            "Parameters": {
              "receiptHandle.$": "$.ReceiptHandle",
              "body.$": "States.StringToJson($.Body)"
            },
            "ResultPath": "$.parsed",
            "Next": "CheckRetry"
          },
          "CheckRetry": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:getItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              }
            },
            "ResultPath": "$.ddb",
            "Next": "EvaluateRetry"
          },
          "EvaluateRetry": {
            "Type": "Choice",
            "Choices": [
              {
                "Variable": "$.ddb.Item.retry_count.N",
                "NumericLessThan": 3,
                "Next": "IncrementRetry"
              }
            ],
            "Default": "SkipReplay"
          },
          "IncrementRetry": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              },
              "UpdateExpression": "ADD retry_count :inc",
              "ExpressionAttributeValues": {
                ":inc": {
                  "N": "1"
                }
              }
            },
            "Next": "RunGlueJob"
          },
          "RunGlueJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "ResultPath": "$.glueResult",
            "Parameters": {
              "JobName": "ingest_sales_data",
              "Arguments": {
                "--source_bucket.$": "$.parsed.body.bucket",
                "--source_key.$": "$.parsed.body.key"
              }
            },
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "ResultPath": "$.error",
                "Next": "UpdateFailure"
              }
            ],
            "Next": "UpdateSuccess"
          },
          "UpdateSuccess": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "SUCCESS"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "Next": "DeleteMessage"
          },
          "UpdateFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "FAILED"
                },
                ":e": {
                  "S.$": "States.JsonToString($.error)"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "Next": "DeleteMessage"
          },
          "SkipReplay": {
            "Type": "Pass",
            "Next": "DeleteMessage"
          },
          "DeleteMessage": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
            "Parameters": {
              "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
              "ReceiptHandle.$": "$.parsed.receiptHandle"
            },
            "End": true
          }
        }
      },
      "Next": "Success"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
```
- ![replay_failed_ingestion_flow](images/production_grade_glue_version5_dynamo_db/step_functions/replay_failed_ingestion_flow.svg)
- **ReceiveFromDLQ** — The Step Function opens the sales-ingestion-dlq and pulls up to 10 messages at once. These are messages that were written there by orchestrate_data_ingestion when Glue previously failed.
- **CheckIfEmpty** — Checks if the Messages key is present in the SQS response. If the queue was empty, SQS returns no Messages key at all (not an empty array — it literally omits the key). The Choice state uses IsPresent: false to detect this and short-circuits straight to Success — nothing to do.
- **Map (ReplayBatch)** — For each message in the batch, it spawns a parallel iterator with MaxConcurrency: 2, meaning it processes at most 2 failed files at the same time.
- **ExtractPayload** — A Pass state that uses States.StringToJson to parse the raw SQS message body (which is a string) into a usable JSON object. It also pulls out the ReceiptHandle, which is the SQS token you need later to delete the message.
- **CheckRetry** — Looks up the file's record in file_processing_registry DynamoDB using file_key as the key. It retrieves the current retry_count.
- **EvaluateRetry** — Checks if retry_count < 3. This is your poison message guard. If a file has already been retried 3 or more times and keeps failing, it's considered a poison message — likely corrupt data or a permanent schema issue. You don't want to keep hammering Glue with it. So retry_count >= 3 routes to SkipReplay.
- **IncrementRetry** — Before attempting Glue, it immediately increments retry_count by 1 in DynamoDB. This is important — it increments before running Glue, not after. That way if Glue hangs, crashes mid-run, or the Step Function itself dies, the counter is already updated. You won't accidentally lose track of how many times you've tried.
- **RunGlueJob** — Starts the actual Glue ETL job with .sync, meaning it waits for Glue to fully finish before moving on. It passes source_bucket and source_key extracted from the DLQ message body.
- **UpdateSuccess / UpdateFailure** — Depending on whether Glue succeeded or failed, it writes the result back to DynamoDB. On success it sets status = SUCCESS. On failure it sets status = FAILED and stores the error string from $.error via States.JsonToString.
- **SkipReplay** — Just a Pass state that does nothing. The poison message flows straight through to DeleteMessage.
- **DeleteMessage** — This always runs, regardless of success, failure, or skip. It deletes the message from the DLQ using the ReceiptHandle. This is correct behaviour — even if Glue failed again, you still remove it from the DLQ because you've already recorded the failure in DynamoDB. If you left it in the DLQ, the next replay run would just pick it up again infinitely.

#### SQS (FileProcessingQueue.fifo)
- ```json
    {
    "Version": "2012-10-17",
    "Statement": [
        {
        "Sid": "AllowEventBridgeToSend",
        "Effect": "Allow",
        "Principal": {
            "Service": "events.amazonaws.com"
        },
        "Action": "sqs:SendMessage",
        "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue",
        "Condition": {
            "ArnEquals": {
            "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
            }
        }
        }
    ]
    }
  ```
- ![FileProcessingQueue.fifo_1](images/production_grade_glue_version5_dynamo_db/SQS/FileProcessingQueue.fifo_1.png)
- ![FileProcessingQueue.fifo_2](images/production_grade_glue_version5_dynamo_db/SQS/FileProcessingQueue.fifo_2.png)
- ![FileProcessingQueue.fifo_3](images/production_grade_glue_version5_dynamo_db/SQS/FileProcessingQueue.fifo_3.png)
- This is another SQS queue that is used in this architecture so as to tackle the backpressure 
- Backpressure is what happens when a downstream system cannot consume data as fast as an upstream system is producing it, and that slowness propagates back upstream to slow down or pause the producer.
- The word comes from fluid dynamics — imagine water flowing through pipes of different diameters. If a wide pipe feeds into a narrow pipe, water pressure builds up at the junction. The narrow pipe is creating backpressure against the flow. The same concept applies to data systems.
- A concrete example from my own pipeline makes this clearest. Imagine your S3 bucket starts receiving 500 CSV files per minute from an upstream application. Your Glue ETL job can only process 2 files concurrently due to your MaxConcurrency: 2 setting. Without any buffering, those 500 events would all hit your Step Function simultaneously, Glue would reject most of them with ConcurrentRunsExceededException, and you'd have a cascade of failures.
- This is exactly why I added SQS between EventBridge and Lambda. SQS is acting as your backpressure mechanism. The 500 events land safely in the queue, and Lambda only pulls from it at a controlled rate using your batch size of 5 and max concurrency of 2. The queue absorbs the burst — it holds the excess load and releases it at the pace the downstream system can handle. The upstream system (S3/EventBridge) never needs to know that Glue is slow.
- Without backpressure handling, systems fail in two common ways. The first is the fast producer overwhelming the slow consumer — the consumer crashes under load or starts dropping data silently. The second is unbounded memory growth — if the consumer is slow and the producer keeps pushing, the buffer between them grows until the system runs out of memory.
- In data engineering specifically you see backpressure handled in a few different ways. Queues like SQS, Kafka, and RabbitMQ are the most common approach — they decouple the producer and consumer so each can run at its own pace. Rate limiting at the producer side is another approach, where the producer is told to slow down explicitly. Streaming frameworks like Apache Flink and Spark Structured Streaming have backpressure built in natively — if a downstream operator is slow, Flink automatically reduces the rate at which the source reads new records.
#### Create an IAM role for lambda function (TriggerStepFunctionRoleForLambdaFunc)
- Create an IAM role named ```TriggerStepFunctionRoleForLambdaFunc``` for lambda function with appropriate permissions (Policies)
- Permissions added to this role is 
    - ```AllowLambdaToAccessSQSAndStepFunction```
        - ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "states:StartExecution"
                        ],
                        "Resource": "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes"
                        ],
                        "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue.fifo"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        "Resource": "*"
                    }
                ]
            }
          ```
    - ```DynamoDbAccessPolicyForLambdaFunction```
        - ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DynamoDbAccessPolicyForLambdaFunction",
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:PutItem",
                            "dynamodb:GetItem",
                            "dynamodb:UpdateItem"
                        ],
                        "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                    }
                ]
            }
          ```

#### Lambda function (StartStateFunction)
- ```python
    import json
    import boto3
    import uuid
    from datetime import datetime, timezone, timedelta

    sf = boto3.client("stepfunctions")
    dynamodb = boto3.client("dynamodb")

    STATE_MACHINE_ARN = "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"
    TABLE_NAME = "file_processing_registry"

    def lambda_handler(event, context):

        execution_id = str(uuid.uuid4())[:8]

        IST = timezone(timedelta(hours=5, minutes=30))
        timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")

        print(f"\n===== Lambda Invocation START =====")
        print(f"ExecutionID: {execution_id}")
        print(f"Timestamp: {timestamp}")
        print("Full Event From SQS:", json.dumps(event))
        print("===================================\n")

        files = []

        for record in event["Records"]:
            body = json.loads(record["body"])

            bucket = body["detail"]["bucket"]["name"]
            key = body["detail"]["object"]["key"]

            file_key = f"s3://{bucket}/{key}"

            try:
                # Idempotency check (CRITICAL)
                dynamodb.put_item(
                    TableName=TABLE_NAME,
                    Item={
                        "file_key": {"S": file_key},
                        "status": {"S": "IN_PROGRESS"},
                        "retry_count": {"N": "0"},
                        "created_at": {"S": timestamp}
                    },
                    ConditionExpression="attribute_not_exists(file_key)"
                )

                # Only add if it's NOT duplicate
                files.append({
                    "bucket": bucket,
                    "key": key,
                    "file_key": file_key
                })

                print(f"Accepted file: {file_key}")

            except dynamodb.exceptions.ConditionalCheckFailedException:
                print(f"Duplicate skipped: {file_key}")

        print("Files to process:", files)

        # Only trigger StepFn if there are valid files
        if files:
            sf.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                input=json.dumps({"files": files})
            )
            print("Step Function triggered")
        else:
            print("No new files to process")

        return {"status": "ok"}
    ```
- ![StartStateFunction_1](images/production_grade_glue_version5_dynamo_db/lambda_function/StartStateFunction_1.png)
- ![lambda_startstatefn_flow](images/production_grade_glue_version5_dynamo_db/lambda_function/lambda_startstatefn_flow.svg)
- What triggers it?
    - SQS triggers this Lambda automatically whenever messages arrive in FileProcessingQueue.fifo. Each invocation receives a batch of up to 5 messages (your configured batch size), delivered as ```event["Records"]```.
- Section 1 : Setup and logging
    - ```python
        execution_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now(IST).strftime(...)
      ```
    - At the start, it generates a short random ```execution_id``` (first 8 chars of a UUID) and captures the current IST timestamp. These are purely for logging — they let you correlate CloudWatch log lines back to a specific Lambda invocation when debugging. Nothing is done with ```execution_id``` beyond printing it.
- Section 2 : Parse each SQS record
    - ```python
        for record in event["Records"]:
            body = json.loads(record["body"])
            bucket = body["detail"]["bucket"]["name"]
            key = body["detail"]["object"]["key"]
            file_key = f"s3://{bucket}/{key}"
      ```
    - Each SQS message body is a JSON string containing the original EventBridge event from S3. The Lambda parses it and extracts three things: the bucket name, the object key (file path inside the bucket), and then constructs ```file_key``` by combining them into a full S3 URI like ```s3://aws-glue-s3-bucket-one/raw_data/sales_data/sales_data_1.csv```. This file_key is what gets used as the primary key in DynamoDB.
- Section 3 : The idempotency check (the most important part)
    - ```python
      dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={ "file_key": ..., "status": "IN_PROGRESS", "retry_count": "0", ... },
            ConditionExpression="attribute_not_exists(file_key)"
        )
      ```
    - This is an atomic conditional write. It says: write this record to DynamoDB, but only if an item with this ```file_key``` does not already exist. DynamoDB evaluates the condition and the write in a single operation — there is no gap between checking and writing that another process could slip through.
    - If the file is new, the write succeeds, the record is created with ```status = IN_PROGRESS``` and ```retry_count = 0```, and the file gets added to the files list.
    - If the file was already processed (or is currently being processed), DynamoDB raises ```ConditionalCheckFailedException```. The ```except``` block catches it silently, logs ```"Duplicate skipped"```, and moves on. The file is never added to ```files```.
- Section 4 : Trigger the Step Function
    - ```python
        if files:
        sf.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps({"files": files})
        )
      ```
    - After processing all records in the batch, if there is at least one genuinely new file, it fires ```orchestrate_data_ingestion``` with the list of new files as input. Each file object in the list contains ```bucket```, ```key```, and ```file_key``` all three are needed downstream: ```bucket``` and ```key``` go to Glue, ```file_key``` goes to DynamoDB updates in the Step Function.
    - If ```files``` is empty (every record in the batch was a duplicate), the Step Function is never triggered at all. No compute wasted.
- The Lambda is intentionally doing only two things: duplicate filtering and Step Function invocation. It is not running Glue, not doing transformation, not writing success/failure status. It just decides whether a file deserves to enter the pipeline at all, creates the initial DynamoDB record as a lock, and hands off. Everything else happens inside the Step Function.
- For more information about how the lamdba function works refer to this video : https://youtu.be/XFGSuj83wdc

#### Step function (orchestrate_data_ingestion)
- Attach this permissions to IAM role 
    - ![orchestrate_data_ingestion_2](images/production_grade_glue_version5_dynamo_db/step_functions/orchestrate_data_ingestion_2.png)
    - Let AWS create IAM role for this step function and add more permissions to this role as per your needs 
    - Add permissions for dynamoDb named ```DynamoDBStepFunctionAccessPolicy```
        - ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DynamoDBStepFunctionAccessPolicy",
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:UpdateItem",
                            "dynamodb:GetItem"
                        ],
                        "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                    }
                ]
            }
          ```
```json
{
  "Comment": "Glue ETL Orchestration with DynamoDB Tracking",
  "StartAt": "ProcessFiles",
  "States": {
    "ProcessFiles": {
      "Type": "Map",
      "ItemsPath": "$.files",
      "MaxConcurrency": 2,
      "Iterator": {
        "StartAt": "RunGlueJob",
        "States": {
          "RunGlueJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "ingest_sales_data",
              "Arguments": {
                "--source_bucket.$": "$.bucket",
                "--source_key.$": "$.key"
              }
            },
            "ResultPath": "$.glueResult",
            "Retry": [
              {
                "ErrorEquals": [
                  "Glue.ConcurrentRunsExceededException"
                ],
                "IntervalSeconds": 60,
                "MaxAttempts": 10,
                "BackoffRate": 1.5
              },
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "IntervalSeconds": 30,
                "MaxAttempts": 2,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "ResultPath": "$.error",
                "Next": "UpdateFailure"
              }
            ],
            "Next": "UpdateSuccess"
          },
          "UpdateSuccess": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "SUCCESS"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "End": true
          },
          "UpdateFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "FAILED"
                },
                ":e": {
                  "S.$": "$.error.Cause"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "Next": "SendToDLQ"
          },
          "SendToDLQ": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sqs:sendMessage",
            "Parameters": {
              "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
              "MessageBody": {
                "bucket.$": "$.bucket",
                "key.$": "$.key",
                "file_key.$": "$.file_key"
              }
            },
            "End": true
          }
        }
      },
      "End": true
    }
  }
}
```
- ![version5_orchestration_step_function](images/production_grade_glue_version5_dynamo_db/step_functions/version5_orchestration_step_function.png)
- Three new states were inserted into this step function, and ```file_key``` was added to the Lambda payload 
- The first addition is UpdateSuccess. After Glue finishes successfully, instead of jumping straight to End, the Step Function now calls dynamodb:updateItem and sets status = SUCCESS with a timestamp. This is what closes the idempotency loop — Lambda wrote IN_PROGRESS, and now the Step Function writes SUCCESS. The next time the same file arrives, Lambda's put_item with attribute_not_exists(file_key) will see the existing record and throw ConditionalCheckFailedException, which is exactly what you want.
- The second addition is UpdateFailure. When Glue fails, before sending anything to the DLQ, the Step Function now writes status = FAILED and captures $.error.Cause as error_message in DynamoDB. This gives you a structured audit record you can query — you can run aws dynamodb scan --table-name file_processing_registry and immediately see which files failed and why, without digging through CloudWatch logs.
- The third addition is that SendToDLQ's MessageBody now explicitly includes file_key alongside bucket and key. This is what makes the replay_failed_ingestion Step Function work correctly — when it reads a message from the DLQ and needs to call dynamodb:updateItem, it now has the file_key to use as the partition key lookup.
- This version update brings 
    - ```Step Function = orchestrator + state updater```

#### AWS Glue ETL (ingest_sales_data)
- Create an IAM role called ```AWSGlueRole``` and give appropriate permissions so that AWS glue ETL can function properly
    - Add permissions 
        - Add an AWS managed permisson called ```AWSGlueServiceRole```
        - Add a custom permission named ```LimitedS3PermissionPolicy```
            - ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:ListBucket"
                            ],
                            "Resource": [
                                "arn:aws:s3:::aws-glue-s3-bucket-one",
                                "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                                "arn:aws:s3:::data-sink-one",
                                "arn:aws:s3:::data-sink-one/*"
                            ]
                        }
                    ]
                }
              ```
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

from functools import reduce
from pyspark.sql import functions as F

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_key'
])

source_bucket = args['source_bucket']
source_key = args['source_key']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """Rules = [
    ColumnCount = 16,

    IsComplete "rating",
    IsComplete "rating_count",
    IsComplete "discounted_price",
    IsComplete "actual_price",

    ColumnValues "rating" >= 0,
    ColumnValues "rating_count" >= 0,
    ColumnValues "discounted_price" >= 0,
    ColumnValues "actual_price" >= 0,
    ColumnValues "discount_percentage" >= 0,
    
    IsUnique "product_id"
]"""

# -------- DELIMITER CORRUPTION CHECK -------- #
# It checks for delimiter / structural corruption STARTS
import csv
import io

raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").collect()

if len(raw_lines) < 2:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (file has no data rows)")

def count_csv_columns(line: str) -> int:
    """Count columns correctly, respecting quoted fields containing commas."""
    try:
        return len(next(csv.reader(io.StringIO(line))))
    except StopIteration:
        return 0

header_line = raw_lines[0][0]
expected_col_count = count_csv_columns(header_line)

bad_row_indices = []
for i, row in enumerate(raw_lines[1:], start=2):
    line = row[0]
    actual_col_count = count_csv_columns(line)
    if actual_col_count != expected_col_count:
        bad_row_indices.append((i, actual_col_count, line[:80]))

if bad_row_indices:
    details = "\n".join(
        [f"  Line {idx}: expected {expected_col_count} cols, got {actual} → {preview}..."
         for idx, actual, preview in bad_row_indices]
    )
    raise Exception(
        f"Glue Visual ETL | Corrupted CSV detected — {len(bad_row_indices)} row(s) have wrong column count "
        f"(likely semicolons used as delimiters instead of commas):\n{details}"
    )

print(f"Glue Visual ETL | Column count validation passed: all rows have {expected_col_count} columns")
# It checks for delimiter / structural corruption ENDS
# -------- DELIMITER CORRUPTION CHECK -------- #

# Script generated for node Raw data source S3
RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE", "optimizePerformance": False}, connection_type="s3", format="csv", 
    # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
    connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
    transformation_ctx="RawdatasourceS3_node1772431168408")
# for debugging only
print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

df = RawdatasourceS3_node1772431168408.toDF()

print("Glue Visual ETL | DEBUG bucket:", source_bucket)
print("Glue Visual ETL | DEBUG key:", source_key)
print("Glue Visual ETL | DEBUG columns:", df.columns)
print("Glue Visual ETL | DEBUG row count:", df.count())
print("Glue Visual ETL | DEBUG schema:", df.schema)
print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())

# -------- CORRUPTION CHECK -------- #
# checks Spark-level corruption after parsing STARTS

# If Spark inferred no columns → corrupted file
if len(df.columns) == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (no columns inferred)")

expected_cols = len(df.columns)

# If dataframe empty → corrupted file
if df.limit(1).count() == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (empty dataframe)")

# Row-level corruption detection
null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]

bad_rows = df.filter(
    reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7
)

if bad_rows.limit(1).count() > 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (null-heavy rows)")

# checks Spark-level corruption after parsing ENDS
# -------- CORRUPTION CHECK -------- #

#--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#
# Script generated for node SQL Query
SqlQuery61 = '''
SELECT
    product_id,
    product_name,
    category,
    about_product,
    user_id,
    user_name,
    review_id,
    review_title,
    review_content,
    img_link,
    product_link,

    -- discounted_price: ₹149 → 149.0
    CAST(
        REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS discounted_price,

    -- actual_price: ₹1,000 → 1000.0
    CAST(
        REGEXP_REPLACE(actual_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS actual_price,

    -- discount_percentage: 85% → 85.0
    CAST(
        REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
        AS DOUBLE
    ) AS discount_percentage,

    -- rating: 3.9 → 3.9
    CAST(
        REGEXP_REPLACE(rating, '[^0-9.]', '')
        AS DOUBLE
    ) AS rating,

    -- rating_count: 24,871 → 24871
    CAST(
        REGEXP_REPLACE(rating_count, '[^0-9]', '')
        AS INT
    ) AS rating_count

FROM myDataSource;
'''

try:
    SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")
except Exception as e:
    print(f"Glue Visual ETL | SQL query execution failed | {e}")
    raise RuntimeError("Failing Glue job explicitly | Failed to execute sql query")
#--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#

# Script generated for node Drop Null Fields
DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

# -------- DEDUPLICATION STEP -------- #
# This will check if there exists a row with a duplicate review_id if yes then it will drop one of the row and make sure that the row is unique
# Doing this prevents any data duplication 
df_clean = DropNullFields_node1772431857999.toDF()

deduped_df = df_clean.dropDuplicates()

print("Glue Visual ETL | Before dedup:", df_clean.count())
print("Glue Visual ETL | After dedup:", deduped_df.count())

deduped_dynamic_frame = DynamicFrame.fromDF(
    deduped_df, glueContext, "deduped_dynamic_frame"
)
# -------- DEDUPLICATION STEP -------- #

# Script generated for node Silver layer data sink S3
try:
    EvaluateDataQuality().process_rows(frame=deduped_dynamic_frame, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
except Exception as e:
    print("Glue Visual ETL | DQ failed:", e)
    raise RuntimeError("Failing Glue job explicitly | Failed to run data quality rules")

SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
SilverlayerdatasinkS3_node1772432020219.writeFrame(deduped_dynamic_frame)
job.commit()
```
- AWS Glue ETL configurations
    - ![ingest_sales_data_1](images/production_grade_glue_version5_dynamo_db/AWS_glue_ETL/ingest_sales_data_1.png)
    - ![ingest_sales_data_2](images/production_grade_glue_version5_dynamo_db/AWS_glue_ETL/ingest_sales_data_2.png)
    - ![ingest_sales_data_3](images/production_grade_glue_version5_dynamo_db/AWS_glue_ETL/ingest_sales_data_3.png)
    - ![ingest_sales_data_4](images/production_grade_glue_version5_dynamo_db/AWS_glue_ETL/ingest_sales_data_4.png)
    - ![ingest_sales_data_5](images/production_grade_glue_version5_dynamo_db/AWS_glue_ETL/ingest_sales_data_5.png)
    - ![ingest_sales_data_6](images/production_grade_glue_version5_dynamo_db/AWS_glue_ETL/ingest_sales_data_6.png)
- The Glue script is the actual workhorse of the entire pipeline. Every other component — S3, EventBridge, SQS, Lambda, Step Function — exists purely to get a file path to this script safely and exactly once. Here is what it does, stage by stage.
    - ![glue_etl_pipeline_flow](images/production_grade_glue_version5_dynamo_db/AWS_glue_ETL/glue_etl_pipeline_flow.svg)
    - How it receives its input
        - The Step Function calls ```glue:startJobRun``` and passes ```--source_bucket``` and ```--source_key``` as job arguments. Glue captures them with ```getResolvedOptions```. This is critical the script processes exactly one specific file, not a whole folder. It knows precisely which file to open because the Lambda extracted bucket and key from the S3 event and the Step Function forwarded them here.
    - Stage 1 — Delimiter corruption check
        - Before Spark even touches the file, the script reads it as raw text line by line and counts the columns in every single row using Python's ```csv.reader```. It compares each row's column count against the header's column count. If any row has a different count — which happens when a file uses semicolons instead of commas as delimiters, or has extra commas inside unquoted fields — it raises an ```Exception``` immediately with the exact line numbers that are broken. This is a fast, pre-Spark guard that catches structural corruption before wasting cluster resources.
    - Stage 2 — Read CSV into DynamicFrame
        - Now Glue reads the file into a ```DynamicFrame``` using ```create_dynamic_frame.from_options```. Notice ```"recurse": False``` it reads only the exact file specified, not the whole folder. The ```"mode": "PERMISSIVE"``` setting means Spark won't crash on individual bad rows during parsing; instead it sets those fields to null, which the subsequent checks then catch.
    - Stage 3 — Spark-level corruption check
        - After Spark parses the file, three more checks run. First it verifies that Spark actually inferred at least one column — an empty column list means the file was completely unreadable. Second it checks the DataFrame isn't empty. Third it looks for rows where more than 70% of columns are null, which indicates rows that Spark parsed but mostly failed on. Any of these throws an ```Exception```.
    - Stage 4 — SQL type casting
        - This is where the actual transformation happens. The CSV stores everything as strings — prices come in as ```"₹1,499"```, discounts as ```"85%"```, ratings as ```"3.9"```. The SQL query uses ```REGEXP_REPLACE``` to strip all non-numeric characters, then ```CAST``` converts them to ```DOUBLE``` or ```INT```. The 11 text columns (```product_id```, ```product_name```, ```category```, etc.) pass through unchanged. If this SQL fails for any reason, it re-raises as a RuntimeError so the Step Function's Catch block can detect it and route to ```UpdateFailure``` + DLQ.
    - Stage 5 — Drop null columns
        - The drop_nulls function walks the schema recursively and finds any columns where every single value is either empty string, ```"null```", or ```-1```. Those entire columns are dropped. This handles the case where a CSV has a column header but no data ever in it across the whole file.
    - Stage 6 — Deduplication
        - ```dropDuplicates()``` with no arguments compares every column in every row. If two rows are completely identical across all columns, one gets dropped. This prevents duplicate rows from landing in the Silver layer if the upstream system accidentally sent the same record twice.
    - Stage 7 — Data quality evaluation
        - ```EvaluateDataQuality``` runs a formal ruleset against the data before writing it anywhere. The rules check that the column count is exactly 16 (schema drift detection), that key numeric columns have no nulls, that no numeric column has negative values, and that ```product_id``` is unique. This is wrapped in a ```try/except``` if the rules fail, it raises ```RuntimeError``` explicitly. This is important because without the explicit raise, Glue might log the DQ failure but continue writing bad data to S3.
    - Stage 8 Write to Silver S3 and update catalog
        - If all checks pass, the data is written to ```s3://data-sink-one/silver_layer/sales_data/``` as Snappy-compressed Parquet files, partitioned by ```category```. The ```enableUpdateCatalog=True``` and ```setCatalogInfo``` call means Glue simultaneously registers or updates the table ```silver_table_sales_data``` in the Glue Data Catalog. This is what makes the data immediately queryable in Athena without needing to run a crawler separately.
        - Finally job.commit() tells Glue the job succeeded cleanly.
- How failure flows back to the rest of the pipeline?
    - Any ```raise``` in this script causes the Glue job to exit with a failed status. The Step Function is waiting with ```glue:startJobRun.sync```, so it detects the failure immediately and routes to ```UpdateFailure``` which writes ```status = FAILED``` and the error message to DynamoDB then sends the event to the DLQ. From there your ```replay_failed_ingestion``` Step Function can pick it up later once the underlying problem is fixed.

#### DynamoDB setup
- ![dynamo_db_1](images/production_grade_glue_version5_dynamo_db/dynamo_db/dynamo_db_1.png)
- ![dynamo_db_2](images/production_grade_glue_version5_dynamo_db/dynamo_db/dynamo_db_2.png)
- ![dynamo_db_3](images/production_grade_glue_version5_dynamo_db/dynamo_db/dynamo_db_3.png)
- ![dynamo_db_4](images/production_grade_glue_version5_dynamo_db/dynamo_db/dynamo_db_4.png)
- ![dynamo_db_5](images/production_grade_glue_version5_dynamo_db/dynamo_db/dynamo_db_5.png)
- The table name in dynamoDB is ```file_processing_registry```
- After the setup you will see that lamdba function added these in dynamoDb
    - ![dynamo_db_6](images/production_grade_glue_version5_dynamo_db/dynamo_db/dynamo_db_6.png)
    - As you can see that the files are being tracked successfully.
- Things to remember :
    - Partition key should be set to ```file_key```
    - This is the command used to query the data present in the dynamoDB ```aws dynamodb scan --table-name file_processing_registry```
- Theory :
    - Partition key : A unique identifier that determines the item's physical storage location through hashing.
    - Sort keys (OPTIONAL) : It is a secondary attribute which enables ordering and range queries within a partition.
    - Primary key : ```{partition_key}:{sort_key}```
- For more information on DynamoDB refer to this video : https://youtu.be/2X2SO3Y-af8?si=8HLw7LvQEV_TEG-B   

### Issues I faced during Implementation phase for version 5
#### The issue I was facing is that ```UpdateFailure``` is working correctly ```status = FAILED``` and ```error_message``` is polulated in dynamoDB but the DLQ is empty. This happens when there is failure in the execution AWS glue ETL pipeline. (orchestrate_data_ingestion step function related bug)
Explainaing what is happening : 
- My dynamoDB record shows 
    - ```status = FAILED``` UpdateFailure ran successfully
    - ```error_message``` is populated with the full Glue error.
    - ```updated_at = 2026-03-24T11:49:40.057Z```
- So ```UpdateFailure``` is completing. But the DLQ is empty which means ```SendToDLQ``` is failing silently after ```UpdateFailure```.
- The reason is in ```SendToDLQ```. Look at what it tries to read:
    - ```json
        "MessageBody": {
        "bucket.$":   "$.bucket",
        "key.$":      "$.key",
        "file_key.$": "$.file_key"
        }
      ```
- These paths ```$.bucket```, ```$.key```, ```$.file_key``` came from the Map iterator input. But after ```RunGlueJob``` fails with ```"ResultPath": "$.error"```, the error gets written to ```$.error```. At this point the Step Function state still has ```$.bucket```, ```$.key```, ```$.file_key``` available from the original Map item input.
- However, look at your Glue runs in Image 1 they are all failing in about 1 minute, and the ```States.ALL``` retry fires 2 times with 30 second intervals before the Catch block runs. During those retries, the ```ResultPath``` behaviour can shift the state. More critically look at the failure time: ```17:17:37``` to ```17:18:47``` is only 1m 2s, which means the retries may not be completing cleanly, causing the input state to be in an unexpected shape when ```UpdateFailure``` runs.
- The most likely cause is this: SendToDLQ is throwing because ```$.bucket```, ```$.key```, or ```$.file_key``` is resolving to null in the state context after the retry/catch cycle. You can confirm this by going to the Step Function execution for the ```17:17:37``` run and checking whether ```SendToDLQ``` appears in the execution history at all, and what error it shows.

Solution : 
- pass file context explicitly through the error catch
    - Change ```RunGlueJob```'s ```Catch``` block to preserve the original input alongside the error:
        - ```json
            "Catch": [
            {
                "ErrorEquals": ["States.ALL"],
                "ResultPath": "$.error",
                "Next": "UpdateFailure"
            }
            ]
          ```
    - This is already correct. The problem is subtler ```SendToDLQ``` needs to reference fields that are guaranteed to exist. Add a dedicated ```Pass``` state between ```UpdateFailure``` and ```SendToDLQ``` that explicitly builds the message body from known-safe paths:
        - ```json
          "UpdateFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
                "TableName": "file_processing_registry",
                "Key": { "file_key": { "S.$": "$.file_key" } },
                "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
                "ExpressionAttributeNames": { "#s": "status" },
                "ExpressionAttributeValues": {
                ":s": { "S": "FAILED" },
                ":e": { "S.$": "States.JsonToString($.error)" },
                ":t": { "S.$": "$$.State.EnteredTime" }
                }
            },
            "ResultPath": "$.dynamoResult",
            "Next": "SendToDLQ"
            },
            "SendToDLQ": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sqs:sendMessage",
            "Parameters": {
                "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                "MessageBody": {
                "bucket.$":   "$.bucket",
                "key.$":      "$.key",
                "file_key.$": "$.file_key"
                }
            },
            "End": true
            }
          ```
    - The key change is adding ```"ResultPath": "$.dynamoResult"``` to ```UpdateFailure```. Without this, ```UpdateFailure```'s output replaces the entire state with the DynamoDB response, wiping out ```$.bucket```, ```$.key```, and ```$.file_key```. When ```SendToDLQ``` then tries to read ```$.bucket``` it gets null and throws.
    - With ```"ResultPath": "$.dynamoResult"```, the DynamoDB response is written to ```$.dynamoResult``` and the rest of the original state including ```$.bucket```, ```$.key```, ```$.file_key``` is preserved for ```SendToDLQ``` to read correctly.

#### Fixed step function code (orchestrate_data_ingestion step function)
```json
{
  "Comment": "Glue ETL Orchestration with DynamoDB Tracking",
  "StartAt": "ProcessFiles",
  "States": {
    "ProcessFiles": {
      "Type": "Map",
      "ItemsPath": "$.files",
      "MaxConcurrency": 2,
      "Iterator": {
        "StartAt": "RunGlueJob",
        "States": {
          "RunGlueJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "ingest_sales_data",
              "Arguments": {
                "--source_bucket.$": "$.bucket",
                "--source_key.$": "$.key"
              }
            },
            "ResultPath": "$.glueResult",
            "Retry": [
              {
                "ErrorEquals": [
                  "Glue.ConcurrentRunsExceededException"
                ],
                "IntervalSeconds": 60,
                "MaxAttempts": 10,
                "BackoffRate": 1.5
              },
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "IntervalSeconds": 30,
                "MaxAttempts": 2,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "ResultPath": "$.error",
                "Next": "UpdateFailure"
              }
            ],
            "Next": "UpdateSuccess"
          },
          "UpdateSuccess": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "SUCCESS"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "End": true
          },
          "UpdateFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "FAILED"
                },
                ":e": {
                  "S.$": "States.JsonToString($.error)"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "ResultPath": "$.dynamoResult",
            "Next": "SendToDLQ"
          },
          "SendToDLQ": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sqs:sendMessage",
            "Parameters": {
              "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
              "MessageBody": {
                "bucket.$": "$.bucket",
                "key.$": "$.key",
                "file_key.$": "$.file_key"
              }
            },
            "End": true
          }
        }
      },
      "End": true
    }
  }
}
```

#### retry_count type mismatch (replay_failed_ingestion step function)
Explainaing what is happening : 
- The problem DynamoDB Number type vs Step Functions string comparison
- When DynamoDB returns a Number attribute, it comes back in the Step Functions state as a JSON object like this:
    - ```json
      "retry_count": { "N": "0" }
      ```
- The "N" value is a string "0", not a numeric 0. Step Functions stores DynamoDB Number values as strings internally.
- ```EvaluateRetry``` condition is:
    - ```json
        {
        "Variable": "$.ddb.Item.retry_count.N",
        "NumericLessThan": 3,
        "Next": "IncrementRetry"
        }
      ```
- ```NumericLessThan: 3``` expects the variable to be a numeric type. But ```$.ddb.Item.retry_count.N``` is the string ```"0"```
- The numeric comparison against a string fails silently it evaluates to false and the Default branch runs, sending you to SkipReplay every time.

Solution : 
- use StringLessThan instead
    - ```json
      "EvaluateRetry": {
        "Type": "Choice",
        "Choices": [
            {
            "Variable": "$.ddb.Item.retry_count.N",
            "StringLessThan": "3",
            "Next": "IncrementRetry"
            }
        ],
        "Default": "SkipReplay"
        }
      ```
    - ```StringLessThan: "3"``` correctly compares the string ```"0"``` against the string ```"3"```, and since ```"0" < "3"``` lexicographically, it routes to ```IncrementRetry``` and the Glue job runs.
    - This works correctly for single-digit retry counts (0, 1, 2). Since my limit is 3, I will never reach double digits, so string comparison is safe here.

#### Error in (replay_failed_ingestion)
This is the error message that I recieved when trying to execute the replay step function:
```json
An error occurred while executing the state 'RunGlueJob' (entered at the event id #26). The JSONPath '$.parsed.body.bucket' specified for the field '--source_bucket.$' could not be found in the input '{"SdkHttpMetadata":{"AllHttpHeaders":{"Server":["Server"],"Connection":["keep-alive"],"x-amzn-RequestId":["VRHKQ440NT8P25MIPT1MU4NPMBVV4KQNSO5AEMVJF66Q9ASUAAJG"],"x-amz-crc32":["2745614147"],"Content-Length":["2"],"Date":["Wed, 25 Mar 2026 07:01:55 GMT"],"Content-Type":["application/x-amz-json-1.0"]},"HttpHeaders":{"Connection":"keep-alive","Content-Length":"2","Content-Type":"application/x-amz-json-1.0","Date":"Wed, 25 Mar 2026 07:01:55 GMT","Server":"Server","x-amz-crc32":"2745614147","x-amzn-RequestId":"VRHKQ440NT8P25MIPT1MU4NPMBVV4KQNSO5AEMVJF66Q9ASUAAJG"},"HttpStatusCode":200},"SdkResponseMetadata":{"RequestId":"VRHKQ440NT8P25MIPT1MU4NPMBVV4KQNSO5AEMVJF66Q9ASUAAJG"}}'
```
- The error says ```$.parsed.body.bucket``` could not be found, and the input it shows is the DynamoDB ```updateItem``` response ```SdkHttpMetadata```, ```HttpStatusCode: 200```, etc. This means ```IncrementRetry```'s output is replacing the entire state, wiping out ```$.parsed``` before ```RunGlueJob``` can read it.
- The root cause missing ```ResultPath``` on ```IncrementRetry```
    - Look at ```IncrementRetry``` state:
        - ```json
            "IncrementRetry": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": { ... },
            "Next": "RunGlueJob"
            }
          ```
    - There is no ```ResultPath```. When a Task state has no ```ResultPath```, the AWS SDK response from DynamoDB completely replaces the entire state input. So when ```RunGlueJob``` runs, the state no longer contains ```$.parsed.body.bucket``` it only contains the DynamoDB HTTP response object you can see in the error.

Solution : 
- Add ```"ResultPath": "$.incrementResult"``` to ```IncrementRetry``` so the DynamoDB response is written to a separate field and the original state including ```$.parsed.body.bucket```, ```$.parsed.body.key```, ```$.parsed.body.file_key``` is preserved:
    - ```json
        "IncrementRetry": {
        "Type": "Task",
        "Resource": "arn:aws:states:::dynamodb:updateItem",
        "Parameters": {
            "TableName": "file_processing_registry",
            "Key": {
            "file_key": {
                "S.$": "$.parsed.body.file_key"
            }
            },
            "UpdateExpression": "ADD retry_count :inc",
            "ExpressionAttributeValues": {
            ":inc": { "N": "1" }
            }
        },
        "ResultPath": "$.incrementResult",
        "Next": "RunGlueJob"
        }
      ```
- add ResultPath to every Task state that writes to DynamoDB so the response never overwrites the original input:
    - ```json
      "UpdateFailure": {
        "Type": "Task",
        "Resource": "arn:aws:states:::dynamodb:updateItem",
        "Parameters": {
            "TableName": "file_processing_registry",
            "Key": {
            "file_key": { "S.$": "$.parsed.body.file_key" }
            },
            "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
            "ExpressionAttributeNames": { "#s": "status" },
            "ExpressionAttributeValues": {
            ":s": { "S": "FAILED" },
            ":e": { "S.$": "States.JsonToString($.error)" },
            ":t": { "S.$": "$$.State.EnteredTime" }
            }
        },
        "ResultPath": "$.updateFailureResult",
        "Next": "DeleteMessage"
        },
        "UpdateSuccess": {
        "Type": "Task",
        "Resource": "arn:aws:states:::dynamodb:updateItem",
        "Parameters": {
            "TableName": "file_processing_registry",
            "Key": {
            "file_key": { "S.$": "$.parsed.body.file_key" }
            },
            "UpdateExpression": "SET #s = :s, updated_at = :t",
            "ExpressionAttributeNames": { "#s": "status" },
            "ExpressionAttributeValues": {
            ":s": { "S": "SUCCESS" },
            ":t": { "S.$": "$$.State.EnteredTime" }
            }
        },
        "ResultPath": "$.updateSuccessResult",
        "Next": "DeleteMessage"
        }
      ```
- Every Task state that writes to DynamoDB ```IncrementRetry```, ````UpdateSuccess````, ```UpdateFailure``` needs a ```ResultPath``` that dumps the AWS SDK response somewhere harmless. Without it, the DynamoDB HTTP response body replaces the entire state and ```$.parsed.receiptHandle``` disappears before ```DeleteMessage``` can use it.

#### Design error in (replay_failed_ingestion) where right now the delete event message from sqs queue is running regardless of ETL success or failure I need to fix it 
- Before : 
![replay_state_function_diagram_1](images/production_grade_glue_version5_dynamo_db/step_functions/replay_state_function_diagram_1.png)
- After : 
![replay_state_function_diagram_2](images/production_grade_glue_version5_dynamo_db/step_functions/replay_state_function_diagram_2.png)
- In order to implement the changes all you need to do is 
    - Before : 
        - ```json
            "UpdateFailure": {
            ...
            "Next": "DeleteMessage"   ← always deletes, success or failure
            }
          ```
    - change the above code to this : only delete on success, keep message on failure in DLQ
        - ```json
            "UpdateFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
                "TableName": "file_processing_registry",
                "Key": {
                "file_key": { "S.$": "$.parsed.body.file_key" }
                },
                "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
                "ExpressionAttributeNames": { "#s": "status" },
                "ExpressionAttributeValues": {
                ":s": { "S": "FAILED" },
                ":e": { "S.$": "States.JsonToString($.error)" },
                ":t": { "S.$": "$$.State.EnteredTime" }
                }
            },
            "ResultPath": "$.updateFailureResult",
            "End": true
            }
          ```
#### Fixed step function code (replay_failed_ingestion step function) 
```json
{
  "Comment": "Replay ETL from DLQ (Fixed - State Preservation + Safe Errors)",
  "StartAt": "ReceiveFromDLQ",
  "States": {
    "ReceiveFromDLQ": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
      "Parameters": {
        "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
        "MaxNumberOfMessages": 10
      },
      "ResultPath": "$.dlq",
      "Next": "CheckIfEmpty"
    },
    "CheckIfEmpty": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.dlq.Messages",
          "IsPresent": false,
          "Next": "Success"
        }
      ],
      "Default": "ReplayBatch"
    },
    "ReplayBatch": {
      "Type": "Map",
      "ItemsPath": "$.dlq.Messages",
      "MaxConcurrency": 2,
      "Iterator": {
        "StartAt": "ExtractPayload",
        "States": {
          "ExtractPayload": {
            "Type": "Pass",
            "Parameters": {
              "receiptHandle.$": "$.ReceiptHandle",
              "body.$": "States.StringToJson($.Body)"
            },
            "ResultPath": "$.parsed",
            "Next": "CheckRetry"
          },
          "CheckRetry": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:getItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              }
            },
            "ResultPath": "$.ddb",
            "Next": "EvaluateRetry"
          },
          "EvaluateRetry": {
            "Type": "Choice",
            "Choices": [
              {
                "Variable": "$.ddb.Item.retry_count.N",
                "StringLessThan": "3",
                "Next": "IncrementRetry"
              }
            ],
            "Default": "SkipReplay"
          },
          "IncrementRetry": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              },
              "UpdateExpression": "ADD retry_count :inc",
              "ExpressionAttributeValues": {
                ":inc": {
                  "N": "1"
                }
              }
            },
            "ResultPath": "$.incrementResult",
            "Next": "RunGlueJob"
          },
          "RunGlueJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "ResultPath": "$.glueResult",
            "Parameters": {
              "JobName": "ingest_sales_data",
              "Arguments": {
                "--source_bucket.$": "$.parsed.body.bucket",
                "--source_key.$": "$.parsed.body.key"
              }
            },
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "ResultPath": "$.error",
                "Next": "UpdateFailure"
              }
            ],
            "Next": "UpdateSuccess"
          },
          "UpdateSuccess": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "SUCCESS"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "ResultPath": "$.updateSuccessResult",
            "Next": "DeleteMessage"
          },
          "UpdateFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "file_processing_registry",
              "Key": {
                "file_key": {
                  "S.$": "$.parsed.body.file_key"
                }
              },
              "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
              "ExpressionAttributeNames": {
                "#s": "status"
              },
              "ExpressionAttributeValues": {
                ":s": {
                  "S": "FAILED"
                },
                ":e": {
                  "S.$": "States.JsonToString($.error)"
                },
                ":t": {
                  "S.$": "$$.State.EnteredTime"
                }
              }
            },
            "ResultPath": "$.updateFailureResult",
            "End": true
          },
          "SkipReplay": {
            "Type": "Pass",
            "Next": "DeleteMessage"
          },
          "DeleteMessage": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
            "Parameters": {
              "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
              "ReceiptHandle.$": "$.parsed.receiptHandle"
            },
            "End": true
          }
        }
      },
      "Next": "Success"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
```

## Implementation : (Version 6)
### GAPs that I need to fix that were present in Implementation of version 5 
#### High priority these affect correctness and reliability

**1. Lambda idempotency permanently blocks `FAILED` files on re-upload**

The `put_item` with `attribute_not_exists(file_key)` blocks any file whose `file_key` already exists in DynamoDB regardless of whether status is `SUCCESS`, `FAILED`, or `IN_PROGRESS`. This means if a corrupted file fails and a corrected version is uploaded with the same filename, Lambda will silently skip it forever. The fix is to check status first — only permanently block `SUCCESS`, allow `FAILED` records to be overwritten:

```python
existing = dynamodb.get_item(
    TableName=TABLE_NAME,
    Key={"file_key": {"S": file_key}}
)
item = existing.get("Item")

if item and item.get("status", {}).get("S") == "SUCCESS":
    print(f"Already succeeded, skipping: {file_key}")
    continue

# New file or previously failed allow through
try:
    dynamodb.put_item(
        TableName=TABLE_NAME,
        Item={
            "file_key": {"S": file_key},
            "status": {"S": "IN_PROGRESS"},
            "retry_count": {"N": "0"},
            "created_at": {"S": timestamp}
        },
        ConditionExpression="attribute_not_exists(file_key) OR #s = :failed",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":failed": {"S": "FAILED"}}
    )
    files.append({"bucket": bucket, "key": key, "file_key": file_key})
except dynamodb.exceptions.ConditionalCheckFailedException:
    print(f"Currently IN_PROGRESS, skipping: {file_key}")
```

**2. `DQFailureSilent` data quality runs but the job can still write bad data**

Your `EvaluateDataQuality` is wrapped in a `try/except` that raises `RuntimeError` on failure which is correct. However `EvaluateDataQuality().process_rows()` with `"strategy": "BEST_EFFORT"` logs DQ results but does not fail the Glue job on rule violations by itself it depends entirely on your `try/except` catching the exception. The problem is that `process_rows` with `BEST_EFFORT` does not always throw even when rules fail it publishes metrics and continues. This means data that fails the `IsUnique "product_id"` or `ColumnCount = 16` check can silently pass through to the Silver layer. Change the strategy:

```python
EvaluateDataQuality().process_rows(
    frame=deduped_dynamic_frame,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "...",
        "enableDataQualityResultsPublishing": True,
        "failureAction": "FAIL"      # ← add this
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "FAIL_JOB",   # ← change this
        "observations.scope": "ALL"
    }
)
```

**3. `dropDuplicates()` with no arguments is too aggressive**

Your deduplication uses `df_clean.dropDuplicates()` which compares every single column. This means two rows that have identical product data but different `review_id` values will both be kept which is correct. But two rows that are truly identical across all 16 columns will lose one copy which is also correct. The real problem is that `dropDuplicates()` on a large DataFrame triggers a full shuffle across the entire Spark cluster, which is very expensive. For this dataset the correct key for deduplication is `review_id` since that is the natural unique identifier per review:

```python
deduped_df = df_clean.dropDuplicates(["review_id"])
```

This is both semantically correct you are deduplicating on the business key and significantly faster because Spark can partition by `review_id` rather than hashing all 16 columns.

**4. `sales-ingestion-dlq` visibility timeout is 12 hours**

After `replay_failed_ingestion` reads a message via `ReceiveFromDLQ`, that message becomes invisible for 12 hours even if `UpdateFailure` ends with `"End": true` and does not delete it. From a data engineering perspective this means the message is effectively locked for half a day between replay attempts. Reduce the visibility timeout to 5 minutes so the message becomes available again quickly after each replay attempt.

---

#### Medium priority these affect observability and operational quality

**5. No `row_count` written to DynamoDB after successful ingestion**

Your DynamoDB schema in the proposed architecture diagram shows `row_count` as an attribute, but the `UpdateSuccess` state only writes `status` and `updated_at`. You never capture how many rows were actually ingested. This is a significant gap for a data platform without row counts you cannot detect silent data loss (a file that processed successfully but produced 0 rows, or a file that should have 500 rows but only produced 50). Add a row count capture in the Glue script:

```python
row_count = deduped_df.count()
print(f"Glue Visual ETL | Final row count: {row_count}")
```

Then pass it as a Glue job metric or as a separate DynamoDB update. One way is to write it as a Glue job property that the Step Function can read from the `glueResult` after success, then include it in `UpdateSuccess`.

**6. No CloudWatch alerting on DLQ depth**

There is no monitoring documented. In production the most critical metric to alert on is `ApproximateNumberOfMessagesVisible` on `sales-ingestion-dlq` if this grows it means files are failing faster than they are being replayed. A simple CloudWatch alarm that fires when DLQ message count exceeds 0 would give you immediate visibility. Similarly, a CloudWatch alarm on Glue job failures and Step Function execution failures would complete the observability picture.

**7. Debug print statements left in the Glue script**

The Glue script has several production debug lines that should either be removed or controlled by a log level flag:

```python
print("Glue Visual ETL | DEBUG bucket:", source_bucket)
print("Glue Visual ETL | DEBUG columns:", df.columns)
print("Glue Visual ETL | DEBUG schema:", df.schema)
print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())
```

The `df.limit(2).toPandas()` line is particularly costly it materialises a Pandas DataFrame on the driver node and prints raw data values to CloudWatch logs. This is a privacy concern if the data contains PII and a cost concern since it triggers a Spark action. Remove or gate these behind a debug flag.

**8. `raw_lines = spark.read.text(...).collect()` loads entire file to driver**

The delimiter corruption check reads the entire file as text and calls `.collect()` which brings every single line into the Lambda driver's memory. For small files like your sales CSVs this is fine, but if a file is 500MB this will OOM the driver. A safer approach is to sample or limit:

```python
raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").limit(1000).collect()
```

For corruption detection you typically only need to scan a representative sample rather than every row.

#### Lower priority these are good engineering hygiene

**9. `AmazonSQSFullAccess` on the replay Step Function role is too broad**

The IAM role for `replay_failed_ingestion` uses `AmazonSQSFullAccess`. This grants full permissions to all SQS queues in the account. The minimum required is `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and `sqs:GetQueueAttributes` scoped to only `sales-ingestion-dlq`. You noted this is for learning purposes which is fine, but worth flagging for when this moves to a real environment.

**10. No TTL on DynamoDB records**

The `file_processing_registry` table will grow forever every file ever processed keeps a permanent record. For a data engineering use case you typically only need records for the last 30–90 days for audit purposes. Add a `ttl` attribute when writing records in Lambda and enable DynamoDB TTL on that attribute. This keeps the table lean and reduces read costs over time:

```python
import time
ttl_value = int(time.time()) + (90 * 24 * 60 * 60)  # 90 days from now

dynamodb.put_item(
    TableName=TABLE_NAME,
    Item={
        "file_key": {"S": file_key},
        "status": {"S": "IN_PROGRESS"},
        "retry_count": {"N": "0"},
        "created_at": {"S": timestamp},
        "ttl": {"N": str(ttl_value)}   # ← add this
    },
    ...
)
```

### Implementation for version 6:
- With this implementation I am aiming to close all the gaps that are currently present in the version 5 implementation.

#### Architectural diagram
![version6_architecture](images/production_grade_implementation_version_6/architectural_diagram/version6_architecture.svg)

#### S3 bucket
![s3_event_bridge_setup](images/production_grade_implementation_version_6/S3/s3_event_bridge_setup.png)
- This setup allows source S3 bucket to send events to event bridge.

#### EventBridge 
- ![event_bridge_setup_1](images/production_grade_implementation_version_6/EventBridge/event_bridge_setup_1.png)
- Event pattern (filter)
    - ```json
      {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "bucket": {
            "name": ["aws-glue-s3-bucket-one"]
            },
            "object": {
            "key": [{
                "prefix": "raw_data/sales_data/"
            }]
            }
        }
      }
      ```
- ![event_bridge_setup_2](images/production_grade_implementation_version_6/EventBridge/event_bridge_setup_2.png)
- ![event_bridge_setup_3](images/production_grade_implementation_version_6/EventBridge/event_bridge_setup_3.png)
- ![event_bridge_setup_4](images/production_grade_implementation_version_6/EventBridge/event_bridge_setup_4.png)
    - When creating the event bridge for the first time make sure you select this option ```Create a new role for this specific resource``` and let AWS create a new role for this Eventbridge rule on our behalf automatically.
- ![event_bridge_setup_5](images/production_grade_implementation_version_6/EventBridge/event_bridge_setup_5.png)

#### SQS 

**(FileProcessingQueue.fifo)**
- ![FileProcessingQueue.fifo_1](images/production_grade_implementation_version_6/SQS/FileProcessingQueue.fifo_1.png)
- ![FileProcessingQueue.fifo_2](images/production_grade_implementation_version_6/SQS/FileProcessingQueue.fifo_2.png)
- ![FileProcessingQueue.fifo_3](images/production_grade_implementation_version_6/SQS/FileProcessingQueue.fifo_3.png)
- Access Policy used : 
    - ```json
        {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Sid": "AllowEventBridgeToSend",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            }
        ]
        }
      ```

**(sales-ingestion-dlq)**
- ![sales-ingestion-dlq_1](images/production_grade_implementation_version_6/SQS/sales-ingestion-dlq_1.png)
- ![sales-ingestion-dlq_2](images/production_grade_implementation_version_6/SQS/sales-ingestion-dlq_2.png)
- ![sales-ingestion-dlq_3](images/production_grade_implementation_version_6/SQS/sales-ingestion-dlq_3.png)
- Access Policy used : 
    - ```json
        {
        "Version": "2012-10-17",
        "Id": "__default_policy_ID",
        "Statement": [
            {
            "Sid": "__owner_statement",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::406868976171:root"
            },
            "Action": "SQS:*",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq"
            },
            {
            "Sid": "AllowEventBridgeToSendMessage",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            },
            {
            "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            }
        ]
        }
      ```
- Visibility timeout is an SQS setting that controls how long a message stays hidden from other consumers after it has been received by one consumer. When Lambda or a Step Function reads a message from SQS, the queue does not delete it immediately instead it hides it for the duration of the visibility timeout. The expectation is that the consumer will finish processing and explicitly delete the message before the timeout expires. If the consumer crashes, hangs, or fails before deleting it, the timeout expires and SQS makes the message visible again so another consumer can retry it.
- Setting this value correctly matters because the consequences of getting it wrong go in both directions.
    - If the timeout is too short, the message becomes visible again before the consumer has finished processing it. A second consumer picks it up while the first is still running, and now two instances are processing the same message simultaneously. In this pipeline that means two Lambda invocations racing to write the same file_key to DynamoDB, or two replay executions both trying to run Glue against the same file at the same time. The DynamoDB idempotency guard in Lambda catches most of these cases, but it introduces an unnecessary race condition that the correct timeout setting eliminates entirely.
    - If the timeout is too long, a different problem emerges. When replay_failed_ingestion reads a message from sales-ingestion-dlq to retry a failed file, SQS hides that message for the full duration of the timeout regardless of whether the replay succeeded or failed. If the replay fails again and UpdateFailure ends with "End": true without deleting the message, the message is still invisible until the timeout expires. A data engineer trying to trigger another replay immediately after a failure will see an empty queue and have to wait for the full timeout window before the message reappears.

#### Lambda function
- IAM role used for this Lamdba function ```TriggerStepFunctionRoleForLambdaFunc```
    - Custom policies attached to this role 
        - AllowLambdaToAccessSQSAndStepFunction
            - ```json
              {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "states:StartExecution"
                            ],
                            "Resource": "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "sqs:ReceiveMessage",
                                "sqs:DeleteMessage",
                                "sqs:GetQueueAttributes"
                            ],
                            "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue.fifo"
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents"
                            ],
                            "Resource": "*"
                        }
                    ]
                }
              ```
        - DynamoDbAccessPolicyForLambdaFunction
            - ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "DynamoDbAccessPolicyForLambdaFunction",
                            "Effect": "Allow",
                            "Action": [
                                "dynamodb:PutItem",
                                "dynamodb:GetItem",
                                "dynamodb:UpdateItem"
                            ],
                            "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                        }
                    ]
                }
              ```
- Improved lambda function code : 
    - ```python
        import json
        import boto3
        import uuid
        import time
        from datetime import datetime, timezone, timedelta

        sf = boto3.client("stepfunctions")
        dynamodb = boto3.client("dynamodb")

        STATE_MACHINE_ARN = "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"
        TABLE_NAME = "file_processing_registry"

        def lambda_handler(event, context):

            execution_id = str(uuid.uuid4())[:8]

            IST = timezone(timedelta(hours=5, minutes=30))
            timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")

            # TTL: 30 days from now (Unix timestamp)
            ttl_value = int(time.time()) + (30 * 24 * 60 * 60)

            print(f"\n===== Lambda Invocation START =====")
            print(f"ExecutionID: {execution_id}")
            print(f"Timestamp: {timestamp}")
            print("Full Event From SQS:", json.dumps(event))
            expiry_date = datetime.fromtimestamp(ttl_value, tz=timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S IST")
            print(f"The record will be kept in DynamoDB for {expiry_date}")
            print("===================================\n")

            files = []

            for record in event["Records"]:
                body = json.loads(record["body"])

                bucket = body["detail"]["bucket"]["name"]
                key = body["detail"]["object"]["key"]

                file_key = f"s3://{bucket}/{key}"

                # Check for status of file before enforcing idepotency
                # Check if a record already exists and what its status is
                existing = dynamodb.get_item(
                    TableName=TABLE_NAME,
                    Key={"file_key": {"S": file_key}}
                )
                item = existing.get("Item")

                if item:
                    current_status = item.get("status", {}).get("S", "")

                    if current_status == "SUCCESS":
                        # File already successfully ingested — block permanently
                        print(f"Already succeeded, skipping: {file_key}")
                        continue

                    if current_status == "IN_PROGRESS":
                        # Another Lambda instance is currently processing this file
                        print(f"Already in progress, skipping: {file_key}")
                        continue
                    # status == FAILED — allow through, reset the record for retry
                    print(f"Previously failed, allowing re-upload retry: {file_key}")

                try:
                    # Idempotency check (CRITICAL)
                    dynamodb.put_item(
                        TableName=TABLE_NAME,
                        Item={
                            "file_key":    {"S": file_key},
                            "status":      {"S": "IN_PROGRESS"},
                            "retry_count": {"N": "0"},
                            "created_at":  {"S": timestamp},
                            "ttl":         {"N": str(ttl_value)}
                        },
                        ConditionExpression="attribute_not_exists(file_key) OR #s = :failed",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":failed": {"S": "FAILED"}}
                    )

                    # Only add if it's NOT duplicate
                    files.append({
                        "bucket": bucket,
                        "key": key,
                        "file_key": file_key
                    })

                    print(f"Accepted file: {file_key}")

                except dynamodb.exceptions.ConditionalCheckFailedException:
                    # This only fires if another Lambda instance wrote IN_PROGRESS
                    # between our get_item check above and this put_item — a rare
                    # race condition that is now safely handled
                    print(f"Concurrent write detected, skipping: {file_key}")

            print("Files to process:", files)

            # Only trigger StepFn if there are valid files
            if files:
                sf.start_execution(
                    stateMachineArn=STATE_MACHINE_ARN,
                    input=json.dumps({"files": files})
                )
                print("Step Function triggered")
            else:
                print("No new files to process")

            return {"status": "ok"}
      ```
    - This improved code Fixes 
        - Gap 1: Idempotency permanently blocks FAILED files
            - Older lambda function code in version 5 uses this ```ConditionExpression="attribute_not_exists(file_key)"```
            - This blocks any file that already has a DynamoDB record regardless of whether its status is SUCCESS, FAILED, or IN_PROGRESS. So if sales_data_4.csv failed and you upload a corrected version, Lambda sees the existing record and skips it forever with "Duplicate skipped". The only way to recover is to manually add a DLQ message via the SQS console.
            - The fix changes the logic so that only SUCCESS files are permanently blocked. FAILED files are allowed back in because the corrected re-upload is a legitimate recovery path. IN_PROGRESS files are blocked because another Lambda instance is already processing them.
            - The condition changes from a simple attribute_not_exists to a two-step check first read the record, then decide what to do based on its status.
            - **Why are we doing this?**
                - The core reason comes down to one question: who should be responsible for recovering a failed file a human manually intervening in the system, or the system recovering itself automatically?
                - In production data engineering, the answer is always the system. Here is why.
                - What happens without this fix?
                    - A data engineer at a company receives an alert that sales_data_4.csv failed ingestion because it was corrupted. They fix the file and upload the corrected version to S3. From their perspective, the problem is solved the correct file is now in S3.
                    - But nothing happens. The pipeline is completely silent. The file sits in S3 forever, never ingested, never alerting again. The engineer has no feedback that their fix did not work. The downstream dashboard is missing that day's sales data and nobody knows why
                    - Eventually someone notices the data gap, raises a ticket, and a senior engineer has to dig through CloudWatch logs, find the DynamoDB record, understand the visibility timeout, manually construct a JSON message, paste it into the SQS console, and trigger the replay. This takes hours and requires deep knowledge of the pipeline internals.
                - What happens with the fix?
                    - The same engineer fixes the file and uploads the corrected version to S3. The pipeline detects the new upload, sees the existing FAILED record in DynamoDB, resets it to IN_PROGRESS, and triggers the Step Function automatically. Within 2 minutes the file is ingested and the dashboard is updated. The engineer never had to touch SQS, DynamoDB, or CloudWatch.
                - The deeper principle systems should be self-healing.
                    - In data engineering there is a concept called operational burden the amount of human intervention required to keep a pipeline running. Every manual step you require from an operator is a point of failure. Operators make mistakes, forget steps, are on vacation, or simply do not know what to do. A well-designed pipeline minimises operational burden by making the happy path automatic and the recovery path equally automatic.
                    - The re-upload recovery path costs nothing to implement it is a change to one conditional expression in Lambda. But it eliminates an entire category of manual intervention that would otherwise be required every time a file fails and needs to be corrected.
        - Gap 10: No TTL on DynamoDB records
            - Your current put_item never sets a ttl attribute. Without TTL, every record stays in DynamoDB forever. After a year of daily ingestion your table has hundreds of records consuming read capacity on every scan, and you are paying for storage you do not need. DynamoDB's TTL feature automatically deletes expired items at no extra cost. Setting it to 90 days means you keep a full quarter of audit history while the table stays lean.
- Lambda function SQS trigger related configurations
    - ![lambda_1](images/production_grade_implementation_version_6/Lambda_functions/lambda_1.png)

#### State functions

**orchestrate_data_ingestion state function :**
- IAM role and policies configurations :
    - ![orchestrate_data_ingestion](images/production_grade_implementation_version_6/Step_functions/orchestrate_data_ingestion.png)
    - Add a custom policy called ```DynamoDBStepFunctionAccessPolicy``` the rest of them are auto-generated.
    ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DynamoDBStepFunctionAccessPolicy",
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:UpdateItem",
                        "dynamodb:GetItem"
                    ],
                    "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                }
            ]
        }
    ```
- Improved orchestration state function
    - ```json
        {
        "Comment": "Glue ETL Orchestration with DynamoDB Tracking",
        "StartAt": "ProcessFiles",
        "States": {
            "ProcessFiles": {
            "Type": "Map",
            "ItemsPath": "$.files",
            "MaxConcurrency": 2,
            "Iterator": {
                "StartAt": "RunGlueJob",
                "States": {
                "RunGlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                    "JobName": "ingest_sales_data",
                    "Arguments": {
                        "--source_bucket.$": "$.bucket",
                        "--source_key.$": "$.key"
                    }
                    },
                    "ResultPath": "$.glueResult",
                    "Retry": [
                    {
                        "ErrorEquals": [
                        "Glue.ConcurrentRunsExceededException"
                        ],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 10,
                        "BackoffRate": 1.5
                    },
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 2,
                        "BackoffRate": 2
                    }
                    ],
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "ResultPath": "$.error",
                        "Next": "UpdateFailure"
                    }
                    ],
                    "Next": "UpdateSuccess"
                },
                "UpdateSuccess": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName": "file_processing_registry",
                    "Key": {
                        "file_key": {
                        "S.$": "$.file_key"
                        }
                    },
                    "UpdateExpression": "SET #s = :s, updated_at = :t",
                    "ExpressionAttributeNames": {
                        "#s": "status"
                    },
                    "ExpressionAttributeValues": {
                        ":s": {
                        "S": "SUCCESS"
                        },
                        ":t": {
                        "S.$": "$$.State.EnteredTime"
                        }
                    }
                    },
                    "End": true
                },
                "UpdateFailure": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName": "file_processing_registry",
                    "Key": {
                        "file_key": {
                        "S.$": "$.file_key"
                        }
                    },
                    "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t, dlq_event_message = :p",
                    "ExpressionAttributeNames": {
                        "#s": "status"
                    },
                    "ExpressionAttributeValues": {
                        ":s": {
                        "S": "FAILED"
                        },
                        ":e": {
                        "S.$": "States.JsonToString($.error)"
                        },
                        ":t": {
                        "S.$": "$$.State.EnteredTime"
                        },
                        ":p": {
                        "M": {
                            "bucket": {
                            "S.$": "$.bucket"
                            },
                            "key": {
                            "S.$": "$.key"
                            },
                            "file_key": {
                            "S.$": "$.file_key"
                            }
                        }
                        }
                    }
                    },
                    "ResultPath": "$.dynamoResult",
                    "Next": "SendToDLQ"
                },
                "SendToDLQ": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                    "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                    "MessageBody": {
                        "bucket.$": "$.bucket",
                        "key.$": "$.key",
                        "file_key.$": "$.file_key"
                    }
                    },
                    "End": true
                }
                }
            },
            "End": true
            }
        }
        }
      ```
    - This improved code Fixes :
        - The DynamoDB record tells you what failed and why. The DLQ message is the recovery token that tells replay_failed_ingestion exactly which file to re-process.
        - The problem is that the DLQ message is not permanent. It can be lost.
        - During the development of this pipeline, DLQ messages were lost in three different ways. The SendToDLQ state never fired because UpdateFailure was crashing before it could reach that state. The replay Step Function was deleting messages before the fix that added "End": true to UpdateFailure. And once retry_count reaches 3, SkipReplay permanently deletes the message because the file is considered a poison message.
        - In all three cases the outcome was the same — status = FAILED in DynamoDB, empty DLQ, and no automated way back into the pipeline. The only recovery option was manually constructing the JSON payload from memory and pasting it into the SQS console, which is error prone and requires deep knowledge of the message format.
        - Storing dlq_event_message in DynamoDB solves this by making DynamoDB the source of truth for recovery, not SQS. The DLQ message is now treated as a convenience mechanism for triggering automated replay — not as the only place the recovery payload exists. If the DLQ message is lost for any reason, the data engineer can look up the FAILED record in DynamoDB, read the dlq_event_message field, and either paste it directly into SQS to re-add the message or use it to trigger a manual re-ingestion. The exact payload — bucket, key, and file_key — is preserved in the DynamoDB record for as long as the TTL keeps the record alive.
        - This also makes DynamoDB a complete audit log. For every failed file, a single DynamoDB item contains the full lifecycle — when the file arrived, what error caused the failure, what the recovery payload was, how many times it was retried, and when it finally succeeded or was abandoned. Nothing else needs to be checked.

**replay_failed_ingestion step function :**
- IAM role and policies configurations :
    - ![replay_failed_ingestion](images/production_grade_implementation_version_6/Step_functions/replay_failed_ingestion.png)
    - Add two policies to make it work the rest are auto-generated
        - AWS managed policy called ```AmazonSQSFullAccess```\
        - Custom policy called ```DynamoDBStepFunctionAccessPolicy```
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DynamoDBStepFunctionAccessPolicy",
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:UpdateItem",
                        "dynamodb:GetItem"
                    ],
                    "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                }
            ]
        }
        ```
- Improved replay step function
    - ```json
        {
        "Comment": "Replay ETL from DLQ (Fixed - State Preservation + Safe Errors)",
        "StartAt": "ReceiveFromDLQ",
        "States": {
            "ReceiveFromDLQ": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
            "Parameters": {
                "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                "MaxNumberOfMessages": 10
            },
            "ResultPath": "$.dlq",
            "Next": "CheckIfEmpty"
            },
            "CheckIfEmpty": {
            "Type": "Choice",
            "Choices": [
                {
                "Variable": "$.dlq.Messages",
                "IsPresent": false,
                "Next": "Success"
                }
            ],
            "Default": "ReplayBatch"
            },
            "ReplayBatch": {
            "Type": "Map",
            "ItemsPath": "$.dlq.Messages",
            "MaxConcurrency": 2,
            "Iterator": {
                "StartAt": "ExtractPayload",
                "States": {
                "ExtractPayload": {
                    "Type": "Pass",
                    "Parameters": {
                    "receiptHandle.$": "$.ReceiptHandle",
                    "body.$": "States.StringToJson($.Body)"
                    },
                    "ResultPath": "$.parsed",
                    "Next": "CheckRetry"
                },
                "CheckRetry": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                    "TableName": "file_processing_registry",
                    "Key": { "file_key": { "S.$": "$.parsed.body.file_key" } }
                    },
                    "ResultPath": "$.ddb",
                    "Next": "CheckAlreadySucceeded"
                },
                "CheckAlreadySucceeded": {
                    "Type": "Choice",
                    "Choices": [
                    {
                        "Variable": "$.ddb.Item.status.S",
                        "StringEquals": "SUCCESS",
                        "Next": "SkipReplay"
                    }
                    ],
                    "Default": "EvaluateRetry"
                },
                "EvaluateRetry": {
                    "Type": "Choice",
                    "Choices": [
                    {
                        "Variable": "$.ddb.Item.retry_count.N",
                        "StringLessThan": "3",
                        "Next": "IncrementRetry"
                    }
                    ],
                    "Default": "SkipReplay"
                },
                "IncrementRetry": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName": "file_processing_registry",
                    "Key": {
                        "file_key": {
                        "S.$": "$.parsed.body.file_key"
                        }
                    },
                    "UpdateExpression": "ADD retry_count :inc",
                    "ExpressionAttributeValues": {
                        ":inc": {
                        "N": "1"
                        }
                    }
                    },
                    "ResultPath": "$.incrementResult",
                    "Next": "RunGlueJob"
                },
                "RunGlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "ResultPath": "$.glueResult",
                    "Parameters": {
                    "JobName": "ingest_sales_data",
                    "Arguments": {
                        "--source_bucket.$": "$.parsed.body.bucket",
                        "--source_key.$": "$.parsed.body.key"
                    }
                    },
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "ResultPath": "$.error",
                        "Next": "UpdateFailure"
                    }
                    ],
                    "Next": "UpdateSuccess"
                },
                "UpdateSuccess": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName": "file_processing_registry",
                    "Key": {
                        "file_key": {
                        "S.$": "$.parsed.body.file_key"
                        }
                    },
                    "UpdateExpression": "SET #s = :s, updated_at = :t",
                    "ExpressionAttributeNames": {
                        "#s": "status"
                    },
                    "ExpressionAttributeValues": {
                        ":s": {
                        "S": "SUCCESS"
                        },
                        ":t": {
                        "S.$": "$$.State.EnteredTime"
                        }
                    }
                    },
                    "ResultPath": "$.updateSuccessResult",
                    "Next": "DeleteMessage"
                },
                "UpdateFailure": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName": "file_processing_registry",
                    "Key": {
                        "file_key": {
                        "S.$": "$.parsed.body.file_key"
                        }
                    },
                    "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
                    "ExpressionAttributeNames": {
                        "#s": "status"
                    },
                    "ExpressionAttributeValues": {
                        ":s": {
                        "S": "FAILED"
                        },
                        ":e": {
                        "S.$": "States.JsonToString($.error)"
                        },
                        ":t": {
                        "S.$": "$$.State.EnteredTime"
                        }
                    }
                    },
                    "ResultPath": "$.updateFailureResult",
                    "End": true
                },
                "SkipReplay": {
                    "Type": "Pass",
                    "Next": "DeleteMessage"
                },
                "DeleteMessage": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
                    "Parameters": {
                    "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                    "ReceiptHandle.$": "$.parsed.receiptHandle"
                    },
                    "End": true
                }
                }
            },
            "Next": "Success"
            },
            "Success": {
            "Type": "Succeed"
            }
        }
        }
      ```
    - It solves problems like
        - Stale message in DLQ causes duplicate data processing : If someone manually adds a DLQ message for a file that already has status = SUCCESS in DynamoDB — perhaps by mistake, or because the DLQ message was never cleaned up — replay_failed_ingestion will read the message, see retry_count = 0 which is less than 3, increment it to 1, and run Glue again against a file that was already successfully ingested. The file gets processed a second time, and duplicate data lands in the Silver layer.
        - Solution : add a ```CheckAlreadySucceeded``` state
            - Add a Choice state immediately after ```CheckRetry``` that checks status before checking ```retry_count```
            ```json
            "CheckRetry": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:getItem",
            "Parameters": {
                "TableName": "file_processing_registry",
                "Key": { "file_key": { "S.$": "$.parsed.body.file_key" } }
            },
            "ResultPath": "$.ddb",
            "Next": "CheckAlreadySucceeded"
            },
            "CheckAlreadySucceeded": {
            "Type": "Choice",
            "Choices": [
                {
                "Variable": "$.ddb.Item.status.S",
                "StringEquals": "SUCCESS",
                "Next": "SkipReplay"
                }
            ],
            "Default": "EvaluateRetry"
            },
            "EvaluateRetry": {
            "Type": "Choice",
            "Choices": [
                {
                "Variable": "$.ddb.Item.retry_count.N",
                "StringLessThan": "3",
                "Next": "IncrementRetry"
                }
            ],
            "Default": "SkipReplay"
            }
            ```
            - The flow becomes:
                - ```bash
                    CheckRetry → reads DynamoDB
                        ↓
                    CheckAlreadySucceeded
                        ↓ status = SUCCESS  →  SkipReplay → DeleteMessage (remove stale DLQ message)
                        ↓ status = FAILED   →  EvaluateRetry → check retry_count → IncrementRetry → RunGlueJob
                  ```
            - This means a stale DLQ message for an already-successful file gets silently cleaned up the message is deleted from the DLQ and nothing else happens. No duplicate Glue run, no duplicate data in Silver S3.
        - You might be wondering how is ```row_count``` being updated in dynamoDB table after the cause of failure in the pipeline is fixed and the pipeline successfully processed the file that previously failed to be processed. 
            - ```row_count``` in dynamoDB is being updated by AWS glue ETL job itself instead of the replay step function. hence this step function only updates the status from FAIL to SUCCESS.

#### AWS ETL pipeline 
- Add IAM role with appropriate permissions for AWS glue ETL to work properly
    - Create a role named ```AWSGlueRole``` and attach this role to AWS glue that you are creating.
    - Attached policies: 
        - Add AWS managed policy called ```AWSGlueServiceRole``` to this role
        - Add custom policy that was earlier made for Lambda function called ```DynamoDbAccessPolicyForLambdaFunction```
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DynamoDbAccessPolicyForLambdaFunction",
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:PutItem",
                        "dynamodb:GetItem",
                        "dynamodb:UpdateItem"
                    ],
                    "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                }
            ]
        }
        ```
        - Add this custom policy called ```LimitedS3PermissionPolicy```
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::aws-glue-s3-bucket-one",
                        "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                        "arn:aws:s3:::data-sink-one",
                        "arn:aws:s3:::data-sink-one/*"
                    ]
                }
            ]
        }
        ```
- improved code : 
    - ```python
        import sys
        from awsglue.transforms import *
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.gluetypes import *
        from awsgluedq.transforms import EvaluateDataQuality
        from awsglue import DynamicFrame
        import boto3

        from functools import reduce
        from pyspark.sql import functions as F

        def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
            for alias, frame in mapping.items():
                frame.toDF().createOrReplaceTempView(alias)
            result = spark.sql(query)
            return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
        def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
            if isinstance(schema, StructType):
                for field in schema:
                    new_path = path + "." if path != "" else path
                    output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
            elif isinstance(schema, ArrayType):
                if isinstance(schema.elementType, StructType):
                    output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
            elif isinstance(schema, NullType):
                output.append(path)
            else:
                x, distinct_set = frame.toDF(), set()
                for i in x.select(path).distinct().collect():
                    distinct_ = i[path.split('.')[-1]]
                    if isinstance(distinct_, list):
                        distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
                    elif isinstance(distinct_, str) :
                        distinct_set.add(distinct_.strip())
                    else:
                        distinct_set.add(distinct_)
                if isinstance(schema, StringType):
                    if distinct_set.issubset(nullStringSet):
                        output.append(path)
                elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
                    if distinct_set.issubset(nullIntegerSet):
                        output.append(path)
            return output

        def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
            nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
            return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'source_bucket',
            'source_key'
        ])

        source_bucket = args['source_bucket']
        source_key = args['source_key']

        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

        # Default ruleset used by all target nodes with data quality enabled
        DEFAULT_DATA_QUALITY_RULESET = """Rules = [
            ColumnCount = 16,

            IsComplete "rating",
            IsComplete "rating_count",
            IsComplete "discounted_price",
            IsComplete "actual_price",

            ColumnValues "rating" >= 0,
            ColumnValues "rating_count" >= 0,
            ColumnValues "discounted_price" >= 0,
            ColumnValues "actual_price" >= 0,
            ColumnValues "discount_percentage" >= 0,
            
            IsUnique "product_id"
        ]"""

        # -------- DELIMITER CORRUPTION CHECK -------- #
        # It checks for delimiter / structural corruption STARTS
        import csv
        import io

        raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").limit(1000).collect()

        if len(raw_lines) < 2:
            raise Exception("Glue Visual ETL | Corrupted CSV detected (file has no data rows)")

        def count_csv_columns(line: str) -> int:
            """Count columns correctly, respecting quoted fields containing commas."""
            try:
                return len(next(csv.reader(io.StringIO(line))))
            except StopIteration:
                return 0

        header_line = raw_lines[0][0]
        expected_col_count = count_csv_columns(header_line)

        bad_row_indices = []
        for i, row in enumerate(raw_lines[1:], start=2):
            line = row[0]
            actual_col_count = count_csv_columns(line)
            if actual_col_count != expected_col_count:
                bad_row_indices.append((i, actual_col_count, line[:80]))

        if bad_row_indices:
            details = "\n".join(
                [f"  Line {idx}: expected {expected_col_count} cols, got {actual} → {preview}..."
                for idx, actual, preview in bad_row_indices]
            )
            raise Exception(
                f"Glue Visual ETL | Corrupted CSV detected — {len(bad_row_indices)} row(s) have wrong column count "
                f"(likely semicolons used as delimiters instead of commas):\n{details}"
            )

        print(f"Glue Visual ETL | Column count validation passed: all rows have {expected_col_count} columns")
        # It checks for delimiter / structural corruption ENDS
        # -------- DELIMITER CORRUPTION CHECK -------- #

        # Script generated for node Raw data source S3
        RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE", "optimizePerformance": False}, connection_type="s3", format="csv", 
            # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
            connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
            transformation_ctx="RawdatasourceS3_node1772431168408")
        # for debugging only
        print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

        df = RawdatasourceS3_node1772431168408.toDF()

        print("Glue Visual ETL | DEBUG columns:", df.columns)

        # -------- CORRUPTION CHECK -------- #
        # checks Spark-level corruption after parsing STARTS

        # If Spark inferred no columns → corrupted file
        if len(df.columns) == 0:
            raise Exception("Glue Visual ETL | Corrupted CSV detected (no columns inferred)")

        expected_cols = len(df.columns)

        # If dataframe empty → corrupted file
        if df.limit(1).count() == 0:
            raise Exception("Glue Visual ETL | Corrupted CSV detected (empty dataframe)")

        # Row-level corruption detection
        null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]

        bad_rows = df.filter(
            reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7
        )

        if bad_rows.limit(1).count() > 0:
            raise Exception("Glue Visual ETL | Corrupted CSV detected (null-heavy rows)")

        # checks Spark-level corruption after parsing ENDS
        # -------- CORRUPTION CHECK -------- #

        #--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#
        # Script generated for node SQL Query
        SqlQuery61 = '''
        SELECT
            product_id,
            product_name,
            category,
            about_product,
            user_id,
            user_name,
            review_id,
            review_title,
            review_content,
            img_link,
            product_link,

            -- discounted_price: ₹149 → 149.0
            CAST(
                REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
                AS DOUBLE
            ) AS discounted_price,

            -- actual_price: ₹1,000 → 1000.0
            CAST(
                REGEXP_REPLACE(actual_price, '[^0-9.]', '')
                AS DOUBLE
            ) AS actual_price,

            -- discount_percentage: 85% → 85.0
            CAST(
                REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
                AS DOUBLE
            ) AS discount_percentage,

            -- rating: 3.9 → 3.9
            CAST(
                REGEXP_REPLACE(rating, '[^0-9.]', '')
                AS DOUBLE
            ) AS rating,

            -- rating_count: 24,871 → 24871
            CAST(
                REGEXP_REPLACE(rating_count, '[^0-9]', '')
                AS INT
            ) AS rating_count

        FROM myDataSource;
        '''

        try:
            SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")
        except Exception as e:
            print(f"Glue Visual ETL | SQL query execution failed | {e}")
            raise RuntimeError("Failing Glue job explicitly | Failed to execute sql query")
        #--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#

        # Script generated for node Drop Null Fields
        DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

        # -------- DEDUPLICATION STEP -------- #
        # This will check if there exists a row with a duplicate review_id if yes then it will drop one of the row and make sure that the row is unique
        # Doing this prevents any data duplication 
        df_clean = DropNullFields_node1772431857999.toDF()

        deduped_df = df_clean.dropDuplicates(["review_id"])

        print("Glue Visual ETL | Before dedup:", df_clean.count())
        row_count_duped_df = deduped_df.count()
        print("Glue Visual ETL | After dedup:", row_count_duped_df)

        deduped_dynamic_frame = DynamicFrame.fromDF(
            deduped_df, glueContext, "deduped_dynamic_frame"
        )
        # -------- DEDUPLICATION STEP -------- #

        # Script generated for node Silver layer data sink S3
        try:
            # BEST_EFFORT is that it publishes DQ metrics to CloudWatch but does not guarantee throwing an exception when rules fail
            # EvaluateDataQuality().process_rows(frame=deduped_dynamic_frame, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
            
            # FAIL_JOB tells Glue to fail the job immediately when any rule is violated. failureAction: FAIL makes the violation propagate as an exception
            EvaluateDataQuality().process_rows(
                frame=deduped_dynamic_frame,
                ruleset=DEFAULT_DATA_QUALITY_RULESET,
                publishing_options={
                    "dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053",
                    "enableDataQualityResultsPublishing": True,
                    "failureAction": "FAIL"
                },
                additional_options={
                    "dataQualityResultsPublishing.strategy": "STRICT",
                    "observations.scope": "ALL"
                }
            )
        except Exception as e:
            print("Glue Visual ETL | DQ failed:", e)
            raise RuntimeError("Failing Glue job explicitly | Failed to run data quality rules")

        SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
        SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
        SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
        SilverlayerdatasinkS3_node1772432020219.writeFrame(deduped_dynamic_frame)

        # -------- WRITE ROW COUNT TO DYNAMODB -------- #
        # Written after writeFrame so row_count only lands in DynamoDB
        # if the data was actually written to Silver S3 successfully
        file_key = f"s3://{source_bucket}/{source_key}"
        dynamodb_client = boto3.client("dynamodb", region_name="ap-south-1")
        dynamodb_client.update_item(
            TableName="file_processing_registry",
            Key={"file_key": {"S": file_key}},
            UpdateExpression="SET row_count = :r",
            ExpressionAttributeValues={":r": {"N": str(row_count_duped_df)}}
        )
        print(f"Glue Visual ETL | Row count {row_count_duped_df} written to DynamoDB for {file_key}")
        # -------- WRITE ROW COUNT TO DYNAMODB -------- #

        job.commit()
      ```
    - GAPs that are fixed in this version of glue code 
        - **Data quality strategy changed from ```BEST_EFFORT``` to ```STRICT```**
            - The original code used ```BEST_EFFORT``` as the publishing strategy for ```EvaluateDataQuality```. The problem with ```BEST_EFFORT``` is that it only publishes data quality metrics to CloudWatch it logs that rules failed but does not stop the job.
            - This means a file that violates ```ColumnCount = 16``` or ```IsUnique "product_id"``` would have its metrics published as a warning, and the job would continue writing the bad data to the Silver S3 layer without any indication that something was wrong downstream.
            - ```STRICT``` changes this behaviour entirely. When any rule in the ruleset fails, ```STRICT``` immediately raises an exception inside ```EvaluateDataQuality```. This exception is caught by the ```try/except``` block which then raises RuntimeError explicitly. The Glue job exits with a failed status, the Step Function's Catch block detects the failure, ```UpdateFailure``` writes ```status = FAILED``` and the error to DynamoDB, and ```SendToDLQ``` puts the event payload into sales-ingestion-dlq. The data engineer is alerted and the bad data never reaches the Silver layer.
            - The intention is that data quality violations are treated as hard failures, not warnings. A pipeline that silently ingests bad data is worse than a pipeline that fails loudly silent bad data corrupts downstream dashboards and analytics without anyone knowing.
        - **```dropDuplicates()``` changed to ```dropDuplicates(["review_id"])```**
            - The original code used ```dropDuplicates()``` with no arguments. This tells Spark to compare every single column across every single row to find duplicates — all 16 columns, for every row in the DataFrame. To do this Spark has to hash all 16 column values for each row, shuffle the entire dataset across the cluster so rows with the same hash land on the same partition, and then compare them. For a small file this overhead is negligible but the approach is also semantically wrong.
            - ```review_id``` is the natural business key for this dataset. It is the unique identifier assigned to each review. If two rows have the same ```review_id``` they are the same review regardless of what the other 15 columns say. The DQ ruleset already enforces ```IsUnique "product_id"``` but ```review_id``` is the correct key for row-level deduplication since each row represents one review.
            - ```dropDuplicates(["review_id"])``` tells Spark to deduplicate on this one column only. Spark hashes only ```review_id```, partitions by that hash, and compares within partitions. This is faster because it hashes one value instead of 16, and it is semantically correct because you are deduplicating on the business identity of the record rather than an arbitrary combination of all its attributes.
        - **```collect()``` limit changed from no limit to ```limit(1000)```**
            - The delimiter corruption check works by reading the CSV file as raw text line by line, counting the number of comma-separated values in each row using Python's csv.reader, and comparing each row's column count against the header's column count. If any row has a different count it is flagged as corrupted.
            - The original code called ```.collect()``` with no limit, which loads the entire file into the Glue driver's memory as a Python list. For my current sales CSV files with 489 rows this is harmless. But if this same ETL pipeline were ever pointed at a larger file say a 500MB file with hundreds of thousands of rows.```collect()``` would attempt to load the entire file into the driver's 10GB memory allocation. This is not guaranteed to fail but it is an unnecessary risk.
            - ```limit(1000)``` tells Spark to read only the first 1000 rows before collecting to the driver. For corruption detection this is entirely sufficient structural corruption like wrong delimiters or extra columns almost always appears consistently throughout a file, not just in rows beyond row 1000. The trade-off is that a file with corruption starting at row 1001 would pass this check, but that scenario is extremely unlikely in practice and the Spark-level corruption checks that follow would still catch it.
            - The choice of 1000 rather than 10 is deliberate. An earlier version of the code used ```limit(10)``` which was inadequate your own ```sales_data_4.csv``` had corruption starting at line 116, which ```limit(10)``` would have missed entirely. 1000 provides meaningful coverage of the file while keeping driver memory usage bounded regardless of file size.
        - **Glue ETL job will now write the number of rows processed in each file directly to DynamoDB**
            - The boto3 ```update_item``` call inside your Glue script connects to DynamoDB just like any other AWS service call. Glue runs on an EC2-backed cluster that carries the IAM role you attached ```AWSGlueRole```. As long as that role has d```ynamodb:UpdateItem``` permission on ```file_processing_registry```, the call goes through without any intermediary.
            - The Step Function is completely uninvolved in this. It is sitting and waiting for the Glue job to finish — it has no idea what is happening inside the script. The DynamoDB write happens silently inside the Glue process while the Step Function just sees "Glue job is running".
            - This is a common pattern in data engineering using boto3 inside a Glue script to write metadata, audit logs, or metrics to DynamoDB, S3, or other AWS services as a side effect of the main processing job. Glue is just a Python process running on AWS infrastructure with an IAM role, so it can call any AWS service that the role has permission for.
            - Why I decided to do this ?
                - Row count is the simplest and most important data quality signal after successful ingestion. Without it, status = SUCCESS tells you the pipeline ran without errors — but it tells you nothing about whether the right amount of data actually landed.
                - Consider three scenarios that ```status = SUCCESS``` cannot distinguish between. A file arrives with 489 rows and 489 rows land in Silver this is correct. A file arrives with 489 rows but a bug in the deduplication logic removes 400 of them as false duplicates this is a silent data loss that ```SUCCESS``` does not catch. A file arrives completely empty due to an upstream system error but passes all validation checks because an empty file with correct headers is technically valid this is also ```SUCCESS```.
                - ```row_count``` in DynamoDB gives you one line of defence against all three scenarios. A data engineer scanning the table can immediately see that ```sales_data_2.csv``` landed 489 rows while ```sales_data_4.csv``` only landed 15 rows — which is correct because ```sales_data_4.csv``` only had 15 data rows. If ```sales_data_1.csv``` suddenly showed ```row_count = 0``` on a day when it normally shows 489, that is an immediate signal that something is wrong upstream before anyone has to query Athena or check S3.
    - Explaination :  ```BEST_EFFORT``` and ```STRICT``` are values for ```dataQualityResultsPublishing.strategy```. They control how and when results are published to CloudWatch.
        - ```BEST_EFFORT``` : publish results whenever possible, even if publishing itself fails. If the DQ evaluation encounters an internal error while trying to write metrics to CloudWatch, it swallows that error and lets the job continue. It is tolerant of publishing failures. If DQ rules are violated it still publishes the failure metrics but the job continues unless failureAction: "FAIL" is also set.
        - ```STRICT``` : publish results and if publishing itself fails, treat that as a hard error. If Glue cannot write DQ metrics to CloudWatch for any reason, it raises an exception immediately. It is intolerant of publishing failures. Like BEST_EFFORT, if DQ rules are violated it publishes the failure metrics but the job continues unless failureAction: "FAIL" is also set.
        - ```FAIL_JOB``` : does not exist as a valid strategy value on Glue 5.0. The valid values for dataQualityResultsPublishing.strategy on your runtime are STRICT and BEST_EFFORT only. This was an incorrect suggestion based on documentation that did not match your actual Glue version.
    - ```python
        "failureAction": "FAIL",                           # rule violation → exception raised
        "dataQualityResultsPublishing.strategy": "STRICT"  # publishing failure → exception raised
      ```
        - This means two things will cause your job to fail a DQ rule violation, and a failure to publish DQ results to CloudWatch. Both are treated as hard errors. This is the most defensive combination for a production pipeline where you want to know immediately if anything goes wrong, whether it is bad data or a monitoring infrastructure problem.

#### DynamoDB setup
- The table inside this dynamoDB is named ```file_processing_registry``` where I will maintain records that will allow us to track which files are processed successfully , in_progress or has failed to process by AWS glue ETL pipeline.
- ![dynamoDB_1](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_1.png)
- ![dynamoDB_2](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_2.png)
- ![dynamoDB_3](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_3.png)
- ![dynamoDB_4](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_4.png)
- ![dynamoDB_5](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_5.png)
- ![dynamoDB_6](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_6.png)
- After you create your table you will see something like this 
    - ![dynamoDB_8](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_8.png)
    - Now you need to turn ttl settings on so that the ttl attribute that is being added by the lambda function in this table can actually take effect otherwise the records will remain in the table forever and the purpose of adding the ttl column in the table by lambda function will be defeated
    - ![dynamoDB_10](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_10.png)
    - Here in this screen shot you can see the option to turn off the ttl setting this is because in my case the ttl is already on for this table but when you come to this option then here you will see the option to turn the ttl on instead of turn it off
    - ![dynamoDB_9](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_9.png)
    - Once everything is said and done you must see this 
    ![dynamoDB_11](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_11.png)
    - Now the ttl column will be used to delete the records that expires based on the value in unix timestamp that this column has and it will be deleted permanently.
- When a new file is processed by the ETL pipeline the data is inserted in dynamoDB looks something like this 
    - ![dynamoDB_7](images/production_grade_implementation_version_6/DynamoDB/dynamoDB_7.png)
    
### Issues faced during the Implementation for version 6:
#### ETL was not being invoked on file arrival in S3 bucket 
- Before I explain why this was happening I want to show you something
    - ![pending_messages_sqs_blockage](images/production_grade_implementation_version_6/SQS/pending_messages_sqs_blockage.png)  
- Broken pipeline diagnosis
    - ![pipeline_broken_chain_diagnosis_sqs](images/production_grade_implementation_version_6/SQS/pipeline_broken_chain_diagnosis_sqs.svg)
    - This is the problem. ```FileProcessingQueue.fifo``` shows Messages available: 11 and Messages in flight: 1. These are stale messages from all your previous test uploads during cleanup. With a 30 minute visibility timeout, each time Lambda picks up a message and partially processes it, the message becomes invisible for 30 minutes before reappearing. Your new upload is just adding to an already-backed-up queue.
- Solution : (Do not apply this in production)
    - The fix is one click — go to ```SQS → FileProcessingQueue.fifo → click the Purge button``` at the top of the page. This instantly deletes all 12 stuck messages. Then re-upload your CSV file and the pipeline will run immediately.
#### Permission related error AWS glue ETL pipeline was not allowed to write in DynamoDB
Error message : 
```bash
Error Category: PERMISSION_ERROR; Failed Line Number: 272; ClientError: An error occurred (AccessDeniedException) when calling the UpdateItem operation: User: arn:aws:sts::406868976171:assumed-role/AWSGlueRole/GlueJobRunnerSession is not authorized to perform: dynamodb:UpdateItem on resource: arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry because no identity-based policy allows the dynamodb:UpdateItem action
```
- Add this custom policy named ```DynamoDbAccessPolicyForLambdaFunction``` to the role that was created earlier for AWS glue ETL pipeline ```AWSGlueRole```
    - ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DynamoDbAccessPolicyForLambdaFunction",
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:PutItem",
                        "dynamodb:GetItem",
                        "dynamodb:UpdateItem"
                    ],
                    "Resource": "arn:aws:dynamodb:ap-south-1:406868976171:table/file_processing_registry"
                }
            ]
        }
      ```

### DataBricks vs AWS (implementation version 6) comparision:
Comparision diagram : 
![comparision_aws_vs_databricks](images/production_grade_implementation_version_6/comparision_aws_vs_databricks/comparision_aws_vs_databricks.svg)

#### How my Version 6 pipeline would look on Databricks?
Every component in my AWS architecture has a Databricks equivalent, but the amount of custom code I wrote shrinks dramatically because Databricks packages much of what I built manually into platform features.

**File arrival trigger** : In AWS you built a four-component chain: ```S3 → EventBridge → SQS FIFO → Lambda``` just to detect a new file and pass its location downstream. On Databricks this is replaced by a single feature called Auto Loader. Auto Loader uses S3 event notifications or file listing internally and automatically detects new files in an S3 path. You point it at the folder and it handles everything deduplication of events, backpressure, checkpointing with two lines of code:
```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load("s3://aws-glue-s3-bucket-one/raw_data/sales_data/")
```

**Idempotency** : My Lambda spent significant effort building a two-step idempotency check with DynamoDB conditional writes to prevent duplicate processing. On Databricks this is solved by Delta Lake's ```MERGE INTO``` statement. Delta Lake maintains a transaction log for every table. When I write using merge on the business key, it is mathematically impossible to create a duplicate row the table format itself enforces exactly-once semantics without any external state store:
```python
delta_table.alias("target").merge(
    new_data.alias("source"),
    "target.review_id = source.review_id"
).whenNotMatchedInsertAll().execute()
```
The entire DynamoDB ```file_processing_registry``` table, the Lambda idempotency logic, and the conditional put_item complexity disappear. Delta handles it.

**Orchestration** : My Step Functions JSON state machines ```orchestrate_data_ingestion``` and ```replay_failed_ingestion``` would be replaced by Databricks Workflows. I define a DAG of tasks, each task being a notebook or Python script. Retry logic, alerting on failure, email notifications, and dependency management are all configured in the UI or as YAML. The ```replay_failed_ingestion``` step function with its ```CheckAlreadySucceeded```, ```EvaluateRetry```, ```IncrementRetry``` states would collapse into a simple retry configuration on the workflow task.

**ETL transformation** : My Glue PySpark script runs identically on Databricks. The transformation logic ```REGEXP_REPLACE```, ```dropDuplicates(["review_id"])```, the corruption checks is identical PySpark and moves without changes. The main operational difference is that Databricks clusters are warm and reusable. My Glue job has a cold start of approximately 2 minutes every run because it spins up a fresh cluster each time. On Databricks we use a job cluster that starts for each run, or an interactive cluster that is always running and executes jobs in seconds.

**Data quality** : My ```EvaluateDataQuality``` ruleset with ```STRICT``` strategy would be replaced by Delta Live Tables expectations. These are Python decorators on your transformation functions:
```python
@dlt.expect_or_fail("valid column count", "length(product_id) > 0")
@dlt.expect_or_fail("no duplicate reviews", "review_id IS NOT NULL")
def silver_sales():
    return spark.read...
```
The three modes expect logs violations, ```expect_or_drop``` removes bad rows, ```expect_or_fail``` fails the pipeline map directly to what my ```STRICT``` strategy does.

**Catalog and querying** : My Glue Catalog plus Athena would be replaced by Unity Catalog with Databricks SQL. Unity Catalog adds column-level access control, data lineage tracking showing where each column came from and where it flows to, and a centrally governed namespace. Databricks SQL replaces Athena for querying.

**Cost comparison** : 
![cost_comparison_chart](images/production_grade_implementation_version_6/comparision_aws_vs_databricks/cost_comparison_chart.svg)

Cost breakdown — 1000 files/day : 

AWS Cost Breakdown — Version 6 ($6,646/month)

| Service           | What it does                                           | Monthly cost |
|------------------|---------------------------------------------------------|--------------|
| AWS Glue ETL     | 10 workers × 3 min × 1000 files × $0.44/DPU-hr          | $6,600       |
| S3               | Source + Silver storage, ~51GB + requests               | $38          |
| Step Functions   | 200 executions × 32 transitions                         | $5           |
| Lambda           | 1000 invocations × 512MB × 10s                          | $3           |
| CloudWatch       | 184MB log ingest                                        | $0.14        |
| DynamoDB         | 2000 writes + 1000 reads                                | $0.09        |
| SQS FIFO         | 3000 requests                                           | $0.04        |
| EventBridge      | 1000 events                                             | $0.03        |
| **Total**        |                                                         | **$6,646**   |

Glue is 99% of your entire AWS bill. Every other service combined costs $46/month. This is the defining characteristic of a Glue-heavy architecture the compute dominates everything else completely.

Compute Only: $5,847/month VS With Licence: $8,847/month  

| Component                | What it does                                      | Monthly cost |
|--------------------------|---------------------------------------------------|--------------|
| Jobs compute (EC2 + DBU) | 11 nodes × 2 min × 1000 files                    | $5,764       |
| Platform licence (Premium) | Unity Catalog, DLT, Workflows, support         | $3,000       |
| S3                       | Same storage as AWS side                         | $38          |
| Databricks SQL           | 100 analyst queries/day                          | $44          |
| Workflows, Unity Catalog | Included in licence                              | $0           |
| **Total (Compute Only)** |                                                   | **$5,847**   |
| **Total (With Licence)** |                                                   | **$8,847**   |

The compute saving against Glue is $836/month because Databricks runs the same Spark workload slightly more efficiently and without the 2-minute cold start overhead that Glue charges for. But the platform licence of $3,000/month turns that saving into a net loss of $2,200/month versus AWS at this scale.

**The honest crossover point**
At 1,000 files per day Databricks costs $2,200 more per month than AWS. The licence cost is the entire reason. The pure compute cost of Databricks is actually cheaper.

The crossover happens in two scenarios. The first is when your Glue bill grows past $9,000/month — roughly 1,400 files/day with your current configuration — because at that point Databricks compute efficiency and Photon start closing the gap faster than the licence cost widens it. The second and more common scenario is when you add data scientists, ML workflows, and interactive analysis to the platform. If five data scientists are running notebooks daily, that usage is essentially free on Databricks once you have the licence, but on AWS it would mean additional SageMaker or EMR costs that could easily exceed the licence gap.

**How to achieve Unity Catalog features on AWS**
Unity Catalog gives you three things — column-level access control, data lineage, and a centrally governed namespace. Here is how to get each one on AWS.

**Column-level access control** is provided by AWS Lake Formation. Lake Formation sits on top of the Glue Catalog and S3 and lets you define permissions at the database, table, column, and row level. You grant a user access to a table but exclude specific columns — for example allowing analysts to query the sales table but never see user_id or personal information. These permissions are enforced consistently across Athena, Glue, EMR, and Redshift Spectrum. Setting it up requires registering your S3 data lake with Lake Formation, migrating permissions from raw S3 bucket policies to Lake Formation grants, and enabling fine-grained access control in Athena.

**Data lineage** is provided by Amazon DataZone combined with AWS Glue Data Catalog lineage features. Glue can automatically track which jobs read from which tables and write to which tables, building a lineage graph you can visualise in the console. DataZone extends this into a full data catalogue with business metadata, data discovery, and lineage across the entire organisation. For your specific pipeline, the lineage is already partially captured — your DynamoDB file_processing_registry tracks which files were processed and when, and the Glue Catalog knows which jobs created which tables. The gap is that you cannot currently trace a specific column value back to its origin file automatically.

**Centrally governed namespace** is provided by the Glue Data Catalog itself acting as the central metastore, with Lake Formation providing the governance layer on top. Every table registered in the Glue Catalog is discoverable by any service — Athena, Glue, EMR, Redshift Spectrum — through the same namespace. The difference from Unity Catalog is that AWS requires you to configure Lake Formation explicitly for governance while Databricks makes it the default from day one.

TODO FIX this 
Gap 1 — CloudWatch alerting is documented but never implemented
This was Gap 6 from the original list and it appears in the README as a TODO but there is no implementation section for it anywhere in Version 6. You have no automated alerting if the DLQ grows, if Glue jobs fail repeatedly, or if the Step Function starts failing. In production this means a silent failure — files stop landing in Silver S3 and nobody knows until someone manually checks the dashboard.

## Some theory that needs to be covered before moving to version 7 implementation
## Schema evolution implementation on AWS (Databricks equivalent)
### What Delta Lake schema evolution actually does under the hood?
Delta Lake maintains a transaction log a folder called ```_delta_log``` that stores the history of every operation on the table as JSON files. The schema is stored in this log, not in the parquet files themselves. When you write with ```mergeSchema=true```, Delta writes a new entry to the log that says "the schema is now this new wider schema." The old parquet files are untouched. At read time, Delta's reader looks at the current schema from the log and when it reads an old parquet file that is missing the new column, it fills in nulls for that column in memory. No data rewrite happens.

This allows databricks to add new columns to a table without rewriting existing data files and without breaking existing queries.

### The AWS Glue Catalog approach (the closest native equivalent)
My pipeline already uses the Glue Catalog to register your Silver table ```silver_table_sales_data```

The Glue Catalog stores table schemas and Glue has built-in schema evolution support through the ```updateBehavior``` parameter on the sink node.

In my current Glue ETL script you already have this:
```python
SilverlayerdatasinkS3 = glueContext.getSink(
    path="s3://data-sink-one/silver_layer/sales_data/",
    connection_type="s3",
    updateBehavior="LOG",   # ← this is the key parameter
    partitionKeys=["category"],
    enableUpdateCatalog=True,
    transformation_ctx="SilverlayerdatasinkS3"
)
```
The updateBehavior parameter controls exactly what happens when the incoming data has a different schema from what is registered in the Glue Catalog. 

There are three options.
- ```"LOG"``` the current setting : 
    -  logs schema mismatches as warnings but does not fail the job and does not update the catalog schema. New columns in the incoming data are silently dropped. This is actually the worst behaviour for schema evolution because you lose data silently.
- ```"UPDATE_IN_DATABASE"``` : 
    -  this is what you want for schema evolution. When the incoming data has new columns that do not exist in the Glue Catalog table, Glue automatically adds those columns to the catalog schema. Existing parquet files are untouched. New parquet files contain the new columns. Athena handles the null-backfilling at read time for old files that predate the new column. This maps directly to what Delta's mergeSchema=true does.

changes in the current AWS Glue script: 
```python
SilverlayerdatasinkS3 = glueContext.getSink(
    path="s3://data-sink-one/silver_layer/sales_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  # ← changed from LOG
    partitionKeys=["category"],
    enableUpdateCatalog=True,
    transformation_ctx="SilverlayerdatasinkS3"
)
SilverlayerdatasinkS3.setCatalogInfo(
    catalogDatabase="aws-glue-tutorial-aditya",
    catalogTableName="silver_table_sales_data"
)
SilverlayerdatasinkS3.setFormat("glueparquet", compression="snappy")
SilverlayerdatasinkS3.writeFrame(deduped_dynamic_frame)
``` 
With ```UPDATE_IN_DATABASE``` Glue does the following when it writes:
1. Compares the incoming DataFrame schema against the current Glue Catalog schema
2. Identifies new columns in the incoming data
3. Updates the Glue Catalog table definition to include the new columns
4. Writes the new parquet files with the new columns included
5. Old parquet files remain untouched they simply do not have the new column physically present

When Athena queries the table after a schema evolution event, it reads the updated schema from the Glue Catalog and handles missing columns in old parquet files by returning null for those rows. Exactly the same behaviour as Delta Lake.

## How downstream applications actually connect to your data? and what are the effects of schema evolution on them ?
The short answer is that downstream applications almost never read parquet files directly from S3. They connect to a query engine that sits in front of S3 and abstracts the storage details away. The query engine reads the Glue Catalog for the schema, finds the S3 paths for the data, reads the parquet files, and returns results. The downstream application only ever sees SQL query results it has no idea whether the data lives in parquet, CSV, or any other format.

The chain looks like this:
```bash
parquet files in S3
        ↓
Glue Catalog (schema + S3 location metadata)
        ↓
Query engine (Athena / Redshift Spectrum)
        ↓
Downstream application (QuickSight / BI tool / another Glue job)
```
Each layer only knows about the layer directly above it. QuickSight talks to Athena using SQL. Athena talks to the Glue Catalog to understand the schema. The Glue Catalog points Athena to the S3 paths. Athena reads the parquet files. QuickSight never knows S3 exists.

### How each downstream application connects?
**Amazon QuickSight** connects to your data through a data source connection. When you set up QuickSight you choose a data source type Athena is the most common for data lake pipelines like yours. You give QuickSight the Athena workgroup, the Glue database name, and the table name. QuickSight then issues SQL queries to Athena on your behalf whenever a dashboard is loaded or refreshed. It does not read S3 directly. It does not read the Glue Catalog directly. It sends SQL to Athena and receives a result set.

Internally QuickSight has two modes. Direct Query mode sends a live SQL query to Athena every time someone opens a dashboard. SPICE mode imports the query result into QuickSight's own in-memory cache and serves dashboards from that cache. SPICE is faster for dashboards but requires a scheduled refresh to pick up new data.

**Another Glue ETL job** your Gold layer job connects to your Silver table through the Glue Catalog directly. In the Glue script you would read from the catalog table like this:
```python
silver_df = glueContext.create_dynamic_frame.from_catalog(
    database="aws-glue-tutorial-aditya",
    table_name="silver_table_sales_data"
)
```
Glue looks up the table in the Glue Catalog, finds the S3 path ```s3://data-sink-one/silver_layer/sales_data/```, finds the schema, and reads the parquet files. The Gold layer job transforms the Silver data into aggregated analytics-ready data and writes it to a Gold S3 path, registering a new table in the Glue Catalog.

**Redshift Spectrum** connects to the Glue Catalog through an external schema. You run a one-time SQL command in Redshift:
```sql
CREATE EXTERNAL SCHEMA silver_data
FROM DATA CATALOG
DATABASE 'aws-glue-tutorial-aditya'
IAM_ROLE 'arn:aws:iam::406868976171:role/RedshiftRole';
```
After that, Redshift users can query silver_data.```silver_table_sales_data``` using SQL directly in Redshift without loading any data into Redshift storage. Redshift Spectrum reads the parquet files from S3 at query time through the Glue Catalog.

Apache Superset, Tableau, Power BI, or any other BI tool connects through an ODBC or JDBC driver for Athena. The tool connects to Athena as if it were a database, sends SQL queries, and receives results. The BI tool has no awareness of S3 or parquet.

### What actually happens during schema evolution for each downstream application?
What happens to each downstream consumer when my Glue ETL job adds a new column to the silver table using ```UPDATE_IN_DATABASE```.

#### Athena
Athena reads the schema from the Glue Catalog at query time not at connection time. Every time a query runs, Athena fetches the current schema from the Glue Catalog and uses it to interpret the parquet files.

When a new column is added:

Old parquet files physically do not contain the new column. When Athena reads these files and the schema says the column exists, Athena returns null for that column for every row in those old files. No error. No job failure. Just nulls for old rows, real values for new rows.
```sql
-- Before schema evolution: 16 columns
SELECT product_id, rating FROM silver_table_sales_data;
-- Works fine, returns data

-- After a new column called "seller_id" is added
SELECT product_id, rating, seller_id FROM silver_table_sales_data;
-- Works fine
-- Old rows: seller_id = null
-- New rows: seller_id = actual value
```
Athena is fully forward and backward compatible with schema evolution on parquet files because parquet stores column names inside each file. When Athena reads an old parquet file and asks for a column that does not exist in that file, it returns null rather than failing.

#### QuickSight in Direct Query mode
QuickSight sends SQL to Athena. Since Athena handles the schema evolution transparently, QuickSight is also unaffected. The new column will appear in the dataset automatically the next time QuickSight refreshes its dataset schema from Athena.

However there is a subtlety. QuickSight caches the list of available columns in its dataset definition. If you added a new column to the Silver table, QuickSight does not automatically add that column to existing datasets. You need to go to ```QuickSight → Datasets → your dataset → Edit → Sync ```with Athena to pull in the new column. Existing dashboards that do not reference the new column continue working without any change. Dashboards that want to use the new column need the dataset to be refreshed first.

#### QuickSight in SPICE mode
SPICE mode imports data into QuickSight's cache. The cache contains only the columns that existed when the last SPICE refresh ran. If a new column is added to your Silver table after the last SPICE refresh, SPICE does not know about it. The new column will appear only after the next scheduled or manual SPICE refresh. Until then dashboards see the old cached data without the new column.

This is the one downstream consumer where schema evolution requires explicit action a SPICE refresh before the new column is available.

#### Another Glue ETL job reading Silver
If your Gold layer Glue job reads from the Silver Glue Catalog table, it reads the current schema from the catalog at job start time. After schema evolution, the next run of the Gold layer job will see the new column in the Silver table automatically.

The question is what the Gold layer job does with the new column. If the job explicitly selects specific columns:
```python
gold_df = silver_df.select("product_id", "category", "rating", "discounted_price")
```
The new column is simply ignored it is never selected and never propagates to the Gold layer. Your Gold layer job keeps working as before without any change.

If the Gold layer job selects all columns with ```select("*")``` or passes the DataFrame through without explicit column selection, the new column automatically flows through to the Gold layer. Whether this is desirable depends on your intent.

#### Redshift Spectrum
Redshift Spectrum reads the schema from the Glue Catalog at query time, same as Athena. New columns appear automatically in the external table. Old rows return null for the new column. No action required. Existing queries that do not reference the new column continue working identically.

### The real risk downstream queries that break
Schema evolution can break downstream consumers in two specific situations
#### Situation 1 : A column is renamed
Say your upstream system renames discounted_price to sale_price in the CSV files. Your Glue ETL job with UPDATE_IN_DATABASE sees a new column called sale_price and an old column called discounted_price that is no longer being populated. It adds sale_price to the Glue Catalog schema. Old parquet files have discounted_price with real values. New parquet files have sale_price with real values.

Now any Athena query or QuickSight dashboard that references discounted_price will return nulls for all new rows because new files only have sale_price. Any downstream aggregation that sums discounted_price across old and new data will produce wrong results silently — no error, just incorrect numbers. This is the most dangerous failure mode in schema evolution because the pipeline succeeds, the data lands in Silver, QuickSight shows a dashboard, but the numbers are wrong.

#### Situation 2 : A column type changes incompatibly
Say rating_count changes from string to integer in the upstream CSV. Your Glue ETL casts it to INT in the SQL transformation, same as before. But old parquet files stored rating_count as INT and the Glue Catalog says INT. New parquet files also say INT. No conflict here your transformation handles the casting before writing. This scenario is actually safe in your pipeline because you explicitly cast all types in the SQL transformation layer before writing to Silver.

The dangerous version is if you were writing the raw CSV schema to Silver without transformation. Then a type change in the upstream CSV would create a type conflict in the Glue Catalog.

## How to protect downstream applications from schema evolution surprises?
The correct production approach is to separate schema evolution handling into explicit tiers.
- Bronze layer : accept everything
    - The Bronze layer stores raw CSV files exactly as received with no transformation. Schema evolution in the source is recorded here automatically. Bronze is schema-agnostic by design.
- Silver layer : enforce a contract
    - The Silver layer enforces a stable schema contract. Your DQ rule ```ColumnCount >= 16``` catches files with fewer columns than expected. Your SQL transformation explicitly selects and casts specific named columns so even if the upstream CSV adds 5 new columns, your Silver layer only contains the 16 columns you explicitly selected. New columns in the source do not automatically flow to Silver unless you deliberately add them to the SQL SELECT statement.

    - This is actually a schema evolution protection that your pipeline already has by accident. Your SQL query explicitly names every column:
    ```sql
        SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        ...
        FROM myDataSource;
    ```
    - Any new column in the CSV that is not in this SELECT statement is silently dropped before writing to Silver. Your Silver schema is controlled entirely by this SQL query, not by whatever the upstream sends. This is the correct design for a production Silver layer.
- Gold layer : stable aggregations for dashboards
    - The Gold layer further transforms Silver into specific aggregated tables built for specific business questions. Even if Silver schema evolves, the Gold layer SQL explicitly selects the columns it needs for each aggregation. QuickSight connects to Gold tables, not Silver. Gold tables change only when you deliberately update the Gold layer ETL job, not when the Silver schema changes.
    - This three-layer isolation is why the Medallion architecture is the standard for production data platforms. Each layer acts as a firewall that absorbs upstream changes before they reach downstream consumers. Your upstream can change schemas freely, Bronze absorbs everything, Silver normalises and contracts the schema, Gold serves stable aggregations, and QuickSight dashboards never break from upstream changes.

## Implementation Version 7
- The dataset in this pipeline has changed. In this pipeline I am using csv files generated by sonar qube. 

### The final architecture I settled on 
```bash
                               ┌──────────────────────────────┐
                               │       Upstream System        │
                               │       (SonarQube Logs)       │
                               └──────────────┬───────────────┘
                                              │ 1. Uploads JSON/CSV
                                              ▼
                               ┌──────────────────────────────┐
                               │       Source S3 Bucket       │
                               │ 'deployement-logs-data-source'
                               └──────────────┬───────────────┘
                                              │ 2. Object Created Event
                                              ▼
                               ┌──────────────────────────────┐
                               │         EventBridge          │
                               │  'sonar_qube_event_rules'    │
                               └──────────────┬───────────────┘
                                              │ 3. Routes Event
                                              ▼
                               ┌──────────────────────────────┐
                               │        SQS FIFO Queue        │
                               │ 'DataProcessingJobQueue.fifo'│
                               └──────────────┬───────────────┘
                                              │ 4. Triggers (Batch)
┌───────────────────────────┐                 ▼
│         DynamoDB          │  ┌──────────────────────────────┐
│ 1. 'sonar_qube_logs_      │◄─┤       Lambda Function        │
│    processing_registery'  │  │ 'TriggerProcessSonarQubeLog- │
│    (Idempotency Check)    │  │  StepFunction'               │
│ 2. 'glue_semaphore'       │  └──────────────┬───────────────┘
│    (Concurrency Lock)     │                 │ 5. Starts Execution
└─────────────▲─────────────┘                 ▼
              │                ┌──────────────────────────────┐ 6b. Fails ┌──────────────────────────┐
              │                │        Step Function         ├──────────►│       SQS FIFO DLQ       │
              └────────────────┤ 'process_sonar_qube_step_    │           │ 'DataProcessingDLQ.fifo' │
        8. Updates status &    │  function'                   │           └────────────┬─────────────┘
           releases lock       └──────────────┬───────────────┘                        │
                                              │ 6a. Starts Sync                        │ 9. Manual Invoke
                                              ▼                                        ▼
                               ┌──────────────────────────────┐           ┌──────────────────────────┐
                               │        AWS Glue ETL          │           │     Lambda Function      │
                               │ 'process_sonar_qube_logs_ETL'│           │ 'ProcessDLQMessagesOn-   │
                               └──────────────┬───────────────┘           │  DemandLamdbaFunction'   │
                                              │ 7. Writes Data            └────────────┬─────────────┘
                                              ▼                                        │
                               ┌──────────────────────────────┐                        │ 10. Re-starts
                               │   Silver S3 & Glue Catalog   │                        │     Execution
                               │ 'deployement-logs-data-silver'◄───────────────────────┘
                               └──────────────────────────────┘
```

### Implementation steps of version 7 pipeline 

### Source S3 bucket setup 
![create_source_s3_bucket_1](images/production_grade_implementation_version_7/S3_source/create_source_s3_bucket_1.png)
![create_source_s3_bucket_2](images/production_grade_implementation_version_7/S3_source/create_source_s3_bucket_2.png)
![create_source_s3_bucket_3](images/production_grade_implementation_version_7/S3_source/create_source_s3_bucket_3.png)
![create_source_s3_bucket_4](images/production_grade_implementation_version_7/S3_source/create_source_s3_bucket_4.png)
![create_source_s3_bucket_5](images/production_grade_implementation_version_7/S3_source/create_source_s3_bucket_5.png)
![create_source_s3_bucket_6](images/production_grade_implementation_version_7/S3_source/create_source_s3_bucket_6.png)
- Now that I have created a source S3 bucket I am going to configure some settings related to event bridge. 
- This configuration will allow my source se bucket to send Event messages to my event bridge 
![activate_S3_eventBridge_notification](images/production_grade_implementation_version_7/S3_source/activate_S3_eventBridge_notification.png)
![activate_S3_eventBridge_notification_2](images/production_grade_implementation_version_7/S3_source/activate_S3_eventBridge_notification_2.png)
![activate_S3_eventBridge_notification_3](images/production_grade_implementation_version_7/S3_source/activate_S3_eventBridge_notification_3.png)
- What Happens When You Turn This On?
    - The Fundamental Shift in Event Routing
        - When this toggle is Off, S3 event notifications work like this:
            ```bash
            S3 (file arrives) → directly → Lambda / SQS / SNS
                                (tight coupling)
            ```
        - When this toggle is On, S3 event notifications work like this:
            ```bash
            S3 (file arrives) → EventBridge Event Bus → Rules → Targets
                                            (loose coupling)
            ```
            - S3 stops holding the routing logic. It just broadcasts every event to EventBridge's default event bus, and EventBridge decides what to do with it based on rules I define.
    - What S3 Actually Sends to EventBridge?
        - Every time an object is created, deleted, or modified in your bucket, S3 publishes a structured JSON event to the default EventBridge event bus. For my pipeline, the most important one is the Object Created event, which looks like this:
        ```bash
        {
        "version": "0",
        "id": "unique-event-id",
        "source": "aws.s3",
        "detail-type": "Object Created",
        "account": "406868976171",
        "region": "ap-south-1",
        "detail": {
            "bucket": {
            "name": "deployement-logs-data-source"
            },
            "object": {
            "key": "raw_data/sonar_qube_logs/sonarqube_issues_part1.json",
            "size": 102400,
            "etag": "abc123..."
            },
            "reason": "PutObject"
        }
        }
        ```
        - This is why in my Lambda function and Step Function you can extract ```$.detail.bucket.name``` and ```$.detail.object.key``` those fields come directly from this payload.

### Setting up event bridge
![event_bridge_1](images/production_grade_implementation_version_7/EventBridge/event_bridge_1.png)
![event_bridge_2](images/production_grade_implementation_version_7/EventBridge/event_bridge_2.png)
![event_bridge_source_1](images/production_grade_implementation_version_7/EventBridge/event_bridge_source_1.png)
- After setting up the source to the S3 bucket I also have to define an Event pattern (filter) 
![event_patter_filder_diagram](images/production_grade_implementation_version_7/EventBridge/event_patter_filder_diagram.png)
- **What is an Event pattern filter :**
    - Event pattern filter allows you to filter what events you want to send to the target. 
    - Most of the time we don't want to trigger our pipeline on every event this filter allows us to only trigger our pipeline on a particular event 
    - In my case that event is the ```PUT``` event on which I want to trigger my pipeline i.e I want my pipeline to run on file arrival 
- Here is the Event pattern filter:
    - ```json
        {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "bucket": {
            "name": ["deployement-logs-data-source"]
            },
            "object": {
            "key": [{
                "prefix": "raw_data/sonar_qube_logs/"
            }]
            }
        }
        }
      ```

### Setting up SQS message queues 
- In my case I have to setup two sqs message queues:
    - ```DataProcessingJobQueue.fifo``` : This sqs message queue will handle the event messages recieved from event bridge and the messages will be passed on to the downstream lambda function using ```FIFO```
    - ```DataProcessingDLQ.fifo``` : This sqs message queue will handle the event messages of those files in the source s3 bucket that have failed to be processed by the pipeline.
        - This sqs message queue plays an important role in my pipeline as it prevents the event messages from being lost and also helps me keep track of what files have failed to be processed by my pipeline making sure that data is not lost at the event of a failure
#### DataProcessingJobQueue.fifo
![DataProcessingJobQueue_1](images/production_grade_implementation_version_7/sqs/DataProcessingJobQueue_1.png)
- Make sure that you don't keep the message vivibility too long example 30 min or too short example 2 min or 20 seconds. 
- You must make sure to make the message invisible depending on the total amount of time the pipeline is active and executing that file that is mentioned in that particular event message.
- In my case AWS glue job takes 4 to 5 min to complete its execution hence I have gone with the 5 min message invisibility time.
![DataProcessingJobQueue_2](images/production_grade_implementation_version_7/sqs/DataProcessingJobQueue_2.png)
- Here is the access policy code : 
    - ```json
        {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Sid": "AllowEventBridgeToSend",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:746244690650:DataProcessingJobQueue.fifo",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:746244690650:rule/sonar_qube_event_rules"
                }
            }
            }
        ]
        }
      ```
![DataProcessingJobQueue_3](images/production_grade_implementation_version_7/sqs/DataProcessingJobQueue_3.png)
![DataProcessingJobQueue_4](images/production_grade_implementation_version_7/sqs/DataProcessingJobQueue_4.png)

#### DataProcessingDLQ.fifo
![DataProcessingDLQ_1](images/production_grade_implementation_version_7/sqs/DataProcessingDLQ_1.png)
![DataProcessingDLQ_2](images/production_grade_implementation_version_7/sqs/DataProcessingDLQ_2.png)
- Here is the access policy code : 
    - ```json
        {
            "Version": "2012-10-17",
            "Id": "__default_policy_ID",
            "Statement": [
                {
                "Sid": "__owner_statement",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::746244690650:root"
                },
                "Action": "SQS:*",
                "Resource": "arn:aws:sqs:ap-south-1:746244690650:DataProcessingDLQ.fifo"
                },
                {
                "Sid": "AllowStepFunctionToSendMessageToDLQ",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::746244690650:role/service-role/StepFunctions-process_sonar_qube_step_function-role-luacf37bc"
                },
                "Action": "sqs:SendMessage",
                "Resource": "arn:aws:sqs:ap-south-1:746244690650:DataProcessingDLQ.fifo"
                }
            ]
        }
      ```
![DataProcessingDLQ_3](images/production_grade_implementation_version_7/sqs/DataProcessingDLQ_3.png)
![DataProcessingDLQ_4](images/production_grade_implementation_version_7/sqs/DataProcessingDLQ_4.png)

### Setting up and implementing Lambda functions
#### TriggerProcessSonarQubeLogStepFunction
- ```TriggerProcessSonarQubeLogStepFunction``` is the name of this lamdba function
- The reason I had to use lambda function in my architecture is because SQS does not have the capability of directly triggering the step function. 
- So here I am going to use Lambda function to ultimately trigger my step function called ```process_sonar_qube_step_function``` which will orchestrate AWS glue job 
- ![create_lambda_function_1](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_1.png)
- ![create_lambda_function_2](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_2.png)
![create_lambda_function_3](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_3.png)
- Lambda function Python code : 
    - ```python
        import json
        import boto3
        import uuid
        import time
        import os
        from datetime import datetime, timezone, timedelta

        sf       = boto3.client("stepfunctions")
        dynamodb = boto3.client("dynamodb")
        glue     = boto3.client("glue")

        STATE_MACHINE_ARN                        = os.environ["STATE_MACHINE_ARN_ENV"]
        LAMBDA_DYNAMO_TABLE                      = os.environ["DYNAMO_DB_TABLE_NAME"]
        DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME = os.environ["DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME"]
        AWS_GLUE_JOB_NAME                        = os.environ["AWS_GLUE_JOB_NAME"]
        GLUE_SINK_BUCKET                         = os.environ["SINK_BUCKET_NAME"]
        GLUE_CATALOG_DATABASE                    = os.environ["GLUE_CATALOG_DATABASE"]
        GLUE_CATALOG_TABLE_NAME                  = os.environ["GLUE_CATALOG_TABLE_NAME"]
        GLUE_DYNAMO_TABLE                        = os.environ["DYNAMO_DB_TABLE_NAME"]


        def sync_glue_concurrency():
            response        = glue.get_job(JobName=AWS_GLUE_JOB_NAME)
            max_concurrency = response["Job"].get("ExecutionProperty", {}).get("MaxConcurrentRuns", 1)

            try:
                dynamodb.put_item(
                    TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                    Item={
                        "semaphore_key":      {"S": "GLUE_SEMAPHORE"},
                        "current_count":      {"N": "0"},
                        "max_glue_concurrency": {"N": str(max_concurrency)}
                    },
                    ConditionExpression="attribute_not_exists(semaphore_key)"
                )
                print("Semaphore created")
                return

            except dynamodb.exceptions.ConditionalCheckFailedException:
                pass

            try:
                dynamodb.update_item(
                    TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                    Key={"semaphore_key": {"S": "GLUE_SEMAPHORE"}},
                    UpdateExpression="SET max_glue_concurrency = :new",
                    ConditionExpression="max_glue_concurrency <> :new",
                    ExpressionAttributeValues={":new": {"N": str(max_concurrency)}}
                )
            except dynamodb.exceptions.ConditionalCheckFailedException:
                pass


        def acquire_glue_slot():
            """
            Atomically acquire one Glue concurrency slot.
            Returns True if slot acquired, False if Glue is at capacity.
            This is the critical fix: the lock is acquired HERE in Lambda,
            not inside the Step Function after it has already started.
            """
            try:
                dynamodb.update_item(
                    TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                    Key={"semaphore_key": {"S": "GLUE_SEMAPHORE"}},
                    UpdateExpression="SET current_count = current_count + :inc",
                    ConditionExpression="current_count < max_glue_concurrency",
                    ExpressionAttributeValues={":inc": {"N": "1"}}
                )
                print("Glue slot acquired")
                return True

            except dynamodb.exceptions.ConditionalCheckFailedException:
                print("Glue at capacity — returning message to SQS")
                return False


        def lambda_handler(event, context):

            sync_glue_concurrency()

            IST       = timezone(timedelta(hours=5, minutes=30))
            timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")
            ttl_value = int(time.time()) + (30 * 24 * 60 * 60)

            # Report batch item failures so only the unprocessed message
            # returns to SQS — not the entire batch.
            batch_item_failures = []

            for record in event["Records"]:
                body     = json.loads(record["body"])
                bucket   = body["detail"]["bucket"]["name"]
                key      = body["detail"]["object"]["key"]
                file_key = f"s3://{bucket}/{key}"

                # ── Idempotency check ──────────────────────────────────────
                existing = dynamodb.get_item(
                    TableName=LAMBDA_DYNAMO_TABLE,
                    Key={"file_key": {"S": file_key}}
                )
                item = existing.get("Item")

                if item:
                    status = item.get("status", {}).get("S", "")
                    if status == "SUCCESS":
                        print(f"Already succeeded — skipping: {file_key}")
                        continue
                    if status == "IN_PROGRESS":
                        print(f"Already in progress — skipping: {file_key}")
                        continue
                    print(f"Retrying failed file: {file_key}")

                # ── Acquire Glue slot BEFORE starting the Step Function ────
                # If Glue is full, return this specific message to SQS.
                # SQS will retry it after the visibility timeout — no thundering
                # herd because SQS staggers retries independently per message.
                if not acquire_glue_slot():
                    batch_item_failures.append({"itemIdentifier": record["messageId"]})
                    continue

                # ── Write idempotency record ───────────────────────────────
                try:
                    dynamodb.put_item(
                        TableName=LAMBDA_DYNAMO_TABLE,
                        Item={
                            "file_key":    {"S": file_key},
                            "status":      {"S": "IN_PROGRESS"},
                            "retry_count": {"N": "0"},
                            "created_at":  {"S": timestamp},
                            "ttl":         {"N": str(ttl_value)}
                        },
                        ConditionExpression="attribute_not_exists(file_key) OR #s = :failed",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":failed": {"S": "FAILED"}}
                    )
                except dynamodb.exceptions.ConditionalCheckFailedException:
                    # Race condition — another Lambda already claimed this file.
                    # Release the slot we just acquired since we won't use it.
                    dynamodb.update_item(
                        TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                        Key={"semaphore_key": {"S": "GLUE_SEMAPHORE"}},
                        UpdateExpression="SET current_count = current_count - :dec",
                        ConditionExpression="current_count > :zero",
                        ExpressionAttributeValues={
                            ":dec": {"N": "1"},
                            ":zero": {"N": "0"}
                        }
                    )
                    print(f"Race condition — slot released, skipping: {file_key}")
                    continue

                # ── Start Step Function — it already holds the slot ────────
                sf.start_execution(
                    stateMachineArn=STATE_MACHINE_ARN,
                    input=json.dumps({
                        "files": [{
                            "bucket":   bucket,
                            "key":      key,
                            "file_key": file_key,
                            "glue_args": {
                                "--SINK_BUCKET_NAME":        GLUE_SINK_BUCKET,
                                "--GLUE_CATALOG_DATABASE":   GLUE_CATALOG_DATABASE,
                                "--GLUE_CATALOG_TABLE_NAME": GLUE_CATALOG_TABLE_NAME,
                                "--DYNAMO_DB_TABLE_NAME":    GLUE_DYNAMO_TABLE,
                                "--DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME":
                                    DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                            }
                        }]
                    })
                )
                print(f"Step Function started for: {file_key}")

            return {"batchItemFailures": batch_item_failures}
      ```
- Set the trigger in this lambda function with this configuration as below
- When I set this trigger in my lamdba function, this allows my sqs queue to invoke lamdba function the moment it recieves the event message from the event bridge 
- ![create_lambda_function_4](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_4.png)
- IAM role configuration for this lambda function 
- ![create_lambda_function_5](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_5.png)
    - The name of the IAM role for this lamdba function ```TriggerProcessSonarQubeLogStepFunctionRole```
    - Policies attached to this role : 
        - ```AllowLambdaFunctionToGetGlueJobName``` : 
            - This policy allows this lambda function to get the AWS glue job name using boto3 dynamically
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "Statement1",
                        "Effect": "Allow",
                        "Action": "glue:GetJob",
                        "Resource": "arn:aws:glue:ap-south-1:746244690650:job/process_sonar_qube_logs_ETL"
                    }
                ]
            }
            ```
        - ```AllowLambdaToAccessSQSAndStepFunction``` : 
            - This policy allows this lambda function to access ```DataProcessingJobQueue.fifo``` and ```DataProcessingDLQ.fifo``` sqs queues with the ability to ability to delete and recieve messages 
            - This also gives this lambda function to write logs in cloud watch
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "states:StartExecution"
                        ],
                        "Resource": "arn:aws:states:ap-south-1:746244690650:stateMachine:process_sonar_qube_step_function"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes"
                        ],
                        "Resource": [
                            "arn:aws:sqs:ap-south-1:746244690650:DataProcessingJobQueue.fifo",
                            "arn:aws:sqs:ap-south-1:746244690650:DataProcessingDLQ.fifo"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        "Resource": "*"
                    }
                ]
            }
            ```
        - ```DynamoDbAccessPolicyForLambdaFunction``` : 
            - This policy allows this lambda function to talk to dynamoDB. Where this lambda function can create, update and get items from ```sonar_qube_logs_processing_registery``` and ```glue_semaphore``` tables in dynamoDB
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DynamoDbAccessPolicyForLambdaFunction",
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:PutItem",
                            "dynamodb:GetItem",
                            "dynamodb:UpdateItem"
                        ],
                        "Resource": [
                            "arn:aws:dynamodb:ap-south-1:746244690650:table/sonar_qube_logs_processing_registery",
                            "arn:aws:dynamodb:ap-south-1:746244690650:table/glue_semaphore"
                        ]
                    }
                ]
            }
            ```
- Set environment variables for this lambda function 
- ![create_lambda_function_6](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_6.png)

#### ProcessDLQMessagesOnDemandLamdbaFunction
- This lambda function is responsible for handling DLQ mechanism in this pipeline 
- Since the lambda function creation process is the same I am not including any screen shot for the same here.
- Here is the python code : 
    - ```python
        import json
        import boto3
        import time
        import os
        from datetime import datetime, timezone, timedelta

        # ─────────────────────────────────────────────────────────────────────────────
        # Environment variables — same names and pattern as TriggerProcessSonarQubeLogStepFunction.
        # Two additions: DLQ_URL and MAX_MESSAGES_PER_RUN.
        # ─────────────────────────────────────────────────────────────────────────────
        sf       = boto3.client("stepfunctions")
        dynamodb = boto3.client("dynamodb")
        glue     = boto3.client("glue")
        sqs      = boto3.client("sqs")

        STATE_MACHINE_ARN                        = os.environ["STATE_MACHINE_ARN_ENV"]
        LAMBDA_DYNAMO_TABLE                      = os.environ["DYNAMO_DB_TABLE_NAME"]
        DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME = os.environ["DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME"]
        AWS_GLUE_JOB_NAME                        = os.environ["AWS_GLUE_JOB_NAME"]
        GLUE_SINK_BUCKET                         = os.environ["SINK_BUCKET_NAME"]
        GLUE_CATALOG_DATABASE                    = os.environ["GLUE_CATALOG_DATABASE"]
        GLUE_CATALOG_TABLE_NAME                  = os.environ["GLUE_CATALOG_TABLE_NAME"]
        GLUE_DYNAMO_TABLE                        = os.environ["DYNAMO_DB_TABLE_NAME"]   # same alias pattern as original Lambda
        DLQ_URL                                  = os.environ["DLQ_URL"]
        MAX_MESSAGES_PER_RUN                     = int(os.environ.get("MAX_MESSAGES_PER_RUN"))


        # ─────────────────────────────────────────────────────────────────────────────
        # sync_glue_concurrency — identical to TriggerProcessSonarQubeLogStepFunction.
        # Both Lambdas must enforce the same semaphore or the guard is meaningless.
        # ─────────────────────────────────────────────────────────────────────────────
        def sync_glue_concurrency():
            response        = glue.get_job(JobName=AWS_GLUE_JOB_NAME)
            max_concurrency = response["Job"].get("ExecutionProperty", {}).get("MaxConcurrentRuns", 1)

            try:
                dynamodb.put_item(
                    TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                    Item={
                        "semaphore_key":        {"S": "GLUE_SEMAPHORE"},
                        "current_count":        {"N": "0"},
                        "max_glue_concurrency": {"N": str(max_concurrency)}
                    },
                    ConditionExpression="attribute_not_exists(semaphore_key)"
                )
                print("Semaphore created")
                return
            except dynamodb.exceptions.ConditionalCheckFailedException:
                pass

            try:
                dynamodb.update_item(
                    TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                    Key={"semaphore_key": {"S": "GLUE_SEMAPHORE"}},
                    UpdateExpression="SET max_glue_concurrency = :new",
                    ConditionExpression="max_glue_concurrency <> :new",
                    ExpressionAttributeValues={":new": {"N": str(max_concurrency)}}
                )
            except dynamodb.exceptions.ConditionalCheckFailedException:
                pass


        # ─────────────────────────────────────────────────────────────────────────────
        # acquire_glue_slot — identical to TriggerProcessSonarQubeLogStepFunction.
        # Returns True if slot acquired, False if Glue is at capacity.
        # ─────────────────────────────────────────────────────────────────────────────
        def acquire_glue_slot():
            try:
                dynamodb.update_item(
                    TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                    Key={"semaphore_key": {"S": "GLUE_SEMAPHORE"}},
                    UpdateExpression="SET current_count = current_count + :inc",
                    ConditionExpression="current_count < max_glue_concurrency",
                    ExpressionAttributeValues={":inc": {"N": "1"}}
                )
                print("Glue slot acquired")
                return True
            except dynamodb.exceptions.ConditionalCheckFailedException:
                print("Glue at capacity — leaving message in DLQ")
                return False


        # ─────────────────────────────────────────────────────────────────────────────
        # release_glue_slot — extra helper needed only in replay context.
        # The original Lambda returns the message to SQS on capacity failure so it
        # never holds a slot it won't use.  Here, if start_execution throws AFTER
        # acquire_glue_slot() succeeded, we must release the slot manually.
        # ─────────────────────────────────────────────────────────────────────────────
        def release_glue_slot():
            try:
                dynamodb.update_item(
                    TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                    Key={"semaphore_key": {"S": "GLUE_SEMAPHORE"}},
                    UpdateExpression="SET current_count = current_count - :dec",
                    ConditionExpression="current_count > :zero",
                    ExpressionAttributeValues={
                        ":dec":  {"N": "1"},
                        ":zero": {"N": "0"}
                    }
                )
            except dynamodb.exceptions.ConditionalCheckFailedException:
                pass  # Already at 0 — safe to ignore


        # ─────────────────────────────────────────────────────────────────────────────
        # parse_dlq_message
        #
        # DLQ messages written by the FIXED SendToDLQ state contain States.JsonToString($),
        # which is the full Step Function state at that point.  That state includes:
        #   { "bucket": "...", "key": "...", "file_key": "...", "glue_args": {...}, ... }
        #
        # Messages written by the OLD SendToDLQ (States.JsonToString($.error)) only
        # have { "Error": "...", "Cause": "..." } and cannot be replayed automatically.
        # ─────────────────────────────────────────────────────────────────────────────
        def parse_dlq_message(body_str: str):
            try:
                body = json.loads(body_str)
            except json.JSONDecodeError as e:
                raise ValueError(f"DLQ message is not valid JSON: {e}")

            # Detect old-format messages — error-only, no file coordinates
            if set(body.keys()) <= {"Error", "Cause"}:
                raise ValueError(
                    "Message is in old format (error-only, pre-dates SendToDLQ fix). "
                    "Cannot replay automatically — resolve via DynamoDB FAILED records."
                )

            bucket   = body.get("bucket")
            key      = body.get("key")
            file_key = body.get("file_key")

            if not all([bucket, key, file_key]):
                raise ValueError(
                    f"Message is missing required fields (bucket/key/file_key). "
                    f"Keys present: {list(body.keys())}"
                )

            return bucket, key, file_key


        # ─────────────────────────────────────────────────────────────────────────────
        # lambda_handler
        #
        # Triggered MANUALLY ONLY — no SQS trigger on this function.
        # Invoke via Lambda console Test button (payload: {}) or AWS CLI.
        #
        # Polls DataProcessingDLQ.fifo up to MAX_MESSAGES_PER_RUN times.
        # For each message the flow mirrors TriggerProcessSonarQubeLogStepFunction:
        #   idempotency check → acquire slot → write IN_PROGRESS → start Step Function
        #
        # Key difference from the main Lambda:
        #   - Reads from DLQ via sqs.receive_message() instead of event["Records"]
        #   - Deletes message from DLQ only AFTER Step Function starts successfully
        #   - Messages left un-deleted become visible again after VisibilityTimeout
        # ─────────────────────────────────────────────────────────────────────────────
        def lambda_handler(event, context):

            sync_glue_concurrency()

            IST       = timezone(timedelta(hours=5, minutes=30))
            timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")
            ttl_value = int(time.time()) + (30 * 24 * 60 * 60)

            processed   = 0  # handed to Step Function
            skipped     = 0  # idempotency skips
            at_capacity = 0  # Glue full — left in DLQ for next run
            errored     = 0  # unrecoverable message — deleted from DLQ

            messages_to_attempt = MAX_MESSAGES_PER_RUN
            print(f"DLQ Replay | Starting — will attempt up to {messages_to_attempt} messages")

            while messages_to_attempt > 0:

                # SQS max is 10 messages per receive call
                batch_size = min(messages_to_attempt, 10)

                response = sqs.receive_message(
                    QueueUrl=DLQ_URL,
                    MaxNumberOfMessages=batch_size,
                    VisibilityTimeout=300,   # 5 min — enough time to start Step Function and delete
                    WaitTimeSeconds=0        # short poll — on-demand context, don't hang
                )

                messages = response.get("Messages", [])

                if not messages:
                    print("DLQ Replay | No more messages in DLQ — stopping")
                    break

                for msg in messages:
                    receipt_handle = msg["ReceiptHandle"]
                    body_str       = msg["Body"]

                    # ── Parse DLQ message ──────────────────────────────────
                    try:
                        bucket, key, file_key = parse_dlq_message(body_str)
                    except ValueError as e:
                        # Unrecoverable — delete to unblock the queue and log clearly
                        print(f"DLQ Replay | UNRECOVERABLE — deleting. Reason: {e}")
                        sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=receipt_handle)
                        errored += 1
                        messages_to_attempt -= 1
                        continue

                    print(f"DLQ Replay | Processing: {file_key}")

                    # ── Idempotency check — same logic as main Lambda ──────
                    existing = dynamodb.get_item(
                        TableName=LAMBDA_DYNAMO_TABLE,
                        Key={"file_key": {"S": file_key}}
                    )
                    item = existing.get("Item")

                    if item:
                        status = item.get("status", {}).get("S", "")
                        if status == "SUCCESS":
                            # File already succeeded — stale DLQ message, clean it up
                            print(f"Already succeeded — deleting from DLQ: {file_key}")
                            sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=receipt_handle)
                            skipped += 1
                            messages_to_attempt -= 1
                            continue
                        if status == "IN_PROGRESS":
                            # Step Function is already running — leave in DLQ, don't double-fire
                            print(f"Already in progress — skipping: {file_key}")
                            skipped += 1
                            messages_to_attempt -= 1
                            continue
                        print(f"Retrying failed file: {file_key}")

                    # ── Acquire Glue slot — same logic as main Lambda ──────
                    if not acquire_glue_slot():
                        # Glue full — do NOT delete the DLQ message.
                        # Visibility timeout will expire and message becomes available
                        # for the next manual trigger run.
                        at_capacity += 1
                        messages_to_attempt -= 1
                        print(f"DLQ Replay | Glue full — {file_key} left in DLQ for next run")
                        continue

                    # ── Write idempotency record — same logic as main Lambda
                    try:
                        dynamodb.put_item(
                            TableName=LAMBDA_DYNAMO_TABLE,
                            Item={
                                "file_key":    {"S": file_key},
                                "status":      {"S": "IN_PROGRESS"},
                                "retry_count": {"N": "0"},
                                "created_at":  {"S": timestamp},
                                "ttl":         {"N": str(ttl_value)}
                            },
                            ConditionExpression="attribute_not_exists(file_key) OR #s = :failed",
                            ExpressionAttributeNames={"#s": "status"},
                            ExpressionAttributeValues={":failed": {"S": "FAILED"}}
                        )
                    except dynamodb.exceptions.ConditionalCheckFailedException:
                        # Race condition — another process claimed this file.
                        # Release slot we just acquired since we won't use it.
                        dynamodb.update_item(
                            TableName=DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                            Key={"semaphore_key": {"S": "GLUE_SEMAPHORE"}},
                            UpdateExpression="SET current_count = current_count - :dec",
                            ConditionExpression="current_count > :zero",
                            ExpressionAttributeValues={
                                ":dec":  {"N": "1"},
                                ":zero": {"N": "0"}
                            }
                        )
                        print(f"Race condition — slot released, skipping: {file_key}")
                        skipped += 1
                        messages_to_attempt -= 1
                        continue

                    # ── Start Step Function — same input shape as main Lambda
                    try:
                        sf.start_execution(
                            stateMachineArn=STATE_MACHINE_ARN,
                            input=json.dumps({
                                "files": [{
                                    "bucket":   bucket,
                                    "key":      key,
                                    "file_key": file_key,
                                    "glue_args": {
                                        "--SINK_BUCKET_NAME":        GLUE_SINK_BUCKET,
                                        "--GLUE_CATALOG_DATABASE":   GLUE_CATALOG_DATABASE,
                                        "--GLUE_CATALOG_TABLE_NAME": GLUE_CATALOG_TABLE_NAME,
                                        "--DYNAMO_DB_TABLE_NAME":    GLUE_DYNAMO_TABLE,
                                        "--DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME":
                                            DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME,
                                    }
                                }]
                            })
                        )
                        print(f"Step Function started for: {file_key}")
                    except Exception as e:
                        # start_execution failed — release the slot we acquired
                        # and reset DynamoDB to FAILED so it stays retryable.
                        release_glue_slot()
                        dynamodb.update_item(
                            TableName=LAMBDA_DYNAMO_TABLE,
                            Key={"file_key": {"S": file_key}},
                            UpdateExpression="SET #s = :failed",
                            ExpressionAttributeNames={"#s": "status"},
                            ExpressionAttributeValues={":failed": {"S": "FAILED"}}
                        )
                        print(f"Step Function failed to start: {e} — message left in DLQ")
                        errored += 1
                        messages_to_attempt -= 1
                        continue

                    # ── Delete from DLQ only AFTER Step Function starts ────
                    # If we deleted before and start_execution threw, the message
                    # would be lost permanently with no way to replay.
                    sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=receipt_handle)
                    print(f"Deleted from DLQ: {file_key}")

                    processed += 1
                    messages_to_attempt -= 1

            print(
                f"DLQ Replay | Complete — "
                f"processed={processed}, skipped={skipped}, "
                f"left_in_DLQ_glue_full={at_capacity}, unrecoverable_deleted={errored}"
            )

            return {
                "statusCode": 200,
                "body": {
                    "processed":                    processed,
                    "skipped_already_done":         skipped,
                    "left_in_dlq_glue_at_capacity": at_capacity,
                    "unrecoverable_deleted":        errored,
                }
            }
      ```
- Set Env variables in this lambda function : 
![create_lambda_function_8](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_8.png)
- Set IAM role for this lambda function : 
- ![create_lambda_function_9](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_9.png)
    - The name of the IAM role that I set to this lambda function is named ```ProcessDLQMessagesOnDemandLamdbaFunction-role-hpkfxej8```
    - Policies attached to this Role : 
        - ```AllowDLQLamdbaFunctionToGetAWSGlueJobName``` : 
            - This policy allows this lambda function to get aws glue job name dynamically using boto3 library
            - ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "Statement1",
                            "Effect": "Allow",
                            "Action": "glue:GetJob",
                            "Resource": "arn:aws:glue:ap-south-1:746244690650:job/process_sonar_qube_logs_ETL"
                        }
                    ]
                }
              ```
        - ```AllowLambdaToAccessSQSAndStepFunction``` : 
            - This policy allows this lambda function to invoke the step function named ```process_sonar_qube_step_function```
            - This policy allows this lambda function to read, delete and get event messages from ```DataProcessingJobQueue.fifo``` and ```DataProcessingDLQ.fifo``` sqs queues 
            - This policy also allows this lambda function to create logs in cloud watch 
            - ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "states:StartExecution"
                            ],
                            "Resource": "arn:aws:states:ap-south-1:746244690650:stateMachine:process_sonar_qube_step_function"
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "sqs:ReceiveMessage",
                                "sqs:DeleteMessage",
                                "sqs:GetQueueAttributes"
                            ],
                            "Resource": [
                                "arn:aws:sqs:ap-south-1:746244690650:DataProcessingJobQueue.fifo",
                                "arn:aws:sqs:ap-south-1:746244690650:DataProcessingDLQ.fifo"
                            ]
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents"
                            ],
                            "Resource": "*"
                        }
                    ]
                }
              ```
        - ```DynamoDbAccessPolicyForLambdaFunction``` : 
            - This allows lambda functions to get, create or update items in tables named ```glue_semaphore``` and ```sonar_qube_logs_processing_registery``` in dynamoDB
            - ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "DynamoDbAccessPolicyForLambdaFunction",
                            "Effect": "Allow",
                            "Action": [
                                "dynamodb:PutItem",
                                "dynamodb:GetItem",
                                "dynamodb:UpdateItem"
                            ],
                            "Resource": [
                                "arn:aws:dynamodb:ap-south-1:746244690650:table/sonar_qube_logs_processing_registery",
                                "arn:aws:dynamodb:ap-south-1:746244690650:table/glue_semaphore"
                            ]
                        }
                    ]
                }
              ```
- In this lambda function I will not set any trigger because I don't want SQS DLQ queue named ```DataProcessingDLQ.fifo``` to automatically trigger my lambda function I want to trigger the DLQ mechanism manually using the test button
- In order to run this lambda function using the test button make sure to remove the default key value pairs from the input in the lambda function as shown in this screenshot. 
![create_lambda_function_7](images/production_grade_implementation_version_7/LambdaFunction/create_lambda_function_7.png)
- In case you have multiple files in DLQ and you want to re-play all the files then you will have to use event bridge scheduler and schedule this lambda function to run such that the number of times this lambda function should run is euqal to the number of messages in DLQ with the time interval of 5 min in between each execution. 

### Setting up Step functions 
- In this version I managed to reduce the number of step functions that is used to orchestrate AWS glue ETL pipeline. 
- In version 6 there were two step functions but version 7 I have created only one step function and I managed to re-use the same step function for processing my DLQ event messages 
- Step function in version 7 is called ```process_sonar_qube_step_function``` the code of which is shown as below :
    - ```json
      {
        "Comment": "Glue ETL — semaphore acquired by Lambda before execution starts. No thundering herd possible.",
        "StartAt": "ProcessFiles",
        "States": {
            "ProcessFiles": {
            "Type": "Map",
            "ItemsPath": "$.files",
            "MaxConcurrency": 1,
            "Iterator": {
                "StartAt": "RunGlueJob",
                "States": {
                "RunGlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                    "JobName": "process_sonar_qube_logs_ETL",
                    "Arguments": {
                        "--source_bucket.$": "$.bucket",
                        "--source_key.$": "$.key",
                        "--SINK_BUCKET_NAME.$": "$.glue_args.--SINK_BUCKET_NAME",
                        "--GLUE_CATALOG_DATABASE.$": "$.glue_args.--GLUE_CATALOG_DATABASE",
                        "--GLUE_CATALOG_TABLE_NAME.$": "$.glue_args.--GLUE_CATALOG_TABLE_NAME",
                        "--DYNAMO_DB_TABLE_NAME.$": "$.glue_args.--DYNAMO_DB_TABLE_NAME"
                    }
                    },
                    "ResultPath": "$.glueResult",
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "ResultPath": "$.error",
                        "Next": "ReleaseLockAfterFailure"
                    }
                    ],
                    "Next": "ReleaseLockAfterSuccess"
                },
                "ReleaseLockAfterSuccess": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName.$": "$.glue_args.--DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME",
                    "Key": {
                        "semaphore_key": {
                        "S": "GLUE_SEMAPHORE"
                        }
                    },
                    "UpdateExpression": "SET current_count = current_count - :dec",
                    "ConditionExpression": "current_count > :zero",
                    "ExpressionAttributeValues": {
                        ":dec": {
                        "N": "1"
                        },
                        ":zero": {
                        "N": "0"
                        }
                    }
                    },
                    "ResultPath": "$.releaseResult",
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "DynamoDB.ConditionalCheckFailedException"
                        ],
                        "ResultPath": "$.releaseError",
                        "Next": "UpdateSuccess"
                    }
                    ],
                    "Next": "UpdateSuccess"
                },
                "ReleaseLockAfterFailure": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName.$": "$.glue_args.--DYNAMO_DB_CONCURRENCY_TRACKER_TABLE_NAME",
                    "Key": {
                        "semaphore_key": {
                        "S": "GLUE_SEMAPHORE"
                        }
                    },
                    "UpdateExpression": "SET current_count = current_count - :dec",
                    "ConditionExpression": "current_count > :zero",
                    "ExpressionAttributeValues": {
                        ":dec": {
                        "N": "1"
                        },
                        ":zero": {
                        "N": "0"
                        }
                    }
                    },
                    "ResultPath": "$.releaseResult",
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "DynamoDB.ConditionalCheckFailedException"
                        ],
                        "ResultPath": "$.releaseError",
                        "Next": "UpdateFailure"
                    }
                    ],
                    "Next": "UpdateFailure"
                },
                "UpdateSuccess": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName.$": "$.glue_args.--DYNAMO_DB_TABLE_NAME",
                    "Key": {
                        "file_key": {
                        "S.$": "$.file_key"
                        }
                    },
                    "UpdateExpression": "SET #s = :s, updated_at = :t",
                    "ExpressionAttributeNames": {
                        "#s": "status"
                    },
                    "ExpressionAttributeValues": {
                        ":s": {
                        "S": "SUCCESS"
                        },
                        ":t": {
                        "S.$": "$$.State.EnteredTime"
                        }
                    }
                    },
                    "ResultPath": "$.updateSuccessResult",
                    "End": true
                },
                "UpdateFailure": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                    "TableName.$": "$.glue_args.--DYNAMO_DB_TABLE_NAME",
                    "Key": {
                        "file_key": {
                        "S.$": "$.file_key"
                        }
                    },
                    "UpdateExpression": "SET #s = :s, error_message = :e, updated_at = :t",
                    "ExpressionAttributeNames": {
                        "#s": "status"
                    },
                    "ExpressionAttributeValues": {
                        ":s": {
                        "S": "FAILED"
                        },
                        ":e": {
                        "S.$": "States.JsonToString($)"
                        },
                        ":t": {
                        "S.$": "$$.State.EnteredTime"
                        }
                    }
                    },
                    "ResultPath": "$.updateFailureResult",
                    "Next": "SendToDLQ"
                },
                "SendToDLQ": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                    "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/746244690650/DataProcessingDLQ.fifo",
                    "MessageGroupId": "sonar-qube-dlq-group",
                    "MessageBody.$": "States.JsonToString($)"
                    },
                    "ResultPath": "$.dlqResult",
                    "End": true
                }
                }
            },
            "End": true
            }
        }
        }
      ```
    - ![step_function_1](images/production_grade_implementation_version_7/step_function/step_function_1.png)
- These are the configurations that I did when creating the step function 
- ![step_function_2](images/production_grade_implementation_version_7/step_function/step_function_2.png)
- ![step_function_3](images/production_grade_implementation_version_7/step_function/step_function_3.png)
![step_function_4](images/production_grade_implementation_version_7/step_function/step_function_4.png)
![step_function_5](images/production_grade_implementation_version_7/step_function/step_function_5.png)
- IAM role attached to this step function is named ```StepFunctions-process_sonar_qube_step_function-role-luacf37bc ```
    - Policies attached to this role is : 
        - ```AllowServicesToWriteLogs``` : 
            -   This policy allows step function to write logs in cloud watch
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogDelivery",
                            "logs:GetLogDelivery",
                            "logs:UpdateLogDelivery",
                            "logs:DeleteLogDelivery",
                            "logs:ListLogDeliveries",
                            "logs:PutResourcePolicy",
                            "logs:DescribeResourcePolicies",
                            "logs:DescribeLogGroups"
                        ],
                        "Resource": "*"
                    }
                ]
            }
            ```
        - ```DynamoDBTableContentScopedAccessPolicy-c033e285-d19d-4a39-907e-fd05b0dd99d1``` : 
            - This policy allows step funtion to get and update items in ```sonar_qube_logs_processing_registery``` and ```glue_semaphore``` tables in dynamoDB
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DynamoDBStepFunctionAccessPolicy",
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:UpdateItem",
                            "dynamodb:GetItem"
                        ],
                        "Resource": [
                            "arn:aws:dynamodb:ap-south-1:746244690650:table/sonar_qube_logs_processing_registery",
                            "arn:aws:dynamodb:ap-south-1:746244690650:table/glue_semaphore"
                        ]
                    }
                ]
            }
            ```
        - ```GlueJobRunManagementFullAccessPolicy-f580ad74-893a-47e2-96cb-ae620447b8e7``` : 
            - This policy allows the step function to start , get and stop job runs in aws glue.
            ```json
            {
                "Statement": [
                    {
                        "Action": [
                            "glue:StartJobRun",
                            "glue:GetJobRun",
                            "glue:GetJobRuns",
                            "glue:BatchStopJobRun"
                        ],
                        "Effect": "Allow",
                        "Resource": [
                            "*"
                        ]
                    }
                ],
                "Version": "2012-10-17"
            }
            ```
        - ```SQSSendMessageScopedAccessPolicy-9ccb363f-5a86-4466-b4cc-ac822ec251cd``` : 
            - This policy allows step function to send and save messages in ```DataProcessingDLQ.fifo``` and ```DataProcessingJobQueue.fifo``` sqs queue.
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "sqs:SendMessage"
                        ],
                        "Resource": [
                            "arn:aws:sqs:ap-south-1:746244690650:DataProcessingJobQueue.fifo",
                            "arn:aws:sqs:ap-south-1:746244690650:DataProcessingDLQ.fifo"
                        ]
                    }
                ]
            }
            ```
        - ```XRayAccessPolicy-66ef214f-f896-4d88-8a5a-9d460bf392ec``` : 
            - This policy allows step function to use X-ray which is an aws service 
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "xray:PutTraceSegments",
                            "xray:PutTelemetryRecords",
                            "xray:GetSamplingRules",
                            "xray:GetSamplingTargets"
                        ],
                        "Resource": [
                            "*"
                        ]
                    }
                ]
            }
            ```

### Setting up AWS DynamoDB
- In this implementation I have created two tables 
    - ```glue_semaphore``` : 
        - This table allows the other systems like lambda function and step functions to keep track of the number of concurrent AWS glue ETL job running and the number of concurrency set in AWS glue ETL configuration. 
    - ```sonar_qube_logs_processing_registery``` : 
        - This table allows the pipeline to keep track of the files that have : 
            - Success : 
                - The files that have successfully processed by AWS glue ETL job without any errors.
            - Failed : 
                - The files that have failed to be processed by AWS glue ETL job due to some errors.
                - In this case the error_message column will contain a copy of the event message that is being saved in DLQ named ```DataProcessingDLQ.fifo```. This is done to backup the event messages in case someone accidently purged by someone.
            - In_progress : 
                - The files that are currently being processed by AWS glue ETL job
#### sonar_qube_logs_processing_registery
![dynamo_db_sonar_qube_logs_processing_registery](images/production_grade_implementation_version_7/dynamoDB/dynamo_db_sonar_qube_logs_processing_registery.png)
- I am including this screen shot to give you an idea what are the columns that you will have to include in this table that makes sense.
#### glue_semaphore
![dynamo_db_glue_semaphore_1](images/production_grade_implementation_version_7/dynamoDB/dynamo_db_glue_semaphore_1.png)
![dynamo_db_glue_semaphore_2](images/production_grade_implementation_version_7/dynamoDB/dynamo_db_glue_semaphore_2.png)
- I am including this screen shot to give you an idea what are the columns that you will have to include in this table that makes sense.
- One thing to note this table is not a multi-row table this is a single row table i.e in this table once a row is inserted only update operations will be performed in that row.

### Setting up AWS glue ETL job
- AWS glue ETL pipeline configurations: 
    - ![aws_glue_config_1](images/production_grade_implementation_version_7/aws_glue/aws_glue_config_1.png)
    - ![aws_glue_config_2](images/production_grade_implementation_version_7/aws_glue/aws_glue_config_2.png)
    - ![aws_glue_config_3](images/production_grade_implementation_version_7/aws_glue/aws_glue_config_3.png)
    - ![aws_glue_config_4](images/production_grade_implementation_version_7/aws_glue/aws_glue_config_4.png)
        - Here I have set the max_concurrency of AWS glue ETL job to be 2. 
        - The reason for this is to mitigate sudden burst of cost i.e it is the method I used to prevent cost explosion in cases when a large number of files arrive in S3 bucket.
    ![aws_glue_config_5](images/production_grade_implementation_version_7/aws_glue/aws_glue_config_5.png)
    - ![aws_glue_config_6](images/production_grade_implementation_version_7/aws_glue/aws_glue_config_6.png)
- Python code : 
    ```python
    import sys
    import csv
    import io
    import re
    import boto3

    from functools import reduce
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.gluetypes import *
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue import DynamicFrame

    # ─────────────────────────────────────────────────────────────────────────────
    # Helper: run a Spark SQL query on a DynamicFrame
    # ─────────────────────────────────────────────────────────────────────────────
    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

    # ─────────────────────────────────────────────────────────────────────────────
    # Helper: drop columns whose every value is null / empty / sentinel
    # (unchanged from the original sales pipeline — kept for consistency)
    # ─────────────────────────────────────────────────────────────────────────────
    def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
        if not isinstance(path, str):
            raise TypeError(f"Expected path to be str, got {type(path)} :: path_value -> {path}")

        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                output = _find_null_fields(ctx, field.dataType, new_path + field.name,
                                        output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, ArrayType):
            if isinstance(schema.elementType, StructType):
                output = _find_null_fields(ctx, schema.elementType, path,
                                        output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, NullType):
            output.append(path)
        else:
            x, distinct_set = frame.toDF(), set()
            for i in x.select(path).distinct().collect():
                distinct_ = i[path.split('.')[-1]]
                if isinstance(distinct_, list):
                    distinct_set |= set([item.strip() if isinstance(item, str) else item
                                        for item in distinct_])
                elif isinstance(distinct_, str):
                    distinct_set.add(distinct_.strip())
                else:
                    distinct_set.add(distinct_)
            if isinstance(schema, StringType):
                if distinct_set.issubset(nullStringSet):
                    output.append(path)
            elif isinstance(schema, (IntegerType, LongType, DoubleType)):
                if distinct_set.issubset(nullIntegerSet):
                    output.append(path)
        return output

    def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
        nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [],
                                        nullStringSet, nullIntegerSet, frame)
        return DropFields.apply(frame=frame, paths=nullColumns,
                                transformation_ctx=transformation_ctx)

    # ─────────────────────────────────────────────────────────────────────────────
    # Job bootstrap
    # ─────────────────────────────────────────────────────────────────────────────
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'source_bucket', 'source_key',
        'SINK_BUCKET_NAME', 'GLUE_CATALOG_DATABASE',
        'GLUE_CATALOG_TABLE_NAME', 'DYNAMO_DB_TABLE_NAME'
    ])

    source_bucket = args['source_bucket']
    source_key    = args['source_key']

    sc          = SparkContext()
    glueContext = GlueContext(sc)
    spark       = glueContext.spark_session
    job         = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # ─────────────────────────────────────────────────────────────────────────────
    # Controlled vocabulary constants — single source of truth used in BOTH
    # Step 3.5 (pre-transform Spark checks) and the DQ ruleset (Step 7).
    # Keeping them here prevents the two layers from drifting out of sync.
    # ─────────────────────────────────────────────────────────────────────────────
    VALID_TYPES    = {"BUG", "VULNERABILITY", "CODE_SMELL", "SECURITY_HOTSPOT"}
    VALID_SEVERITIES = {"BLOCKER", "CRITICAL", "MAJOR", "MINOR", "INFO"}
    VALID_STATUSES = {"OPEN", "CONFIRMED", "REOPENED", "RESOLVED", "CLOSED"}

    # ─────────────────────────────────────────────────────────────────────────────
    # Data Quality ruleset — post-transform, post-dedup defence layer (Step 7)
    #
    # Design decisions vs. the original ruleset:
    #
    #   ColumnCount REMOVED — it was fragile.  drop_nulls() (Step 5) can legally
    #   remove optional columns (e.g. `tags` when all rows have no tags), which
    #   changes the count unpredictably.  Specific IsComplete rules on mandatory
    #   columns provide a stronger guarantee without that brittleness.
    #
    #   IsComplete added for scan_date_raw, scan_date, project_key, rule.
    #   These were missing from the original ruleset and allowed NULL values to
    #   silently pass through to the Silver layer.
    #
    #   ColumnValues "effort_minutes" >= 0 and "debt_minutes" >= 0 added.
    #   Step 3.5 catches invalid formats before transformation; these rules are
    #   a second line of defence specifically for negative numeric results.
    #
    #   ColumnCount = 13 in the original ruleset was also WRONG — the SQL
    #   transform produces 14 columns (adds scan_date_raw and renames scan_date
    #   to a TIMESTAMP, replacing the original string).
    # ─────────────────────────────────────────────────────────────────────────────
    DEFAULT_DATA_QUALITY_RULESET = """Rules = [
        IsComplete "scan_date_raw",
        IsComplete "scan_date",
        IsComplete "project_key",
        IsComplete "type",
        IsComplete "severity",
        IsComplete "file",
        IsComplete "rule",
        IsComplete "status",

        ColumnValues "type"     in [ "BUG", "VULNERABILITY", "CODE_SMELL", "SECURITY_HOTSPOT" ],
        ColumnValues "severity" in [ "BLOCKER", "CRITICAL", "MAJOR", "MINOR", "INFO" ],
        ColumnValues "status"   in [ "OPEN", "CONFIRMED", "REOPENED", "RESOLVED", "CLOSED" ],

        ColumnValues "line"           >= 0,
        ColumnValues "effort_minutes" >= 0,
        ColumnValues "debt_minutes"   >= 0
    ]"""

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 1 — Delimiter / structural corruption check + raw-text safety checks
    #
    # Checks added in this version:
    #
    #   A. Null byte detection
    #      Null bytes (\x00) in CSV fields are invisible to most parsers and
    #      cause silent string truncation or encoding errors downstream.
    #      spark.read.text() preserves them as-is so we can detect them here.
    #
    #   B. ISO-8601 scan_date format validation (pre-parse)
    #      SonarQube always exports scan_date as "yyyy-MM-ddTHH:mm:ssZ".
    #      Any other format (e.g. "N/A", "27.01.2025", "January 27 2025",
    #      "2025/01/27", "01-27-25") means the upstream exporter is broken or
    #      the file has been manually edited.  Catching this at the raw-text
    #      level — before Spark even parses the CSV — means we fail on the
    #      first bad row rather than silently converting it to a NULL timestamp
    #      inside to_timestamp() in Step 4.
    #
    #      We use csv.reader (not split(",")) so that fields containing commas
    #      inside double-quotes are parsed correctly.
    #
    # Existing checks retained:
    #   - File must have at least 2 lines (header + 1 data row)
    #   - Every row must have the same column count as the header
    #   - Header must contain all 13 expected SonarQube column names
    # ─────────────────────────────────────────────────────────────────────────────
    print(f"Glue ETL SonarQube | Processing file: s3://{source_bucket}/{source_key}")

    raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").limit(1000).collect()

    if len(raw_lines) < 2:
        raise Exception("Glue ETL SonarQube | Corrupted CSV — file has no data rows")

    def count_csv_columns(line: str) -> int:
        """Count columns correctly, respecting quoted fields that contain commas."""
        try:
            return len(next(csv.reader(io.StringIO(line))))
        except StopIteration:
            return 0

    header_line        = raw_lines[0][0]
    expected_col_count = count_csv_columns(header_line)

    print(f"Glue ETL SonarQube | Header detected: {header_line}")
    print(f"Glue ETL SonarQube | Expected column count: {expected_col_count}")

    # ── Validate expected SonarQube schema ────────────────────────────────────────
    EXPECTED_SONAR_COLUMNS = {
        "scan_date", "project_key", "branch", "type", "severity",
        "file", "line", "message", "rule", "status", "effort", "debt", "tags"
    }
    actual_header_cols = set(c.strip() for c in header_line.split(","))
    missing_cols = EXPECTED_SONAR_COLUMNS - actual_header_cols
    if missing_cols:
        raise Exception(
            f"Glue ETL SonarQube | Schema drift detected — "
            f"missing expected columns: {missing_cols}"
        )

    # ── Check A: Null byte detection ──────────────────────────────────────────────
    # Null bytes are control characters that survive CSV parsing and cause silent
    # corruption in string comparisons and Parquet writes.
    null_byte_rows = []
    for i, row in enumerate(raw_lines, start=1):
        if '\x00' in row[0]:
            null_byte_rows.append((i, repr(row[0][:120])))

    if null_byte_rows:
        details = "\n".join([f"  Row {i}: {preview}" for i, preview in null_byte_rows])
        raise Exception(
            f"Glue ETL SonarQube | FAIL — null bytes detected in "
            f"{len(null_byte_rows)} row(s). Null bytes cause silent string "
            f"truncation and Parquet encoding failures:\n{details}"
        )

    print("Glue ETL SonarQube | Null byte check passed")

    # ── Existing check: column count per row ─────────────────────────────────────
    # (Also catches embedded unquoted newlines — they split one logical row into
    # two physical lines, each with fewer columns than expected.)
    bad_row_indices = []
    for i, row in enumerate(raw_lines[1:], start=2):
        line             = row[0]
        actual_col_count = count_csv_columns(line)
        if actual_col_count != expected_col_count:
            bad_row_indices.append((i, actual_col_count, line[:80]))

    if bad_row_indices:
        details = "\n".join(
            [f"  Line {idx}: expected {expected_col_count} cols, got {actual} -> {preview}..."
            for idx, actual, preview in bad_row_indices]
        )
        raise Exception(
            f"Glue ETL SonarQube | FAIL — {len(bad_row_indices)} row(s) have wrong "
            f"column count (embedded unquoted newlines also cause this):\n{details}"
        )

    # ── Check B: scan_date ISO-8601 format — pre-parse, raw text ─────────────────
    # We validate at this layer rather than waiting for to_timestamp() in Step 4
    # because to_timestamp() silently returns NULL for unparseable strings.
    # A silent NULL is a silently corrupted partition key.
    #
    # Header column index for scan_date is always 0 in the SonarQube CSV schema.
    ISO_8601_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
    bad_scan_date_rows = []
    for i, row in enumerate(raw_lines[1:], start=2):
        try:
            fields        = next(csv.reader(io.StringIO(row[0])))
            scan_date_val = fields[0].strip() if fields else ""
            if scan_date_val and not ISO_8601_PATTERN.match(scan_date_val):
                bad_scan_date_rows.append((i, scan_date_val))
        except StopIteration:
            pass

    if bad_scan_date_rows:
        details = "\n".join(
            [f"  Row {i}: '{val}'" for i, val in bad_scan_date_rows]
        )
        raise Exception(
            f"Glue ETL SonarQube | FAIL — {len(bad_scan_date_rows)} row(s) have "
            f"non-ISO-8601 scan_date values. Expected format: yyyy-MM-ddTHH:mm:ssZ\n"
            f"  Accepted: '2025-01-27T08:30:00Z'\n"
            f"  Rejected (examples seen in this file): 'N/A', '27.01.2025', "
            f"'January 27 2025', '2025/01/27', '27-01-2025'\n"
            f"  Offending rows:\n{details}"
        )

    print(f"Glue ETL SonarQube | Column count validation passed ({expected_col_count} cols)")
    print("Glue ETL SonarQube | scan_date ISO-8601 pre-parse check passed")

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 2 — Read CSV into DynamicFrame
    #
    # quoteChar='"' is essential because the `tags` column contains
    # comma-separated values wrapped in double quotes by SonarQube,
    # e.g. "security,cryptography".  Without quoteChar the parser would
    # split that into extra columns and break the schema.
    # ─────────────────────────────────────────────────────────────────────────────
    raw_dynamic_frame = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar":           '"',
            "withHeader":          True,
            "separator":           ",",
            "mode":                "PERMISSIVE",
            "optimizePerformance": False,
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths":   [f"s3://{source_bucket}/{source_key}"],
            "recurse": False,
        },
        transformation_ctx="raw_dynamic_frame"
    )

    df = raw_dynamic_frame.toDF()

    print("Glue ETL SonarQube | DEBUG columns:", df.columns)
    print("Glue ETL SonarQube | DEBUG row count:", df.count())

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 3 — Spark-level corruption checks (after parsing)
    # ─────────────────────────────────────────────────────────────────────────────
    if len(df.columns) == 0:
        raise Exception("Glue ETL SonarQube | Corrupted CSV — no columns inferred by Spark")

    if df.limit(1).count() == 0:
        raise Exception("Glue ETL SonarQube | Corrupted CSV — empty dataframe after parsing")

    expected_cols = len(df.columns)
    null_exprs    = [F.col(c).isNull().cast("int") for c in df.columns]
    bad_rows      = df.filter(reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7)

    if bad_rows.limit(1).count() > 0:
        raise Exception("Glue ETL SonarQube | Corrupted CSV — null-heavy rows detected")

    print("Glue ETL SonarQube | Spark-level corruption checks passed")

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 3.5 — Pre-transform field-level validation  [NEW]
    #
    # Why this layer exists
    # ─────────────────────────────────────────────────────────────────────────────
    # The SQL transform in Step 4 silently coerces bad values:
    #
    #   CAST("unknown" AS INT)              → NULL    (line field)
    #   CAST("LINE_NUM" AS INT)             → NULL    (line field)
    #   REGEXP_REPLACE("-5min","[^0-9]","") → "5"    (sign stripped silently)
    #   REGEXP_REPLACE("N/A","[^0-9]","")  → ""      (effort becomes NULL)
    #   REGEXP_REPLACE("TBD","[^0-9]","")  → ""      (debt becomes NULL)
    #   REGEXP_REPLACE("1h30","[^0-9]","") → "130"   (wrong — should be 90min)
    #
    # None of these raise an exception — they produce wrong or NULL data
    # that passes silently through to the Silver layer and Athena queries.
    #
    # This step catches every one of those cases BEFORE the transform runs,
    # so the job fails loudly with a clear message rather than writing
    # corrupted data.
    #
    # Checks performed (all run before the first exception is raised so
    # that a single job failure gives the operator a complete violation
    # report, not just the first failure it hit):
    #
    #   1.  type vocabulary          — catches lowercase, UNKNOWN, blanks
    #   2.  severity vocabulary      — catches HIGH, medium, blanks
    #   3.  status vocabulary        — catches PENDING, blanks
    #   4.  line numeric format      — catches "unknown", "LINE_NUM", "null", "-"
    #   5.  effort time format       — catches "N/A", "TBD", "20", "-5min", "1h30"
    #   6.  debt time format         — same patterns as effort
    #   7.  project_key whitespace   — catches leading/trailing spaces that break grouping
    #   8.  rule whitespace          — catches padded rule IDs that break deduplication
    #   9.  rule completeness        — catches empty rule (dedup key column)
    #  10.  branch whitespace        — catches padded branch values
    # ─────────────────────────────────────────────────────────────────────────────

    # Regex patterns for effort and debt.
    #
    # SonarQube writes effort/debt as "Nmin" (e.g. "42min") or "Nh Nmin"
    # (e.g. "1h 15min").  Any other format means the field was manually edited
    # or the exporter changed its output.
    #
    # Patterns that MUST be rejected (all found in the test file):
    #   "N/A"   — placeholder, not a duration
    #   "TBD"   — placeholder, not a duration
    #   "0"     — bare integer with no unit — ambiguous (minutes? seconds?)
    #   "20"    — same
    #   "-5min" — negative duration is physically meaningless
    #   "1h30"  — missing space before "min" — would REGEXP_REPLACE to "130",
    #              silently producing 130 minutes instead of 90
    EFFORT_DEBT_PATTERN = re.compile(r'^\d+min$|^\d+h \d+min$')

    # Numeric-only pattern for the line field.
    # CAST in Spark accepts these, everything else silently becomes NULL.
    NUMERIC_PATTERN = re.compile(r'^\d+$')

    # Build the list of all violations across all checks so the operator
    # gets a complete picture in a single job run rather than fix-one-fail-again.
    all_violations = []

    def collect_violations(check_name: str, bad_df, description: str, sample_cols: list):
        """
        Count failing rows, collect up to 5 sample rows, and append to all_violations.
        Does NOT raise — the caller raises once after all checks complete.
        """
        count = bad_df.count()
        if count > 0:
            sample_rows = bad_df.select(*sample_cols).limit(5).collect()
            sample_str  = "\n".join([f"    {row.asDict()}" for row in sample_rows])
            all_violations.append(
                f"\n  [{check_name}] {count} row(s) — {description}\n{sample_str}"
            )

    # ── Check 1: type vocabulary ──────────────────────────────────────────────────
    # Catches: "code_smell" (lowercase), "UNKNOWN" (not in SonarQube's valid set),
    #          and any blank type values.
    # Silent corruption if not caught: REGEXP filtering or DQ ColumnValues would
    # pass lowercase values through because DQ compares strings case-sensitively.
    collect_violations(
        check_name   = "type_vocabulary",
        bad_df       = df.filter(~F.col("type").isin(*VALID_TYPES)),
        description  = (
            f"type must be one of {sorted(VALID_TYPES)}. "
            f"Lowercase values like 'code_smell' are NOT accepted — they break "
            f"DQ ColumnValues checks and Athena partition filters."
        ),
        sample_cols  = ["project_key", "file", "type", "severity"]
    )

    # ── Check 2: severity vocabulary ─────────────────────────────────────────────
    # Catches: "HIGH" (not a SonarQube severity), "medium" (wrong case), blanks.
    # Empty severity is a separate sub-check — it would produce NULL after
    # to_timestamp() and silently fail the IsComplete DQ rule with no context.
    collect_violations(
        check_name   = "severity_non_empty_but_invalid",
        bad_df       = df.filter(
                        F.col("severity").isNotNull() &
                        (F.trim(F.col("severity")) != "") &
                        ~F.col("severity").isin(*VALID_SEVERITIES)
                    ),
        description  = (
            f"severity must be one of {sorted(VALID_SEVERITIES)}. "
            f"'HIGH' and 'medium' are not valid SonarQube severity levels."
        ),
        sample_cols  = ["project_key", "file", "type", "severity"]
    )

    collect_violations(
        check_name   = "severity_missing",
        bad_df       = df.filter(
                        F.col("severity").isNull() |
                        (F.trim(F.col("severity")) == "")
                    ),
        description  = (
            "severity is null or empty. Severity is a mandatory field — "
            "a missing severity means the SonarQube export is incomplete."
        ),
        sample_cols  = ["project_key", "file", "type", "severity"]
    )

    # ── Check 3: status vocabulary ────────────────────────────────────────────────
    # Catches: "PENDING" (not a SonarQube status — likely a pipeline-internal
    # status that leaked into the upstream export), empty status.
    collect_violations(
        check_name   = "status_non_empty_but_invalid",
        bad_df       = df.filter(
                        F.col("status").isNotNull() &
                        (F.trim(F.col("status")) != "") &
                        ~F.col("status").isin(*VALID_STATUSES)
                    ),
        description  = (
            f"status must be one of {sorted(VALID_STATUSES)}. "
            f"'PENDING' is not a valid SonarQube status."
        ),
        sample_cols  = ["project_key", "file", "rule", "status"]
    )

    collect_violations(
        check_name   = "status_missing",
        bad_df       = df.filter(
                        F.col("status").isNull() |
                        (F.trim(F.col("status")) == "")
                    ),
        description  = (
            "status is null or empty. Status is mandatory — "
            "a missing status means the SonarQube export is incomplete."
        ),
        sample_cols  = ["project_key", "file", "rule", "status"]
    )

    # ── Check 4: line numeric format ──────────────────────────────────────────────
    # The line field must be a non-negative integer OR empty/null (some rules
    # apply at file level and have no line number).
    #
    # What CAST silently swallows:
    #   "unknown"  → NULL   "LINE_NUM" → NULL   "null" → NULL   "-" → NULL
    #   "N/A"      → NULL
    #
    # Any of these NULLs would then silently FAIL the DQ rule
    # ColumnValues "line" >= 0 (which only fires for non-NULL values),
    # allowing rows with meaningless line values to reach the Silver layer.
    collect_violations(
        check_name   = "line_non_empty_but_non_numeric",
        bad_df       = df.filter(
                        F.col("line").isNotNull() &
                        (F.trim(F.col("line")) != "") &
                        ~F.col("line").rlike(r'^\d+$')
                    ),
        description  = (
            "line must be a non-negative integer or empty. "
            "Values like 'unknown', 'LINE_NUM', 'null', '-', 'N/A' are not accepted. "
            "CAST() silently converts these to NULL — this check prevents that."
        ),
        sample_cols  = ["project_key", "file", "line", "rule"]
    )

    # ── Check 5: effort format ────────────────────────────────────────────────────
    # effort must match "Nmin" or "Nh Nmin" or be empty.
    #
    # Silent corruption REGEXP_REPLACE causes:
    #   "N/A"   → ""    → CAST → NULL    (looks like missing data)
    #   "TBD"   → ""    → CAST → NULL    (same)
    #   "20"    → "20"  → CAST → 20      (correct numerically but unitless — ambiguous)
    #   "-5min" → "5"   → CAST → 5       (sign stripped — negative becomes positive)
    #   "1h30"  → "130" → CAST → 130     (wrong — should be 90 minutes)
    collect_violations(
        check_name   = "effort_invalid_format",
        bad_df       = df.filter(
                        F.col("effort").isNotNull() &
                        (F.trim(F.col("effort")) != "") &
                        ~F.col("effort").rlike(r'^\d+min$') &
                        ~F.col("effort").rlike(r'^\d+h \d+min$')
                    ),
        description  = (
            "effort must be 'Nmin' (e.g. '42min') or 'Nh Nmin' (e.g. '1h 15min') or empty. "
            "Rejected examples found in this file: 'N/A', 'TBD', '0', '20' (no unit), "
            "'-5min' (negative), '1h30' (missing space — REGEXP_REPLACE would produce "
            "130 instead of 90)."
        ),
        sample_cols  = ["project_key", "file", "rule", "effort"]
    )

    # ── Check 6: debt format ──────────────────────────────────────────────────────
    # Same format contract as effort — same silent corruption risks.
    collect_violations(
        check_name   = "debt_invalid_format",
        bad_df       = df.filter(
                        F.col("debt").isNotNull() &
                        (F.trim(F.col("debt")) != "") &
                        ~F.col("debt").rlike(r'^\d+min$') &
                        ~F.col("debt").rlike(r'^\d+h \d+min$')
                    ),
        description  = (
            "debt must be 'Nmin' or 'Nh Nmin' or empty. "
            "Same rules as effort — same silent corruption risks."
        ),
        sample_cols  = ["project_key", "file", "rule", "debt"]
    )

    # ── Check 7: project_key whitespace contamination ─────────────────────────────
    # Leading or trailing whitespace in project_key means '  syngenta-pim-core  '
    # is treated as a DIFFERENT project than 'syngenta-pim-core'.
    # This causes wrong grouping in Athena queries (SELECT COUNT(*) GROUP BY project_key
    # returns two separate groups that should be one) and corrupts dashboard metrics.
    collect_violations(
        check_name   = "project_key_whitespace",
        bad_df       = df.filter(
                        F.col("project_key").isNotNull() &
                        (F.col("project_key") != F.trim(F.col("project_key")))
                    ),
        description  = (
            "project_key has leading or trailing whitespace. "
            "'  syngenta-pim-core  ' != 'syngenta-pim-core' in GROUP BY and JOIN operations. "
            "This silently splits one project into multiple groups in Athena and QuickSight."
        ),
        sample_cols  = ["project_key", "file", "rule"]
    )

    # ── Check 8: rule whitespace contamination ────────────────────────────────────
    # Whitespace-padded rule IDs break deduplication: "  java:S2259  " and
    # "java:S2259" are the same rule but dropDuplicates() treats them as different.
    # This means the same issue gets written to Silver twice.
    collect_violations(
        check_name   = "rule_whitespace",
        bad_df       = df.filter(
                        F.col("rule").isNotNull() &
                        (F.col("rule") != "") &
                        (F.col("rule") != F.trim(F.col("rule")))
                    ),
        description  = (
            "rule has leading or trailing whitespace. "
            "'  java:S2259  ' != 'java:S2259' in dropDuplicates() — "
            "this causes the same issue to be written to Silver multiple times."
        ),
        sample_cols  = ["project_key", "file", "rule", "type"]
    )

    # ── Check 9: rule completeness ────────────────────────────────────────────────
    # rule is part of the deduplication key (project_key + rule + file + line + scan_date).
    # An empty rule means deduplication is based on an incomplete key, which can cause
    # different issues to collapse into one row or the same issue to duplicate.
    collect_violations(
        check_name   = "rule_missing",
        bad_df       = df.filter(
                        F.col("rule").isNull() |
                        (F.trim(F.col("rule")) == "")
                    ),
        description  = (
            "rule is null or empty. rule is part of the deduplication key "
            "(project_key + rule + file + line + scan_date). "
            "An empty rule produces an incomplete dedup key — "
            "different issues may collapse into one row."
        ),
        sample_cols  = ["project_key", "file", "rule", "message"]
    )

    # ── Check 10: branch whitespace contamination ──────────────────────────────────
    # Same problem as project_key: '  feature/bulk-import  ' and 'feature/bulk-import'
    # become separate partition values.
    collect_violations(
        check_name   = "branch_whitespace",
        bad_df       = df.filter(
                        F.col("branch").isNotNull() &
                        (F.col("branch") != "") &
                        (F.col("branch") != F.trim(F.col("branch")))
                    ),
        description  = (
            "branch has leading or trailing whitespace. "
            "This causes branch values to split into different groups in "
            "Athena queries and QuickSight dashboards."
        ),
        sample_cols  = ["project_key", "branch", "file", "rule"]
    )

    # ── Raise a single exception with a complete violation report ─────────────────
    # Running all checks before raising means the operator sees EVERY problem
    # in one job failure — not just the first one (fix-one-fail-again cycle).
    if all_violations:
        violation_summary = "".join(all_violations)
        total_checks_failed = len(all_violations)
        raise Exception(
            f"Glue ETL SonarQube | PRE-TRANSFORM VALIDATION FAILED — "
            f"{total_checks_failed} check(s) violated in "
            f"s3://{source_bucket}/{source_key}. "
            f"Fix these issues in the source file before re-uploading.\n"
            f"{violation_summary}"
        )

    print("Glue ETL SonarQube | Pre-transform validation passed — all 10 checks clean")

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 4 — Type casting and field extraction via Spark SQL
    #
    # Transformations applied:
    #
    #   scan_date  → TIMESTAMP
    #               to_timestamp() parses ISO-8601 strings: "2024-09-15T08:30:00Z"
    #               Step 3.5 already validated the format, so NULL output here
    #               means a genuinely unparseable value slipped through — the
    #               DQ IsComplete "scan_date" rule in Step 7 will catch it.
    #
    #   line       → INTEGER
    #               Step 3.5 confirmed all non-empty line values are numeric,
    #               so CAST here is safe.  Empty/null line values (valid for
    #               file-level rules) become NULL — expected and correct.
    #
    #   effort_minutes → INTEGER
    #               Step 3.5 confirmed the format is "Nmin" or "Nh Nmin".
    #               REGEXP_REPLACE strips the non-numeric suffix, leaving the
    #               numeric part for CAST.  For "1h 15min" this produces:
    #               REGEXP_REPLACE("1h 15min","[^0-9]","") → "115" → 115.
    #               Note: this intentionally concatenates hours and minutes
    #               into a single integer.  If the downstream requirement is
    #               total_minutes (75 for 1h 15min), a more precise transform
    #               would be needed.  The current approach matches the original
    #               pipeline design.
    #
    #   debt_minutes → INTEGER — same as effort_minutes.
    # ─────────────────────────────────────────────────────────────────────────────
    sonar_transform_sql = '''
    SELECT
        scan_date                                       AS scan_date_raw,
        to_timestamp(scan_date, "yyyy-MM-dd\'T\'HH:mm:ss\'Z\'")
                                                        AS scan_date,
        project_key,
        branch,
        type,
        severity,
        status,
        file,
        CAST(line AS INT)                               AS line,
        rule,
        message,
        tags,
        CAST(
            REGEXP_REPLACE(effort, "[^0-9]", "")
            AS INT
        )                                               AS effort_minutes,
        CAST(
            REGEXP_REPLACE(debt, "[^0-9]", "")
            AS INT
        )                                               AS debt_minutes
    FROM sonarSource
    '''

    try:
        transformed_dynamic_frame = sparkSqlQuery(
            glueContext,
            query=sonar_transform_sql,
            mapping={"sonarSource": raw_dynamic_frame},
            transformation_ctx="transformed_dynamic_frame"
        )
    except Exception as e:
        print(f"Glue ETL SonarQube | SQL transformation failed: {e}")
        raise RuntimeError("Glue ETL SonarQube | Failing job — SQL transformation error")

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 5 — Drop fully-null columns (same pattern as sales pipeline)
    # ─────────────────────────────────────────────────────────────────────────────
    cleaned_dynamic_frame = drop_nulls(
        glueContext,
        frame=transformed_dynamic_frame,
        nullStringSet={"", "null"},
        nullIntegerSet={-1},
        transformation_ctx="cleaned_dynamic_frame"
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 6 — Deduplication
    #
    # SonarQube issues are uniquely identified by the combination of
    # (project_key + rule + file + line + scan_date).
    # Step 3.5 confirmed rule is non-empty and non-whitespace-padded,
    # so this key is now reliable.
    # ─────────────────────────────────────────────────────────────────────────────
    df_clean = cleaned_dynamic_frame.toDF()

    deduped_df = df_clean.dropDuplicates(["project_key", "rule", "file", "line", "scan_date"])

    print("Glue ETL SonarQube | Before dedup:", df_clean.count())
    row_count_after_dedup = deduped_df.count()
    print("Glue ETL SonarQube | After dedup:", row_count_after_dedup)

    deduped_dynamic_frame = DynamicFrame.fromDF(
        deduped_df, glueContext, "deduped_dynamic_frame"
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 7 — Data Quality evaluation (STRICT — third and final defence layer)
    #
    # Three-layer defence design:
    #
    #   Layer 1 — Step 1 (raw text):
    #     Catches structural corruption, null bytes, bad scan_date formats.
    #     Runs on raw bytes before Spark even parses the CSV.
    #
    #   Layer 2 — Step 3.5 (parsed DataFrame, pre-transform):
    #     Catches vocabulary violations, format violations, whitespace
    #     contamination.  Runs on the parsed string values before any
    #     type casting or transformation.
    #
    #   Layer 3 — Step 7 (transformed DynamicFrame, post-dedup):
    #     Catches any violation that slipped through layers 1 and 2
    #     (edge cases, Spark parser behaviour differences, etc.).
    #     Acts as the final gate before Silver write.
    #
    # The DQ ruleset is intentionally conservative:
    #   - ColumnCount removed (fragile due to drop_nulls on optional columns)
    #   - IsComplete on all mandatory columns catches NULLs from failed casts
    #   - ColumnValues vocabulary checks are defence-in-depth vs. Step 3.5
    #   - effort_minutes and debt_minutes >= 0 catches any negative that
    #     somehow survived to this point
    # ─────────────────────────────────────────────────────────────────────────────
    try:
        EvaluateDataQuality().process_rows(
            frame=deduped_dynamic_frame,
            ruleset=DEFAULT_DATA_QUALITY_RULESET,
            publishing_options={
                "dataQualityEvaluationContext": "sonar_dq_context",
                "enableDataQualityResultsPublishing": True,
                "failureAction": "FAIL",
            },
            additional_options={
                "dataQualityResultsPublishing.strategy": "STRICT",
                "observations.scope": "ALL",
            }
        )
    except Exception as e:
        print(f"Glue ETL SonarQube | DQ check failed: {e}")
        raise RuntimeError("Glue ETL SonarQube | Failing job — data quality rules violated")

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 8 — Write Silver layer
    #
    # Partition strategy: partitionKeys=["type", "severity"]
    #
    # Rationale: downstream Athena queries filter almost always on type and/or
    # severity (e.g. "show all VULNERABILITY/CRITICAL issues").  Partitioning
    # on these two columns lets Athena skip irrelevant partitions entirely,
    # reducing data scanned and cost.
    # ─────────────────────────────────────────────────────────────────────────────
    silver_sink = glueContext.getSink(
        path=args['SINK_BUCKET_NAME'],
        connection_type="s3",
        updateBehavior="LOG",
        partitionKeys=["type", "severity"],
        enableUpdateCatalog=True,
        transformation_ctx="silver_sink"
    )
    silver_sink.setCatalogInfo(
        catalogDatabase=args['GLUE_CATALOG_DATABASE'],
        catalogTableName=args['GLUE_CATALOG_TABLE_NAME']
    )
    silver_sink.setFormat("glueparquet", compression="snappy")
    silver_sink.writeFrame(deduped_dynamic_frame)

    # ─────────────────────────────────────────────────────────────────────────────
    # STEP 9 — Write row count to DynamoDB
    #
    # Written AFTER writeFrame so the count only lands in DynamoDB if the
    # Silver write actually succeeded.
    # ─────────────────────────────────────────────────────────────────────────────
    file_key        = f"s3://{source_bucket}/{source_key}"
    dynamodb_client = boto3.client("dynamodb", region_name="ap-south-1")

    dynamodb_client.update_item(
        TableName=args['DYNAMO_DB_TABLE_NAME'],
        Key={"file_key": {"S": file_key}},
        UpdateExpression="SET #s = :s, row_count = :r, updated_at = :u",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": {"S": "SUCCESS"},
            ":r": {"N": str(row_count_after_dedup)},
            ":u": {"S": str(__import__("datetime").datetime.utcnow().isoformat())},
        }
    )

    print(f"Glue ETL SonarQube | Row count {row_count_after_dedup} written to DynamoDB for {file_key}")

    job.commit()
    ```
- IAM role attached to this AWS glue ETL job is called ```process_sonar_qube_logs_ETL_role```
    - Policies attached to this role : 
        - ```AWSGlueServiceRole``` : 
            - This policy is AWS managed policy
        - ```DynamoDbAccessPolicyForLambdaFunction``` : 
            - This policy allows aws glue to create , get and update items tables named ```sonar_qube_logs_processing_registery``` and ```glue_semaphore``` in dynamoDB
            - ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "DynamoDbAccessPolicyForLambdaFunction",
                            "Effect": "Allow",
                            "Action": [
                                "dynamodb:PutItem",
                                "dynamodb:GetItem",
                                "dynamodb:UpdateItem"
                            ],
                            "Resource": [
                                "arn:aws:dynamodb:ap-south-1:746244690650:table/sonar_qube_logs_processing_registery",
                                "arn:aws:dynamodb:ap-south-1:746244690650:table/glue_semaphore"
                            ]
                        }
                    ]
                }
              ```
        - ```LimitedS3PermissionPolicy``` : 
            - This is the custom policy that allows aws glue to get, put and list objects in s3 bucket
            - ```json
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:ListBucket"
                            ],
                            "Resource": [
                                "arn:aws:s3:::deployement-logs-data-source",
                                "arn:aws:s3:::deployement-logs-data-source/*",
                                "arn:aws:s3:::deployement-logs-data-silver",
                                "arn:aws:s3:::deployement-logs-data-silver/*"
                            ]
                        }
                    ]
                }
              ```

## Issues I faced when implementing Version 7 
### 1. Event messages from the source S3 on file upload were not being shown in ```DataProcessingJobQueue.fifo``` sqs queue
#### Cause for this issue 
Content-Based Deduplication is OFF : To let EventBridge send messages to your SQS FIFO queue, you must enable content-based deduplication.

Here is what is happening under the hood: <br>
When EventBridge sends a message to a FIFO queue, it does not provide a MessageDeduplicationId in the API call. FIFO queues have two ways to handle deduplication: <br>
Option 1: Caller provides MessageDeduplicationId  ← EventBridge does NOT do this <br>
Option 2: Queue uses Content-Based Deduplication  ← You must enable this <br>

Since Content-based deduplication is Disabled and EventBridge is not providing a deduplication ID, SQS is rejecting every message from EventBridge silently. The file arrives, EventBridge fires, but SQS refuses the message because the deduplication ID is missing.

#### Solution
![sqs_fifo_enable_content_based_duplication](images/production_grade_implementation_version_7/sqs/sqs_fifo_enable_content_based_duplication.png)
- Enable Content-Based Deduplication
- Go to DataProcessingJobQueue.fifo → Edit → FIFO queue settings → toggle Content-based deduplication to ON.

### 2. Faced max_glue concurrency error
#### When did this error happened
- Every AWS Glue ETL job has a configuration field called Maximum concurrency (visible in Job details → Glue console). In my pipeline this was set to 2. 
- This means Glue will allow at most 2 runs of the job ```process_sonar_qube_logs_ETL``` to execute simultaneously at any point in time, across your entire AWS account. It is not a per-execution limit or a per-Step-Function limit. It is a global account-level counter for that specific job.
- When a third attempt is made to start the job while 2 runs are already active, Glue refuses the request entirely and throws:
    - ```bash
        Glue.ConcurrentRunsExceededException: Concurrent runs exceeded.
        (Service: AWSGlue; Status Code: 400; Error Code: ConcurrentRunsExceededException)
      ```
    - The job does not queue. It does not wait. It hard-fails immediately and returns this exception to whoever called ```startJobRun``` in my case, the Step Function's ```RunGlueJob``` state.

**The exact moment it happened in my pipeline**
My pipeline architecture is event-driven. When a file lands in S3, the following chain fires:
```bash
S3 upload → EventBridge event → SQS FIFO message → Lambda invocation → Step Function execution → Glue job
```
The problem arose during stress testing with 10 files uploaded simultaneously to S3 with the Lambda SQS trigger set to batch size = 1. Here is what that burst looked like in precise sequence:
- **t = 0 seconds:** 10 files land in S3 within a 3-second window. S3 fires 10 EventBridge events. EventBridge routes all 10 to ```DataProcessingJobQueue.fifo```. SQS holds all 10 messages.
- **t = 0–1 seconds:** Lambda's SQS trigger with batch size = 1 and max concurrency = 2 begins processing. Lambda can run at most 2 concurrent invocations. Each invocation reads 1 message, writes the file to DynamoDB as ```IN_PROGRESS```, and immediately calls ```sf.start_execution()```. The Lambda function returns in under 1 second it does not wait for the Step Function to finish. SQS marks those 2 messages as delivered and picks up the next 2.
- **t = 1–3 seconds:** The remaining 8 messages are consumed by subsequent Lambda invocations. Because Lambda returns immediately after calling ```start_execution()```, all 10 Step Function executions are alive and running within 3 seconds of the first upload.
- **t = 3 seconds:** 10 Step Function executions are simultaneously in their RunGlueJob state. Each one calls ```arn:aws:states:::glue:startJobRun.sync```. Glue receives 10 concurrent ```startJobRun``` API calls. It accepts the first 2 its counter goes from 0 to 2. It rejects the next 8 with ```ConcurrentRunsExceededException```. Those 8 Step Function executions immediately jump to their Catch block, write ```FAILED``` to DynamoDB, send an error to the DLQ, and terminate.
- **Result:** 2 files processed successfully, 8 files with status ```FAILED``` and error message Glue.```ConcurrentRunsExceededException``` in DynamoDB.

**KEY TAKEAWAYS** : 
- Previously this version 7 pipeline had **no backpressure mechanism between the SQS queue and the Glue compute layer**. When a burst of files arrived, the pipeline had no way to signal 'I am at capacity, stop sending work downstream.' Every SQS message was immediately converted into a running Step Function execution, regardless of whether Glue had available capacity to accept the job. This is the definition of a missing backpressure mechanism — the downstream limit (Glue concurrency = 2) could not propagate its pressure back upstream to slow the rate of work creation.
- With ```batch_size=1```, Lambda acts as a 1:1 amplifier — every SQS message becomes exactly one Step Function execution, with no buffering or throttling at the conversion point. The SQS queue existed but served only as a delivery mechanism, not as a genuine buffer. A true buffer absorbs excess demand and releases it at a controlled rate. That only happened in the final Version 7 design where ```batchItemFailures``` returns unprocessed messages to the queue, and SQS's own visibility timeout naturally paces the retry rate.

**Comparision with Version 6 pipeline implementation to handle backpressure :** <br>
**The critical difference: Version 6 had a Retry block specifically targeting this error:**
```json
"Retry": [
  {
    "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
    "IntervalSeconds": 60,
    "MaxAttempts": 10,
    "BackoffRate": 1.5
  }
]
```
- So when Glue rejects a start request with ```ConcurrentRunsExceededException```, the Step Function does not fail it waits 60 seconds and tries again. The second retry waits 90 seconds (60 × 1.5). The third waits 135 seconds, and so on.
- The practical outcome for 25 files in Version 6:
    - The error absolutely happens. But instead of writing files as FAILED and sending them to DLQ, the Step Function sits in the Retry loop. Over the next several minutes, as the 2 running Glue jobs finish and free up slots, the retrying Map iterations get their turn. With MaxAttempts: 10 and BackoffRate: 1.5, the total retry window is approximately:
    ```bash
    60 + 90 + 135 + 202 + 303 + 454 + 681 + 1022 + 1533 seconds ≈ 4,480 seconds ≈ 74 minutes
    ```
    - For 25 files with 2 Glue slots running ~3 minutes per job, you need 25/2 = 13 sequential batches × 3 minutes = ~39 minutes total. This fits comfortably within the 10-retry window. All 25 files would eventually succeed — just slowly, with a lot of unnecessary waiting.
- The problems that still exist in Version 6 even with the Retry:
    - The first is cost. During those retry waits, the Step Function executions are alive and burning Step Functions state transition charges for every wait-and-retry cycle. 10 retries across many Map iterations across 5 executions adds up to significant Step Functions cost for a simple backpressure problem.
    - The second is reliability. The Retry block is a fixed schedule it does not know whether a Glue slot is available. It just waits a fixed interval and tries again blindly. If all 10 retries are exhausted before a slot becomes available (for example if Glue jobs take longer than expected), the file fails permanently and goes to the DLQ. The recovery mechanism is time-based, not capacity-based.
    - The third is that it is still not true backpressure. Backpressure means the downstream limit controls the rate of upstream work creation. In Version 6, the downstream limit (Glue being full) does not prevent new Step Function executions from starting it just makes them sit in retry loops. I still have 5 concurrent executions burning resources while waiting. True backpressure would prevent those executions from starting at all.
    ```bash
        ┌────────────┐     ┌──────────────┐     ┌───────────────────┐
        │ S3 Bucket  │ ──► │ EventBridge  │ ──► │ SQS FIFO (Buffer) │
        └────────────┘     └──────────────┘     └─────────┬─────────┘
                                                        │
                                            (Delivers Batch of 5)
                                                        │
                                                        ▼
                                                ┌───────────────────┐
                                                │ Lambda Trigger    │
                                                │ (1:1 Amplifier)   │
                                                └─────────┬─────────┘
                                                        │ Starts 5 Executions
                                                        │ (Fires blindly)
                                                        ▼
                                                ┌───────────────────┐
                                                │  Step Function    │
                                                │ (5 Active Runs)   │
                                                └─────────┬─────────┘
                                                        │ Calls glue:startJobRun.sync
                                                        ▼
                                                ┌───────────────────┐
                                                │   AWS Glue ETL    │
                                                │ (Max Capacity: 2) │
                                                └─────────┬─────────┘
                                                ┌────────┴────────┐
                                                ▼                 ▼
                                            [Accepts 2]      [Rejects 3]
                                            (Runs)                │ Throws ConcurrentRunsExceededException
                                                                ▼
                                                    (Step Function Retry Loop)
                                                    - Waits 60s, 90s, 135s...
                                                    - Executions stay ALIVE
                                                    - Burning AWS costs
    ```

#### Solution : 
In version 7 I went through hell trying to solve this problem and tried different solutions along the way trying to resolve backpressure problem once and for all making sure that max_cocurrency_error never occurs again when a bunch of files arrives in S3 all at once causing my pipeline to explode. 

The following documents every error encountered, every fix attempted, and the reasoning that led to the final stable architecture. This is a faithful reconstruction drawn from actual AWS console observations, CloudWatch logs, and DynamoDB table scans taken during live testing.

#### Phase 1 : Initial Design (Semaphore in Step Function)
The Version 7 pipeline was designed from the start with a DynamoDB semaphore table (glue_semaphore) to coordinate Glue concurrency globally. The initial Step Function architecture placed the semaphore inside the Map iterator:
```bash
GetSemaphore → ConvertSemaphoreValues → CheckCapacity
    ├── capacity available → AcquireLock → RunGlueJob → ReleaseLock
    └── capacity full     → WaitBeforeRetryLock (10 seconds) → GetSemaphore
```
This design had three bugs that were not visible until stress testing with 10 files:
- **Error 1** : SendToDLQ crash: ```'$.error could not be found'```
    - ERROR : ```States.JsonToString($.error) failed in SendToDLQ — the input was {"SdkHttpMetadata":{...},"SdkResponseMetadata":{...}} instead of the error object.```
    - Root cause : ```UpdateFailure``` was missing ```ResultPath```. Without ```ResultPath```, Step Functions defaults to ```ResultPath: '$'```, which replaces the entire state with the DynamoDB HTTP response. This silently erased ```$.error, $.file_key, $.bucket``` everything needed by ```SendToDLQ```. 
    - Solution : The fix was adding ```"ResultPath": "$.updateFailureResult"``` to ```UpdateFailure``` so the DynamoDB response was merged under a safe key rather than overwriting the whole state.
- **Error 2** : ConcurrentRunsExceededException despite semaphore
    - ERROR : ```Glue.ConcurrentRunsExceededException — 7 of 10 files FAILED in the first stress test run.```
    - Root cause : ```AcquireLock``` had no ConditionExpression it was an unconditional DynamoDB increment. The sequence ```GetSemaphore → CheckCapacity → AcquireLock``` is not atomic. With 10 executions all reading the semaphore simultaneously, all 10 passed CheckCapacity before any committed the increment. All 10 called AcquireLock unconditionally, driving count to 10. Glue allowed 2 and rejected 8.
    - Solution : Added ```ConditionExpression: 'current_count < max_glue_concurrency'``` to ```AcquireLock``` with a Catch on ```DynamoDB.ConditionalCheckFailedException``` routing to ```WaitBeforeRetryLock```. Now DynamoDB evaluates the condition and the increment atomically only 2 executions can win the write, the rest get an exception and retry.
- **Error 3** : ```current_count``` could go negative on double-release
    - RISK : If a Step Function execution retried and ReleaseLock ran twice, current_count would decrement below zero, permanently allowing unlimited Glue concurrency.
    - Solution : Added ```ConditionExpression: 'current_count > :zero'``` to both ```ReleaseLockAfterSuccess``` and ```ReleaseLockAfterFailure```. A double-release gets a ```ConditionalCheckFailedException``` caught silently the count never goes below 0.
#### Phase 2 : Lockstep Wave Problem (Jitter Attempts)
After the atomic AcquireLock fix, failures continued in stress tests. Analysis of DynamoDB timestamps revealed a new failure mode: the thundering herd at release time.

| Time                  | What happened |
|----------------------|---------------|
| t = 0s               | 2 executions acquire slots. 8 executions enter `WaitBeforeRetryLock` — all sleeping for exactly 10 seconds. |
| t = 10s, 20s ... 140s | All 8 wake simultaneously. See count = 2. All fail `CheckCapacity`. All sleep 10s again. Perfect lockstep. |
| t = 150s             | Glue job 1 finishes. Count drops to 1. All 8 wake at the same moment, all read count = 1, all pass `CheckCapacity`, all hit `AcquireLock` simultaneously. 1 wins, 7 fail. Same wave repeats. |
| t = 163s             | Glue job 2 finishes 13 seconds after job 1. Count briefly hits 0. Multiple executions see count = 0 or 1 before the new winner increments. More than 2 slip through → `ConcurrentRunsExceededException`. |

- **Jitter Attempt 1** : Split on '0'
    - Added ```ComputeJitter``` state using ```States.MathAdd(10, States.ArrayLength(States.StringSplit($$.Execution.Name, '0')))``` to produce different wait times per execution based on the count of zeros in the execution UUID.
    - RESULT : Still failing. Analysis of actual execution UUIDs showed '0' appears 1–3 times in most UUIDs, producing wait times of only 12–14 seconds , 5 executions clustered at 12 seconds, creating the same synchronized wave at smaller scale.












## Creating an end-to-end ETL pipeline from source to dashboard (TODO)
```bash
Sources (OLTP DB, CSV, APIs, logs)
        ↓
Ingestion Layer (Glue / Kinesis / DMS)
        ↓
Raw S3 (Bronze)
        ↓
Transformation (Glue / Spark / dbt)
        ↓
Curated S3 (Silver/Gold)
        ↓
Athena / Redshift
        ↓
QuickSight Dashboard
```

Implement this tomorrow 
use this code for analysis make sure that you are able to provide data according to this so that analysis can be completed and the final curated data can be used in AWS dashboard use this notebook as a reference which transformation to use when.
https://www.kaggle.com/code/mhassansaboor/amazon-sales-insights-strategy

## Partition data in S3 bucket
### What is Data pruning?
Data pruning means : Skipping irrelevant data so the system doesn't scan unnecessary files or blocks.

In simple terms . 
- Instead of reading everything the engine reads only what's needed.

**Why it matters?** <br>
- In systems like Athena , Spark, Redshift spectrum etc.
- You pay based on: 
    - How much data is scanned.
- So pruning directly reduces:
    - Query time 
    - Cost
    - Resource usage

**What Is Partitioning?** <br>
- Partitioning means:
     - Physically dividing data into folders (or logical segments) based on a column.
- ```bash
s3://sales-data/
    year=2024/
        month=01/
        month=02/
    year=2025/
        month=01/
```
- Each partition holds only data for that value.

**How Partitioning Enables Pruning?** <br>
```sql
SELECT *
FROM sales
WHERE year = 2025;
```
- If data is partitioned by year, Athena:
    - Looks only inside year=2025/
    - Ignores year=2024/
- This is called:
    - Partition Pruning
- The engine prunes away irrelevant partitions.

**Without Partitioning** <br>
If everything is stored in one folder:
- Athena must:
    - Scan entire dataset
    - Even if you only need 2025
- That means:
    - More cost
    - Slower queries

