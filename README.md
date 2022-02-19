## ANP Fuel Sales ETL Pipeline
Repository created to solve the "ANP Fuel Sales ETL Test".

## Proposal
The proposal consists in the development of an ETL pipeline to extract the pivot cache from the `xls` report [made available](http://www.anp.gov.br/dados-estatisticos) by Brazilian government's regulatory agency for oil/fuels, ANP (National Agency for Petroleum, Natural Gas and Biofuels).

The file contains pivot tables like the one below:

![pivot](https://user-images.githubusercontent.com/67954957/154768483-3fda5e55-81e4-4067-b74a-ab7929be3c06.png)

## Proposed solution
The solution proposal consisted of using instances in OCI and AWS clouds to perform the entire pipeline. Below is the model that was adopted:
<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154769509-60a5885c-b5ac-4ee2-a20f-5eecc65778d6.png" width="700" height="475">
</p>
The use of OCI was for Apache Airflow services in the Docker container and processing in Python, AWS in the use of EMR for processing Spark and s3 bucket for storing the processed files.

## Requirements
* [Docker](https://docs.docker.com/engine/install/ubuntu/)
* [Docker-compose](https://docs.docker.com/compose/install/)
* libreoffice
* Installing the libraries in the `requirements.txt` file in the `utils_lib` folder

## Running
The operation of the pipeline consists of executing the extract and transform scripts by airflow through `sshOperator`.
For this execution it was necessary to transfer the access keys for the cloud instances to the `ssh_keys` folder. So when executing the dag, the `sshOperator` performs an ssh access and executes the scripts in the established order.

* In the `extract` step, the report is downloaded, converted and sent to the raw layer of s3.
* In the `transform` step, the raw layer file is loaded through spark and the transformations to the structure requested by the challenge are carried out.

## Process Steps
Dag execution

<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154781330-3a48bd88-b70e-4c1d-9d6f-52059970ae1e.png">
</p>
<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154781353-a06bccc1-5553-4fc8-9271-12278bfd8d18.png">
</p>

Write data to s3
<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154781416-506912f2-4ece-42bd-bbab-4c9133db107f.png">
</p>

Raw Data

<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154781477-33fded80-8450-4682-8fe0-ecab04d4248b.png">
</p>

Transform Data

<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154781550-97d7816a-76e5-4730-a3e5-162b89ef24b2.png">
</p>

Loading Data

<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154781635-7fa15ebd-5a1b-41d4-bef2-8254236103a4.png">
</p>

<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154781664-8d18902d-c9f2-49aa-9a9e-932cfc4741ee.png">
</p>

The `s3_files` folder contains the generated files.

With the data available in the correct format, tables were created in AWS Athena, finalizing the ETL pipeline.

<p align="center">
<img src="https://user-images.githubusercontent.com/67954957/154782358-07b8b6ec-dfda-4a37-a239-c5ce9314a27c.png">
</p>

