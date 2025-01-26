# coding_challenge
#### Overview
I have approached this task using Databricks with Unity Catalog, a Delta Live Tables pipeline and a standard workflow. 

#### Ingestion and incomplete data
To make ingestion as lightweight as possible, the data files were uploaded to an ADLS2 account and I added the container as an external location to Unity Catalog in the form of a Volume.\
\
A DLT pipeline has been used for the ingestion, the volume path is referenced in the pipeline's streaming table **turbines_cleaned_raw_records** using autoloader, which would detect and load any additional files should they be added. For the existing files being updated, the dlt pipeline is set to do a full refresh on each run, so changes in the existing files will also be read.\
<img width="687" alt="image" src="https://github.com/user-attachments/assets/82b75bd5-dd3c-4c28-97d0-fa6391d6af49" />

\
The **turbines_cleaned_raw_records** streaming table uses expectations to drop records where the data columns are null, ensuring each row has a complete set of data. Ideally I would have rules in a file which could be called by a function to apply dynamically to any entity being loaded, but as it was only 3 columns for this exercise I have hard-coded the rules into the pipeline.

#### Transformation
In order to perform the various aggregations to create summary statistics and to identify outliers, the materialised view **turbines_cleaned_aggregations** also makes use of an expectations statement to drop records which have been flagged as an outlier.\
Technically another materialsed view could have been created within the dlt pipeline to select only the summary statistics, but given that I wanted to write out the results of the ingestion and the summary statistics to persistent delta tables, I left this step out of the dlt pipeline and instead created a simple notebook to read from the streaming table and view and save as delta tables in the Silver schema. The DLT pipeline and the notebook have therefore been orchestrated into a simple workflow.
<img width="685" alt="image" src="https://github.com/user-attachments/assets/b4171616-06e4-4bfb-87c7-5dce2cc34e08" />


#### Catalog
I have used Unity Catalog with a Bronze and Silver schema to roughly organise the source files, views and tables:
- The source files are in an ADLS Gen 2 account and the container has been added as a Volume (external location) named **turbines_raw**
- The streaming table and materialised view from the dlt pipeline are in the Bronze schema too
- The two delta tables have been written to Silver
<img width="263" alt="image" src="https://github.com/user-attachments/assets/b4cf6010-77f0-4896-b5ad-d09f790f8e24" />

#### Deployment
The temptation was great to try and develop this as a fully-fledged asset bundle which could be deployed and tested without any set up. Without the time to go down a CI/CD rabbit hole under my current time constraint, I have had to make the assumption that a Databricks testing envrionment with the basic configuration I have provided will be available to whoever wishes to test my code, or that the code itself should be self-explanatory.

