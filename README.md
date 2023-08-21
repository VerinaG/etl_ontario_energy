# Ontario Energy ETL Pipeline

Extract, Transform, and Load Pipeline that ingests monthly Ontario Energy data (CSV and XML files) to populate the Ontario Energy Production Dashboard. The dashboard aims to illustrate Ontario's energy production by fuel type, to show the distribution and efficiency of our province's generators. It also showcases our grid's current demand and system losses, and the potential supply of energy Ontario can provide given our current generator trends. 

Monthly energy production, import/export data and grid system losses and dispatched load data is extracted from https://www.ieso.ca/ and stored in respective S3 buckets. The three sets of data are then downloaded locally and transformed into clean, useable data for the dashboard. Following the transformation, the data is staged and uploaded to Snowflake for storage, and displayed using Looker. 

## ETL Pipeline Diagram
![ETL Diagram](https://github.com/VerinaG/etl_ontario_energy/assets/30220521/404a6557-3837-4936-a3bc-5f75da04f697)

## Apache Airflow DAG Graph
<img width="932" alt="image" src="https://github.com/VerinaG/etl_ontario_energy/assets/30220521/6fdee008-cc48-4c41-bd17-f9bf14215f55">

|Tasks                                |Description                          |
|-------------------------------------|-------------------------------|
|`inertie_data_pull`                  |Download Intertie Schedule and Flow Report and upload to S3. This report provides the total import and export scheduled transactions and flows of energy between the IMO Controlled grid and each intertie zone. |
|`load_data_pull`                     |Download the Day-Ahead Constrained Total Report and upload to S3. This report provides a forecast of expected load, available energy, operating reserve requirements, and losses for each hour of the forecast period, as established by the DACE run. |
|`output_data_pull`                   |Download Generator Output and Capability Report and upload to S3. This report provides close-to-real-time output levels for Ontario’s generators registered as a market participant, and their availability to produce. It is published hourly, as soon as the data is available, containing data from generators with capacities 20 MW or greater, registered with the IESO. |
|`download_inertie_data_from_s3`<br/>`download_load_data_from_s3`<br/>`download_output_data_from_s3`|Download all reports from S3 to local docker directory for transformation. |
|`inertie_data_circuit_breaker`<br/>`load_data_circuit_breaker`<br/>`output_data_circuit_breaker`|Circuit breakers to detect bad data and stop the pipeline for further inspection.|
|`transform_intertie_load_data`| Transform intertie and load data to calculate import/export, dispatached load and system loss data.|
|`tranform_monthly_output_data`| Transform monthly output data to calculate energy production per fuel type/generator, and metrics such as fuel type/generator capacity versus output. |
|`stage_inertie_load_to_snowflake`<br/>`stage_output_to_snowflake`| Stage transformed data files into internal table stages to be copied into Snowflake tables. |
|`copy_intertie_load_to_table_snowflake`<br/>`copy_output_to_table_snowflake` | Copy data from staged files into Snowflake tables. |
|`clean_stage_snowflake`| Clear all internal table stages in Snowflake after successful copy. |
|`clean_local_directory`| Clear all downloaded data files from docker local directory. |



## Ontario Energy Production Dashboard - Looker
<img width="668" alt="image" src="https://github.com/VerinaG/etl_ontario_energy/assets/30220521/cd506efc-db3b-4f2e-ba6f-8a89bcb15a33">
<img width="669" alt="image" src="https://github.com/VerinaG/etl_ontario_energy/assets/30220521/1f68d68f-4db9-406a-97a4-b5a3d783e4f5">


**Dashboard:** https://lookerstudio.google.com/reporting/6dc32785-13a1-4269-a1b7-4723df069b7c




