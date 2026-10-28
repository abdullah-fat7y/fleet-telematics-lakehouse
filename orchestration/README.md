# Pipeline Orchestration and Dashboard Refresh

## Overview

Microsoft Fabric orchestrates the Fleet Telematics Lakehouse processing workflow and automates the refresh of the Power BI semantic model. This ensures that processed analytical data can be loaded into the dashboard on a daily schedule.

## 1. Fabric Workspace Lineage

The workspace lineage shows how the main components are connected:

- **Eventstreams:** `fleet_eventstream` and `fleet_eventstream_v2`
- **Lakehouse:** `fleet_lakehouse`
- **SQL analytics endpoint:** Provides SQL access to the Lakehouse data.
- **Semantic model:** `Fleet_Telematics_Dashboard`
- **Power BI report:** `Fleet_Telematics_Dashboard`

### Lineage Diagram

<!-- Replace the path below with the actual screenshot path in your repository. -->

<img width="1736" height="325" alt="Screenshot 2026-10-28 235948" src="https://github.com/user-attachments/assets/e09b1ef3-6ba5-463a-8b56-7bbf3a163fbb" />


## 2. Pipeline Orchestration

The pipeline `pl_telematics_medallion_orchestration` coordinates the main processing activities in the required sequence, including:

1. Environment setup
2. Bronze-to-Silver transformation
3. Anomaly detection and evaluation
4. Gold dimension and fact table creation
5. Data-quality checks
6. KPI reconciliation

Notebook activities are connected through execution dependencies, while selected wait activities separate processing stages.

### Pipeline Schedule

<img width="589" height="527" alt="photo_2026-10-28_23-45-19" src="https://github.com/user-attachments/assets/165b90b1-efdc-45a5-9331-de1f25852c10" />


## 3. Successful Pipeline Execution

The pipeline was executed successfully in Microsoft Fabric. The run output showed a successful overall status and successful completion of the displayed activities.

<img width="1600" height="703" alt="photo_2026-10-28_23-45-17" src="https://github.com/user-attachments/assets/27e3325e-a4e5-410b-9a81-d846376e9bdb" />


## 4. Scheduled Power BI Refresh

The `Fleet_Telematics_Dashboard` semantic model uses **Import mode** and has a separate daily refresh schedule.

| Configuration | Value |
|---|---|
| Semantic model | `Fleet_Telematics_Dashboard` |
| Refresh frequency | Daily |
| Refresh time | 06:00 AM |
| Time zone | Cairo (UTC+02:00) |
| Pipeline schedule | Daily at 05:00 AM Cairo time |

The refresh is scheduled one hour after the pipeline's scheduled start time to allow the data-processing workflow to complete before the semantic model imports the updated data.

### Refresh Schedule

<img width="1917" height="916" alt="Screenshot 2026-10-29 002607" src="https://github.com/user-attachments/assets/b42f3f2e-e741-429f-8713-f86996f711df" />


## 5. Operational Note

The pipeline and semantic model refresh use separate schedules. The refresh does not automatically wait for the pipeline to finish. Pipeline run history and semantic model refresh history should therefore be checked to verify successful execution and confirm that the latest processed data is available in the dashboard.
