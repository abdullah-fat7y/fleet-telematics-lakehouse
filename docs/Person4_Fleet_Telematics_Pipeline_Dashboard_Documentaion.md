# Fleet Analytics Dashboard

**SIC 8 / 2026 · Big Data Engineering Track · Project 03: Supply Chain & Intelligent Logistics**
Enterprise Fleet Telematics Lakehouse & Dynamic Logistics Warehouse
Microsoft Fabric · Delta Lake · Power BI

This document explains the Power BI Fleet Analytics Dashboard in plain English. It covers the business problem, each of the six pages, and how raw vehicle readings become the numbers on screen.

> The data is **simulated**. The dashboard reports what the implemented detection rules found. It does not prove that a real incident happened.

---

## Contents

1. [Business Context and Objectives](#1-business-context-and-objectives)
2. [Dashboard Documentation](#2-dashboard-documentation)
   - [2.1 Fleet Overview](#21-fleet-overview)
   - [2.2 Routes and Cold Chain](#22-routes-and-cold-chain)
   - [2.3 Driver Scorecard](#23-driver-scorecard)
   - [2.4 Anomaly Investigation](#24-anomaly-investigation)
   - [2.5 Pipeline Health](#25-pipeline-health)
   - [2.6 Definitions & Methodology](#26-definitions--methodology)
3. [Data Sources and Model](#3-data-sources-and-model)
4. [Thresholds, Assumptions and Limitations](#4-thresholds-assumptions-and-limitations)
5. [Glossary](#5-glossary)

---

## 1. Business Context and Objectives

### The business challenge

A fleet operator runs thousands of delivery vehicles on fixed routes between cities. Some trucks carry ordinary goods (dry vans). Others are refrigerated trucks that must keep cargo cold. Managers need to know:

- Are vehicles using fuel sensibly?
- Is cold cargo staying cold?
- Which drivers idle too long or drive too fast?
- Where do unusual events happen?

The project handbook notes that vehicle diagnostics, position and cargo conditions arrive as separate streams, which makes route deviations, refrigeration breaches and excessive idling hard to investigate. This project joins them in a lakehouse and warehouse. The dashboard is where managers see the result.

### The data behind the dashboard

| Item | Detail |
|---|---|
| Fleet | 5,000 vehicles (about 1,500 refrigerated), 5,200 drivers, 10 depots, 30 fixed routes (corridors) |
| Readings | One per vehicle every 30 seconds: GPS, speed, RPM, engine temperature, fuel rate, odometer and, for refrigerated trucks, cargo temperature |
| Time window | 2 Nov 2026 06:00 UTC to 4 Nov 2026 15:05 UTC (all times UTC) |
| Planned problems | Cold-chain breaches, excessive idling, route deviations (handbook), plus engine overheating added by the team |
| Volume | 8,146,714 clean readings in Silver; 113,475 rejected readings kept for audit |

### The problem the dashboard solves

Raw telemetry is millions of rows. The dashboard turns the curated Gold tables into a few trusted answers, and shows whether those answers can be trusted. It serves two audiences:

- **Fleet and operations managers:** performance, safety and cold-chain answers.
- **Data engineers and reviewers:** proof that the pipeline ran correctly and Gold agrees with Silver.

### What the dashboard monitors

| Area | Question it answers | Page | Headline |
|---|---|---|---|
| Fleet performance | How far do vehicles travel per litre, and which routes stand out? | 1, 2 | 2.77 km/L |
| Driver behaviour | Which drivers idle long, speed or leave their route? | 3 | 8.63% speeding |
| Cold-chain compliance | What share of refrigerated readings stayed in 2–8 °C? | 1, 2 | 99.86% |
| Anomalies | What was detected, how long did it last and when? | 1, 4 | 9,724 episodes |
| Pipeline reliability | Did quality checks pass, and do Gold and Silver agree? | 5 | 28 PASS, 1 WARN |

---

## 2. Dashboard Documentation

<img width="1291" height="777" alt="Screenshot 2026-10-28 213920" src="https://github.com/user-attachments/assets/59782221-201f-4da9-a5dd-7ffb377f2867" />


| Page | Name | Purpose |
|---|---|---|
| 1 | Fleet Overview | Headline KPIs, anomaly mix, route comparison |
| 2 | Routes and Cold Chain | Fuel and cold-chain performance by route |
| 3 | Driver Scorecard | Idling, speeding and route deviations per driver |
| 4 | Anomaly Investigation | Where, when and how long anomalies lasted |
| 5 | Pipeline Health | Quality checks, reconciliation, job timings |
| 6 | Definitions & Methodology | Rules, thresholds and limitations |

**Interacting with any page:** Click a bar, row, donut slice or slicer button to filter or highlight the other visuals on that page. Click it again to clear. Hold Ctrl to select several. This is standard Power BI behaviour. The screenshots do not show whether any interactions were disabled.

---

### 2.1 Fleet Overview

**Purpose:** Give a manager the whole picture in about ten seconds: five headline numbers, the anomaly mix and a route comparison.

<img width="1297" height="777" alt="Screenshot 2026-10-28 122827" src="https://github.com/user-attachments/assets/063887c4-52f5-45a1-8f9e-c285291d44d0" />
<!-- Add the Fleet Overview screenshot above -->

**Visuals and data objects**

| Object | Type | What it shows | Source |
|---|---|---|---|
| Fuel Efficiency km/L (2.77) | Card | Fleet-wide kilometres per litre | `gold_kpi_fuel_efficiency_route` |
| Cold Chain Compliance % (99.86%) | Card | Share of refrigerated readings inside 2–8 °C | `gold_kpi_cold_chain` |
| Cold Chain Breach Episodes (163) | Card | Detected breach episodes | `gold_kpi_cold_chain` |
| Anomaly Episodes (10K) | Card | All detected episodes (9,724) | `gold_fact_anomaly_episode` |
| Vehicles Reporting (5K, target 5,000) | Card | Distinct vehicles seen vs the 5,000 required | Gold vehicle/trip data (exact table not visible) |
| Anomaly Episodes by Type | Column chart | Route deviation 5.9K, engine overheating 2.6K, excessive idling 1.0K, cold-chain breach 0.2K | `gold_fact_anomaly_episode` + `gold_dim_anomaly_type` |
| Route Performance Summary | Table with data bars | Per route: fuel efficiency vs fleet, compliance, episodes per 1,000 km (fleet total 1.82) | KPI tables, `gold_fact_anomaly_episode`, `gold_dim_route` |

**Metrics explained**

| Metric | Meaning |
|---|---|
| Fuel efficiency (km/L) | Total distance divided by total fuel. Higher is better. Always calculated from sums, never by averaging. |
| Cold-chain compliance % | Of all refrigerated-truck temperature readings, the share between 2 °C and 8 °C inclusive. Dry vans are excluded. |
| Cold-chain breach episode | 3 or more consecutive readings outside 2–8 °C, counted once. |
| Anomaly episode | One continuous unusual event for one vehicle. Counting episodes avoids counting the same event many times. |
| Episodes per 1,000 km | Episodes divided by distance, scaled to 1,000 km, so busy and quiet routes compare fairly. |
| Fuel Efficiency vs Fleet | A route's km/L minus the fleet km/L. Negative means worse than the fleet. |

**Filters:** No slicers. Click an anomaly bar to filter the route table, or click a route row to highlight it.

> **Check**
> - "Fuel Efficiency vs Fleet km/L" shows 0.00 for every route. The differences are about ±0.002 km/L and the column has two decimals.
> - Route deviation is about 61% of episodes. The Gold README says these look driven by GPS jumps.

---

### 2.2 Routes and Cold Chain

**Purpose:** Show which routes have the most cold-chain trouble and how each route's fuel efficiency compares with the fleet.

<img width="1422" height="853" alt="Screenshot 2026-10-28 154906" src="https://github.com/user-attachments/assets/e9faa9ad-2f8d-4047-8d29-2023f95a2b6e" />
<!-- Add the Routes and Cold Chain screenshot above -->

| Object | Type | What it shows | Source |
|---|---|---|---|
| Cold-Chain Breach Episodes by Route | Bar chart | Minya – Port Said leads with 11; four routes have 10; Hurghada – Minya 9 | `gold_kpi_cold_chain` |
| Cold-Chain Compliance Trend | Area chart | Compliance % by day, 2–4 Nov. The axis starts at 99.0%. | `gold_kpi_cold_chain` + `gold_dim_date` |
| Fuel Efficiency by Route (km/L) | Diverging bar chart | Each route's difference from the fleet average. Cairo – Mansoura highest, Cairo – Suez lowest. | `gold_kpi_fuel_efficiency_route` |
| Route Performance Details | Table | Total km, Total Litres, Cold Readings Total, Compliance %, Breach Episodes. Totals: 5,345,177.79 km, 1,931,814.52 L, 2,415,864 readings, 99.86%, 163 | KPI tables + `gold_dim_route` |

**New metrics**

| Metric | Meaning |
|---|---|
| Total km / Total Litres | Numerator and denominator of fuel efficiency. Gaps over 5 minutes and GPS jumps above 200 km/h add no distance or fuel. |
| Cold Readings Total | Number of refrigerated readings counted. It shows how much evidence stands behind each percentage. |

**Filters:** No slicers. Click a route in any visual to highlight it in the others.

> **Check**
> - The chart titled "Fuel Efficiency by Route (km/L)" plots the *difference from the fleet average*, not km/L.
> - Differences are tiny, which is typical of simulated data.
> - The last trend point covers a partial day.

---

### 2.3 Driver Scorecard

**Purpose:** Show which drivers idle most, speed most often or leave their corridor, so supervisors can focus coaching.

<img width="1417" height="851" alt="Screenshot 2026-10-28 154929" src="https://github.com/user-attachments/assets/5af31d1c-4fee-4c86-8230-5adb389077f2" />
<!-- Add the Driver Scorecard screenshot above -->

| Object | Type | What it shows | Source |
|---|---|---|---|
| Idling Minutes (20.58K) | Card | Total idling minutes | `gold_kpi_driver_scorecard` |
| Excessive Idling Episodes (983) | Card | Idling stretches of 10+ minutes | `gold_kpi_driver_scorecard` |
| Speeding Rate % (8.63%) | Card | Share of readings above 90 km/h | Scorecard `speeding_readings` (denominator not visible) |
| Route Deviations (6K) | Card | Detected deviations (5,932) | `gold_kpi_driver_scorecard` |
| Driver Performance Details (Top 20) | Table with data bars | Name, depot, risk level, idling minutes, idle share, excessive idling, speeding rate, deviations. The Total row sums only the 20 drivers shown. | scorecard, `gold_dim_driver`, `gold_fact_trip` |
| Home Depot | Slicer (10 buttons) | Filters to one depot | `gold_dim_driver` |
| Idle Share by Risk Level | Donut chart | Idle share split by low, medium, high risk | `gold_dim_driver` + trip data |

**Metrics explained**

| Metric | Meaning |
|---|---|
| Idling | Speed below 2 km/h and engine RPM above 0. Planned stops with the engine off do not count. |
| Excessive idling episode | Idling that lasts 10 minutes or more. |
| Speeding rate | Readings with speed above 90 km/h as a share of readings. |
| Route deviation | More than 2 km from the corridor for 3 or more readings. |
| Idle Share of Trip Time | Idle minutes as a share of trip duration (assumed from the label). |
| Risk level | A driver attribute (low, medium, high) from reference data, not calculated by the dashboard. |

**Filters:** Click a Home Depot button to filter the cards, table and donut. Click a risk level in the donut to filter the table.

> **Check**
> - Driver names repeat (Sara Fouad appears twice). Add `driver_id` before using the list for decisions.
> - A driver whose depot or risk level changed has two rows (SCD Type 2). Group by `driver_id` for one total per person.
> - The table title says "by Idling Minutes" but the sort arrow is on Idle Share.
> - Speeding rates are similar for all drivers (about 7.5–9.9%), which suggests simulated variation.

---

### 2.4 Anomaly Investigation

**Purpose:** Move from "how many" to "where, when and how long" and drill down to individual episodes.

<img width="1422" height="852" alt="Screenshot 2026-10-28 154949" src="https://github.com/user-attachments/assets/0939d411-0d35-4579-a64a-6f08ec6fcbf2" />
<!-- Add the Anomaly Investigation screenshot above -->

| Object | Type | What it shows | Source |
|---|---|---|---|
| Episodes per 1,000 km by Route and Anomaly Type | Stacked bar chart | Anomaly intensity per route, split by type. Colours match page 1. No legend on this chart. | episode fact, `gold_dim_route`, `gold_dim_anomaly_type` |
| Episodes by Time of Day | Donut chart | Morning 59.49%, afternoon 38.27%, evening 2.24% | episode fact + `gold_dim_time` |
| Average Episode Duration Minutes by Type | Bar chart | Excessive idling about 20 min; cold-chain breach and route deviation about 1 min; engine overheating under 1 min | episode fact (`duration_minutes`) |
| Detected Episode Details (Top 20) | Table with data bars | Episode ID, vehicle, route, anomaly, month, day, duration, reading count. Longest: 66 min (VEH-03073, excessive idling) | episode fact, vehicle, route, date |
| Anomaly | Slicer | Cold-chain breach, Engine overheating, Excessive idling, No anomaly, Route deviation | `gold_dim_anomaly_type` |
| Vehicle Type | Slicer | `dry_van` or `refrigerated_truck` | `gold_dim_vehicle` |

**Metrics explained**

| Metric | Meaning |
|---|---|
| Day part | Night (before 06:00), morning (06–12), afternoon (12–18), evening (18:00 on). Data starts at 06:00, so there is no night slice. |
| Duration minutes | Time from the first to the last reading of an episode. |
| Reading count | Number of 30-second readings inside the episode. |
| Episode ID | Text key: `vehicle | anomaly type | sequence`, e.g. `VEH-03073|excessive_idling|1`. |

**Filters:** Pick one or more anomaly types, and optionally a vehicle type. For example, choose "Cold-chain breach" and `refrigerated_truck` to review only cold-chain events.

> **Check**
> - "No anomaly" is the placeholder member (key 0). Selecting it empties the page.
> - The Time of Day chart needs a link between the episode table and the time table. That link is not visible in the supplied model view.
> - Low detection quality in the README (precision 0.167, recall 0.333, F1 0.222) means counts show what was *detected*, not what was injected.

---

### 2.5 Pipeline Health

**Purpose:** Prove the numbers are trustworthy: checks ran, Gold agrees with Silver, driver history loaded, and jobs finished in reasonable time.

<img width="1422" height="856" alt="Screenshot 2026-10-28 155002" src="https://github.com/user-attachments/assets/c1e190d8-568f-4cf0-b926-f3cea4ecbaa2" />
<!-- Add the Pipeline Health screenshot above -->

| Object | Type | What it shows | Source |
|---|---|---|---|
| Checks PASS (28) | Card | Checks that passed in the latest run | `gold_quality_results` |
| Checks WARN (1) | Card | Checks with a warning | `gold_quality_results` |
| Checks FAIL | Card | Shows "--" (blank) because the count is zero | `gold_quality_results` |
| Recon Match % (100.00%) | Card | Share of reconciled metrics marked MATCH | `gold_reconciliation_results` |
| Drivers With SCD2 History (416, expected 416) | Card | Drivers with more than one version | `gold_dim_driver` |
| Data Quality Results | Matrix | Checks by Gold table; each cell is failing rows. Green = pass, orange = warning. | `gold_quality_results` |
| Execution Duration by Job | Bar chart | Seconds per job: NB02 TOTAL 263, fact_telemetry 114, NB04 reconciliation 103, NB03 quality_checks 83, NB04 build_kpis 31, fact_trip 24, fact_anomaly_episode 16 | `gold_job_run_times` |
| Silver-to-Gold Reconciliation Details | Table | Per KPI metric: groups compared, mismatched, Gold vs Silver totals, max difference, status | `gold_reconciliation_results` |
| Daily Data Volume | Column chart | Day 2 about 10.0K, day 3 about 4.1K, day 4 about 9.9K | Measure not visible |

**Terms explained**

| Term | Meaning |
|---|---|
| Quality check | An automated test on a Gold table. PASS = fine. WARN = unusual but not blocking. FAIL = blocking, the pipeline stops. |
| Reconciliation | KPIs are recalculated from Silver and compared with Gold (tolerance 1e-6). This page shows 0 mismatches across 52,917 groups. |
| SCD Type 2 history | Old versions of a driver (depot, risk level, licence class) are kept. 416 drivers changed. |

**Filters:** No slicers. Clicking a check or column highlights related results.

> **Check before presenting**
> - **Two uncoloured non-zero cells:** the telemetry column shows 2,797 (`range_gold_measures`) and 93,408 (`row_count_matches_silver`) with no status colour, while the cards say 28 PASS, 1 WARN, no FAIL. The matrix probably includes more than one run (for example the deliberate demo failure run). Filter it to the latest real run.
> - **The one warning** is `range_sensor_values` on telemetry (32,638 rows). This is by design: out-of-range sensor values stay visible instead of being deleted.
> - **Daily Data Volume** is far below the millions of readings per day. Confirm what it counts and rename it. Day 3 is clearly lower.

---

### 2.6 Definitions & Methodology

**Purpose:** A text-only page that states the rules behind every number. It has no data visuals.

<img width="1425" height="848" alt="Screenshot 2026-10-28 155015" src="https://github.com/user-attachments/assets/bb791158-92b4-4cce-868b-03d7be0384da" />
<!-- Add the Definitions & Methodology screenshot above -->

| Section | What it says |
|---|---|
| 1. Fuel Efficiency | Total distance ÷ total fuel, in km/L. Intervals over 300 s and GPS jumps are excluded. |
| 2. Cold-Chain Compliance | Refrigerated readings in 2–8 °C (inclusive) ÷ all refrigerated readings × 100. Measures adherence to the simulation threshold, not regulatory compliance. |
| 3. Speeding and Idling | Speeding: speed > 90 km/h. Idling: speed < 2 km/h and RPM > 0. |
| 4. Anomalies and Episode Counts | Counts are detected events, not verified ground truth. |
| 5. Data Window and Limitations | Use the real start and end timestamps. "Data Through" shows the latest trip end. The latest day may be incomplete. |

---

## 3. Data Sources and Model

### Where the data comes from

The data moves through a Microsoft Fabric lakehouse using the medallion pattern. Bronze keeps raw data, Silver cleans it and flags anomalies, and Gold organises it for analysis. Power BI reads only Gold.

| Step | Stage | What happens |
|---|---|---|
| 01 | Generator | A Python simulator creates readings for 5,000 vehicles every 30 s and injects anomalies and bad data |
| 02 | API and database | An ingestion API stores readings in PostgreSQL |
| 03 | Streaming and batch | Live changes flow through CDC and Kafka into a Fabric Eventstream. An hourly extractor lands history files in OneLake. Silver records the path (`kafka_cdc` or `db_extract`). |
| 04 | Bronze | Raw append-only tables, dead-letter queue, ingestion logs |
| 05 | Silver | Clean, de-duplicated readings, anomaly flags, 9,724 episodes, 113,475 rejected rows |
| 06 | Gold | Star schema, three KPI tables, quality / reconciliation / job-timing tables (notebooks NB01–NB04) |
| 07 | Power BI model | Gold tables imported with relationships and DAX measures |
| 08 | Dashboard | The six report pages |

### Power BI model

<img width="1603" height="810" alt="PBIDesktop_vGSwhmSa3d" src="https://github.com/user-attachments/assets/b67f1a33-77f8-404b-94aa-9b75bcb5c47b" />
<!-- Add the model view screenshot above -->

| Table | One row is… | Used for |
|---|---|---|
| `gold_dim_date` / `gold_dim_time` | one day / one minute | Date filters, trend axis, day part |
| `gold_dim_vehicle` | one vehicle | Vehicle type slicer |
| `gold_dim_driver` | one driver version (SCD2) | Driver name, depot, risk level |
| `gold_dim_route` | one route | Route names |
| `gold_dim_anomaly_type` | one anomaly type plus `none` | Anomaly names and slicer |
| `gold_fact_trip` | one trip | Trip duration, idle minutes |
| `gold_fact_anomaly_episode` | one detected episode | Episode counts, duration, time of day |
| `gold_kpi_fuel_efficiency_route` | route × day | Total km, total litres |
| `gold_kpi_cold_chain` | route × day | Readings in range and total, breach episodes |
| `gold_kpi_driver_scorecard` | driver version × day | Idling, speeding, deviations |
| `gold_quality_results` | one check per run | Quality cards and matrix |
| `gold_reconciliation_results` | one KPI metric per run | Reconciliation table |
| `gold_job_run_times` | one job run | Execution duration chart |

`gold_fact_telemetry` (one row per 30-second reading) exists in Gold but is not in the supplied model view. Reading-level information reaches the dashboard through the KPI tables.

### How raw readings become dashboard numbers

Example: the Fuel Efficiency card (2.77 km/L).

| Step | Action | Example |
|---|---|---|
| 1 | Silver reading | Time gap, GPS distance and fuel rate per reading |
| 2 | Gold measure | `distance_km` kept only for valid intervals (0–300 s) without GPS jumps. `fuel_used_l = fuel_rate × gap ÷ 3600` |
| 3 | Daily KPI table | `total_km` and `total_litres` per route and day |
| 4 | Power BI measure | Total km ÷ Total litres, on the current filter |
| 5 | Visual | 5,345,177.79 km ÷ 1,931,814.52 L = 2.77 km/L |

**Why ratios are divided late:** Ratios such as km/L and compliance % cannot be added or averaged. Gold stores the numerator and denominator, and Power BI divides after filtering, so totals stay correct for any selection.

### Dashboard measures

The exact DAX formulas were not visible in the screenshots. Descriptions below come from the labels and the README definitions.

| Measure | Meaning |
|---|---|
| Fuel Efficiency km/L | Total km ÷ total litres |
| Fuel Efficiency vs Fleet km/L | Route km/L minus fleet km/L |
| Cold Chain Compliance % | Readings in range ÷ readings total |
| Cold Chain Breach Episodes | Sum of `breach_episodes` |
| Anomaly Episodes | Count of episode rows |
| Episodes per 1,000 km | Episodes ÷ total km × 1,000 |
| Vehicles Reporting | Distinct vehicles with data |
| Idling Minutes, Excessive Idling Episodes, Route Deviations | Sums of scorecard columns |
| Speeding Rate % | Speeding readings as a share of readings |
| Idle Share of Trip Time | Idle minutes ÷ trip duration (assumed) |
| Checks PASS / WARN / FAIL | Checks per status in the latest real run |
| Recon Match % | MATCH rows ÷ all reconciliation rows |

---

## 4. Thresholds, Assumptions and Limitations

### Thresholds

| Rule | Value | Applies to |
|---|---|---|
| Valid reading gap | 0 to 300 seconds | Distance, fuel and idle minutes |
| Plausible speed | 200 km/h or less | Faster calculated speed is treated as a GPS jump (distance = 0) |
| Speeding | Speed above 90 km/h | Speeding readings and rate |
| Idling | Speed below 2 km/h and RPM above 0 | Idling minutes |
| Excessive idling | 10 minutes or more | Excessive idling episodes |
| Cold-chain range | 2 to 8 °C inclusive | Compliance (refrigerated only) |
| Cold-chain breach | 3+ consecutive readings outside range | Breach episodes |
| Route deviation | More than 2 km from corridor for 3+ readings | Route deviation episodes |
| Day part | Night < 06, morning < 12, afternoon < 18, then evening | Time-of-day chart |
| Reconciliation tolerance | Relative 1e-6 | Gold vs Silver comparison |

### Assumptions

- The data is simulated. Results describe the simulation, not a real fleet.
- All timestamps are UTC.
- Dry vans have no cargo temperature and are excluded from cold-chain figures. A route with no refrigerated readings has an empty compliance value, not 0% or 100%.
- Only Gold is shown. The 113,475 rejected rows stay in Silver for audit.
- Measures not visible in the screenshots (Idle Share of Trip Time, Speeding Rate % denominator, Vehicles Reporting, Daily Data Volume) are described from the README and labels. Confirm them against the report file.

### Limitations

| Limitation | Why it matters |
|---|---|
| Low detection quality | README reports precision 0.167, recall 0.333, F1 0.222 on a small truth set (6 truth episodes vs 9,724 detected). Anomaly counts are indicative only. |
| Route deviation may be GPS-driven | Sample episodes show impossible speeds (about 1.2 million km/h over two readings). This is the largest anomaly type. |
| Cold-chain compliance is not certification | It measures the simulation's 2–8 °C rule only. |
| Small route differences | Fuel efficiency differs by about ±0.002 km/L between routes. |
| Partial days | Data ends 4 Nov 15:05, and day 3 volume is much lower. Compare complete days only. |
| Driver history date | The change date is supplied at load time as 2026-11-03 12:00. Other project documentation may mention a different time. Confirm before explaining before/after driver changes. |
| Name ambiguity | Driver names repeat. Use `driver_id`. |

### Suggested improvements

| # | Improvement | Page |
|---|---|---|
| 1 | Filter the Data Quality matrix to the latest real run | 5 |
| 2 | Show more decimals for "Fuel Efficiency vs Fleet" and retitle the chart as a difference | 1, 2 |
| 3 | Add `driver_id` to the driver table and align its title with its sort | 3 |
| 4 | Confirm and rename Daily Data Volume. Add a Data Through card. | 5 |
| 5 | Add a legend to the stacked anomaly chart and hide the "No anomaly" option | 4 |
| 6 | Label the zoomed axis on the compliance trend | 2 |

---

## 5. Glossary

| Term | Meaning |
|---|---|
| Anomaly episode | One continuous unusual event for a vehicle, counted once |
| Bronze / Silver / Gold | Raw, cleaned and analysis-ready data layers |
| Cold chain | Keeping refrigerated cargo within a safe temperature range |
| Corridor / route | A fixed path between two depots, shown as "City – City" |
| Dead-letter queue | A holding area for events that could not be processed |
| Delta table | A table format with reliable updates and history |
| Depot | A base where vehicles and drivers are stationed (10 in total) |
| Dimension / fact table | A dimension describes something (route, driver). A fact records events (trips, episodes). |
| Dry van | A truck without refrigeration |
| GPS jump | A position reading that jumps an impossible distance, excluded from distance |
| Idling | Engine on, vehicle almost stationary |
| KPI | A headline number such as fuel efficiency |
| Non-additive ratio | A figure like km/L that must be recalculated from numerator and denominator |
| Quality gate | Automatic checks that must pass before KPIs are built |
| Reconciliation | Recalculating a KPI from Silver to confirm Gold gives the same answer |
| Refrigerated truck | A truck with a temperature-controlled cargo area (reefer) |
| SCD Type 2 | Keeps history by adding a new row for each version of a record |
| Semantic model | The Power BI layer holding tables, relationships and measures |

---

**Sources:** Graduation Projects Handbook (Big Data Engineering Track, SIC 8 / 2026), Gold-layer README, Power BI model view and six dashboard screenshots dated 28 October 2026.

*Simulation / Pipelines / Lakehouse / Analytics*
