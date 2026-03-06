# MTA Capacity Stress Index

## Datasets

### MTA Subway Hourly Ridership: 2025 - present

https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-2025/5wq4-mkjj/about_data

Columns
- `transit_timestamp: DATE`: rounded to nearest hour, local time
- `transit_mode: TEXT`: subway, Staten Island Railway, Roosevelt Island tram
- `station_complex_id: ALPHANUMERIC`: unique identifier
- `station_complex: TEXT`: entry swipe or tap complex location. Multiple subway lines per large stations like Times Square
- `borough: TEXT`: NYC borough one of five
- `payment_method: TEXT`: OMNY or MetroCard
- `fare_class_category: TEXT` 
- `ridership: NUMERIC` - number of riders that entered subway complex
- `transfers: NUMERIC` 
- `latitude: DECIMAL`
- `longitude: DECIMAL`
- `Georeference`

### MTA Subway Customer Journey-Focused Metrics: 2025 - present

https://data.ny.gov/Transportation/MTA-Subway-Customer-Journey-Focused-Metrics-Beginn/s4u6-t435/about_data

Additional Platform Time (APT) is the estimated average extra time that customers spend waiting on the platform for a train, compared with their scheduled wait time.

Columns
- `month_date: DATE`: month of calculation
- `division: TEXT`: A division (numbered subway lines and S-line at 42nd street), B division (lettered subway lines)
- `line: TEXT`: subway line (1, 2, 3, 4, 5, 6, 7, A, C, E, B, D, F, M, G, JZ, L, N, Q, R, W, S 42nd, S Rock, S Fkln)
- `period: TEXT`: peak and off-peak service periods
- `num_passengers: NUMERIC`: total number of estimated passengers reported each month on each line
- `additional_platform_time: NUMERIC`: average estimated additional time in minutes customers wait for their trained, each month on each line
- `additional_train_time: NUMERIC`
- `total_apt`: total estimated additional time in minutes customers wait for their trained, each month on each line
- `total_att: NUMERIC`
- `over_five_mins: NUMERIC`
- `over_five_mins_perc: PERCENT`
- `customer_journey_time: PERCENT`

### MTA Subway Stations

https://data.ny.gov/Transportation/MTA-Subway-Stations/39hk-dx4f/about_data

Since APT dataset doesn't have station_complex, a Station-Route Table is also needed to help join the two together

## Transformations

Needs to group by `station_complex` and `transit_timestamp` to get entry swipes per hour and per station.

Process timestamp into `hour`, `day_of_week`, `is_weekend`, and `time_window` (`"early_morning","morning_peak","midday","evening_peak"`)

Use window functions to compute `monthly_ridership` and `monthly_rank`
- `monthly_ridership`: sum of all hourly ridership for station_complex `X` across all hours in month `M`
- `monthly_rank`: Ranking of all stations within each month.

## Capacity Stress Index (CSI)

`volume`: how many riders per hour per station

`temporal_concentration`: how dispersed or concentrated the ridership is in a given month for a single station
- let's say in a single month, a station experiences a relatively consistent number of ridership per hour (uniform), a low concentration, a well dispersed ridership over the whole month
- now let's say it's Thanksgiving and there's significantly more riders on Thanksgiving day relative to the rest of November
- the above can be captured by coefficient of variance (CV) = std dev / mean ridership for a month
- High CV for a specific month = Highly concentrated ridership in some narrower time window within the month

Volume alone doesn't capture if a station is facing **unusually high** volume. Big stations get a lot of riders all the time.

Temporal concentration (CV) alone doesn't indicate the magnitude of the "unusual volume"

Capacity Stree Index (CSI)
- normalized volume: peak hourly ridership for a station-month normalized relative to the 90th percentile ridership seen in year 2025
- Herfindal-Hirschman Index (HHI) for a station-month measuring dispersion of hourly ridership within that station-month

The CSI is really calculating which stations in a given month are experiencing unusually high volumes relative to other stations in the same month (normalized volume) AND unusually high volumes over certain hours in that month time frame
- Specifically a threshold of >0.65 CSI leads to a danger flag (BUT WAIT THERE'S ALSO APT condition)

### Volume

Volume is normalized according to peak ridership. 

For each station-month in year 2025
- Compute peak hourly ridership

Find 90th percentile of peak hourly ridership among all stations and months in the year 2025
- 90th percentile is less sensitive to outliers than the simple maximum peak ridership seen in year 2025

For each station-month in year 2025
- Divide peak hourly ridership by that 90th percentile
- `normalized_volume` >= 1.0 means the ridership in this station is greater than or equal to the 90th percentile peak ridership seen in 2025 **for that station in that month**

### HHI

[HHI](https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index)
- In the context of market competition, let's say the market share of companies A, B, and C are known and add up to 100%
- Square the shares and sum up = HHI
- If the shares are relatively even among the companies, the HHI will be low
- Higher HHI is a signal that one company may have more market share than the rest and is a sign that there is a monopoly

In this project:

For each station-month:
- Compute each hour's share of total ridership over the course of a month and sum across the all hours in that month
    - `share = hour_ridership / daily_total_ridership`
    - `HHI = share_hr0 ^2 + share_hr1 ^2 ... + share_hr720 ^2`
- Low HHI means ridership ridership is evenly distributed throughout the month for that station.
- High HHI means the station can face much higher ridership in certain hourse of that month

## APT

Additional platform time > 3 minutes is also a bad sign because it means customers are waiting longer than expected for their train.

## Danger Zone!

CSI > 0.65 AND APT > 3 minutes
- CSI indicates the station in that month can experience high volume and high temporal concentration of ridership in the span of a couple hours
- APT indicates that that high volume and concentration of riders are also waiting a very long time

Overall, a station that meets the above two conditions points where the service experience is likely very bad for a rider/customer waiting on a platform.

(of course how to fix that problem is something else entirely)

## PySpark

Basics
- Pandas executes code immediately ("eager evaluation") on a DataFrame that lives in RAM 
- PySpark does "lazy evaluation" - doesn't do anything until you call an **action** (`.write`,`.show()`)
- Instead your initial code is building up a **query plan**.
- Spark's query optimizer, Catalyst, then decides how to best execute that plan efficiently.
- The DataFrame in PySpark is better understood as a "description of a distributed computation" because the data could be spread across different partitions on different machines. 
    - You *can* put them on Python memory using `collect()` but this isn't advised for very large datasets.


```python
df = spark.read.parquet("/DIRECTORY/")
```
- Spark only reads the metadata from each parquet file footer

```python
df = df \
    .withColumn("hour_of_day", F.hour("transit_timestamp")) \
    .withColumn("day_of_week",  F.dayofweek("transit_timestamp")) \
    .withColumn("is_weekend", (F.dayofweek("transit_timestamp").isin(1,7).cast("int"))) 
```
dayofweek = 1 (Sunday), 7 (Saturday)

```python
F.lit(p90)
```

Wrap values with Spark literal column for certain column-wide operations to work properly

## Airflow

### Core Concepts

**Task** single unit of work that runs independently - so imports are inside functions. Tasks are distributed among a specified number of workers by Airflow to execute.

**Directed Acyclic Graph (DAG)** definition of tasks and in what order they depend on each other. "Acyclic" means there's no circular dependencies. If Task B waits for Task A, Task A cannot then also wait for Task B.
- Unlike standard Python scripting, something adding a function parameter that's never used can help create a dependency such that you guarantee that function with the artificial parameter does not run UNTIL AFTER a previous task is completed.

**TaskFlow API** Modern way to write Airflow using `@dag` and `@task` decorators to enable you to write with Python syntax. Airflow used to require using its own syntax.

**XCom** Cross-communication, Airflow's mechanism for passing values between tasks.

### Coding stuff

```python
@dag(
    dag_id="mta_monthly_pipeline",
    schedule="0 6 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mta", "ridership"],
)
```

`schedule="0 6 1 * *"` - Cron syntax (UNIX job scheduler) for "minute 0, hour 6, day 1, every month, any day of week" (6AM first of every month)

```
┌─ minute (0-59)
│ ┌─ hour (0-23)
│ │ ┌─ day of month (1-31)
│ │ │ ┌─ month (1-12)
│ │ │ │ ┌─ day of week (0-6, Sun=0)
│ │ │ │ │
0 6 1 * *
```

`catchup=False` - If `True` AirFlow will run the DAG for every month between start date and "today" automatically after this script is loaded. Setting to `False` means it only only run moving forward.

For example, let's say it's mid March 2026. if nothing is done after starting AirFlow, then the next time this pipeline DAG runs is on April 1st. Then it will only pull data automatically for March 2026. Even though start date is January 1, 2026, data for the months of January and February will never get pulled unless manual processes are triggered.

`[TASK_FUNCTION].override(task_id=...)(parameter=...)` to give each instance a unique ID. This is how one generic task function produces two distinct tasks in the DAG.

Other Python scripts for transforming/processing the raw data should be called as subprocesses