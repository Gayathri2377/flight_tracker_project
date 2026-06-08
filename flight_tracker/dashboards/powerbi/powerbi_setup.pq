# Power BI Setup Guide for Flight Tracker
# =========================================
# This file contains all DAX measures and M queries to connect Power BI to DuckDB.
#
# STEP 1: Install DuckDB ODBC Driver
#   → Download from: https://duckdb.org/docs/api/odbc/overview
#   → Install the 64-bit driver
#
# STEP 2: Create ODBC DSN
#   → Open "ODBC Data Sources (64-bit)"
#   → Add → DuckDB Driver
#   → DSN Name: FlightTracker
#   → Database: C:\path\to\flight_tracker\data\warehouse\flights.duckdb
#
# STEP 3: In Power BI Desktop
#   → Get Data → ODBC → FlightTracker DSN
#   → Import tables: flight_current, flight_kpis, region_summary,
#                    top_operators, hourly_flight_trend, aircraft_dimension
#
# =========================================
# M QUERY — flight_current
# =========================================

let
    Source = Odbc.DataSource("dsn=FlightTracker", [HierarchicalNavigation=true]),
    Schema = Source{[Name="main"]}[Data],
    Table  = Schema{[Name="flight_current"]}[Data]
in
    Table

# =========================================
# M QUERY — hourly_flight_trend
# =========================================

let
    Source = Odbc.DataSource("dsn=FlightTracker"),
    Query  = Value.NativeQuery(Source,
        "SELECT hour_bucket, region, flight_count, avg_altitude_ft,
                avg_speed_knots, emergency_count
         FROM hourly_flight_trend
         ORDER BY hour_bucket"),
    #"Changed Types" = Table.TransformColumnTypes(Query,{
        {"hour_bucket", type datetime},
        {"flight_count", Int64.Type},
        {"avg_altitude_ft", type number}
    })
in
    #"Changed Types"

# =========================================
# DAX MEASURES
# =========================================

# Page 1 — Executive Overview

Active Flights =
COUNTROWS(flight_current)

Avg Altitude ft =
CALCULATE(
    AVERAGEX(flight_current, flight_current[altitude_ft]),
    NOT(ISBLANK(flight_current[altitude_ft]))
)

Avg Speed Knots =
CALCULATE(
    AVERAGEX(flight_current, flight_current[gs_knots]),
    NOT(ISBLANK(flight_current[gs_knots]))
)

Emergency Count =
COUNTROWS(FILTER(flight_current, flight_current[is_emergency] = TRUE()))

Airborne Count =
COUNTROWS(FILTER(flight_current, flight_current[on_ground] = FALSE()))

On Ground Count =
COUNTROWS(FILTER(flight_current, flight_current[on_ground] = TRUE()))

Unique Aircraft Types =
DISTINCTCOUNT(flight_current[aircraft_type])

Unique Airlines =
DISTINCTCOUNT(flight_current[flight])    -- prefix-based

# Page 3 — Fleet Analytics

Aircraft Type Distribution =
SUMMARIZE(
    flight_current,
    flight_current[aircraft_type],
    "Count", COUNT(flight_current[hex])
)

Top Airlines by Flights =
TOPN(
    10,
    SUMMARIZE(
        flight_current,
        flight_current[flight],
        "Flights", COUNT(flight_current[hex])
    ),
    [Flights], DESC
)

# Page 4 — Safety Dashboard

Emergency Rate % =
DIVIDE([Emergency Count], [Active Flights], 0) * 100

# Page 5 — Historical Trends

Hourly Flight Count =
SUM(hourly_flight_trend[flight_count])

Flight Growth % (DoD) =
VAR Today    = MAX(hourly_flight_trend[flight_count])
VAR Yesterday = CALCULATE(
    MAX(hourly_flight_trend[flight_count]),
    DATEADD(hourly_flight_trend[hour_bucket], -1, DAY)
)
RETURN DIVIDE(Today - Yesterday, Yesterday, 0) * 100

# =========================================
# POWER BI PAGE LAYOUT
# =========================================

# PAGE 1 — Executive Overview
#   Row 1: Cards → Active Flights | Avg Altitude | Avg Speed | Emergencies
#   Row 2: Cards → Airborne | On Ground | Aircraft Types | Airlines
#   Row 3: Donut → Flights by Region  |  Bar → Top Operators

# PAGE 2 — Live Map
#   Full page ArcGIS / Azure Maps visual
#   Lat field:  flight_current[lat]
#   Lon field:  flight_current[lon]
#   Size:       altitude_ft
#   Color:      is_emergency (red=true, blue=false)
#   Tooltip:    hex, flight, aircraft_type, gs_knots, altitude_ft

# PAGE 3 — Fleet Analytics
#   Donut:  Aircraft Type Distribution (flight_current[aircraft_type])
#   Bar:    Top Airlines (flight from flight_current, count)
#   Table:  aircraft_dimension (hex, registration, aircraft_type, total_observations)

# PAGE 4 — Safety Dashboard
#   Card:   Emergency Count
#   Card:   Emergency Rate %
#   Table:  flight_current filtered is_emergency=TRUE
#   Bar:    Squawk code distribution (squawk, count)

# PAGE 5 — Historical Trends
#   Line:  hourly_flight_trend → hour_bucket vs flight_count (by region)
#   Line:  hourly_flight_trend → avg_altitude_ft over time
#   Line:  hourly_flight_trend → avg_speed_knots over time
#   Bar:   emergency_count by hour
