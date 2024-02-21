# Formula 1 data

Some analyses related to Formula One.

# Download data

Notebook `downloader.ipynb` contains code to download data using the fastf1 package and save it locally for further analysis.

To run the notebook, you can change some parameters in the first cell and choose which races you want to download.

## Available data:

After downloading, you will find several files named `<year><round_number><session_type>_<name>.parquet`, where 

* `year` is a 4-digit year number,
* `round_number` is the 2-digit round number (in 2024, round 0 is the Pre-Season Testing, round 1 is Bahrein, round 2 is Australia and so on),
* `session_type` has the values `FP1`, `FP2`, `FP3` for free practice sessions, `Q` for qualifying (`SQ` for sprint qualifying), `S` for sprint and `R`for the main race.
* `name` refers to the type of data.

The available types of data are

* `drivers`: Drivers that participated in the session
* `results`: Official results from session
* `laps`: All laps driven in session
* `race_control`: Race control decisions during session (i.e., yellow flags, blue flags)
* `status`: Changes in session status (i.e., active, red flagged)
* `weather`: weather information during the session
* `telemetry`: detailed telemetry for each car (speed, RPM, gear, throttle, brake, DRS)
* `positions`: position tracking (race-line) for each car


## Useful links

* https://docs.fastf1.dev

* https://github.com/theOehrly/Fast-F1

* http://ergast.com/mrd/

* https://www.kaggle.com/datasets/cjgdev/formula-1-race-data-19502017

* https://www.kaggle.com/datasets/jtrotman/formula-1-race-events
