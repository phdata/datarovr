# DataRovr #

## Build it ##
1. `git clone git@bitbucket.org:phdata/datarovr.git`
2. `cd datarovr`
3. `vim env.sh` (see note below)
4. `source env.sh`
5. `mvn package` or `mvn -DskipTests package`

**NOTE:**
Building with tests requires being able to authenticate to Snowflake. The simplest way
is to set up your environment to have the necessary variables populated. Usually we
recommend creating an `env.sh` in the root of the repository (this is already git
ignored). And populating it with the following variables at minimum:

```shell
export DR_SNOWFLAKE_URL=https://snowdata.snowflakecomputing.com
export DR_SNOWFLAKE_DB=<connection database>
export DR_SNOWFLAKE_SCHEMA=<connection schema>
export DR_SNOWFLAKE_WAREHOUSE=DEFAULT_USER_WH
export DR_SNOWFLAKE_USER=<your user account>
export DR_SNOWFLAKE_PRIVATE_KEY_FILE=~/snowflake_rsa_key.p8
export DR_SNOWFLAKE_PRIVATE_KEY_FILE_PWD=<your keyfile password>
```

## Run it ##
With the same environment variables populated as above:

`java -jar target/datarovr-<version>.jar [options]`

Most of those options can also be passed via the command line if you prefer.

After the connection parameters, the most important parameters are as follows
(you can read more about each option in the "Configure it" section):

### Select Options ###
`--metrics` Select which metrics to run. (default: all metrics)
`--tables` Select which tables to profile. (default: "*")

### Action Options ###
At least one of these options is required for the software to actually do anything.

`--report_html` Name of the html file to write visualizations to.

Populating this option will result in the reports file being generated.


`--metric_table_prefix` Prefix for the metric table names.

Populating this will result in the data for each metric being appended to tables
named with this as a prefix, and the metric's "table name" as a suffix.


`--metric_dump_table` Name of the metric dump table.

Populating this will result in all metrics being normalized into the same set of columns
and that data being appended to this table.


`--metric_csv_directory` Name of the directory to store metric data locally.

Populating this will create a directory by the passed name (if it doesn't exist)
and then store a csv file for each metric selected. The program will refuse to
overwrite existing files.


For a concrete working example:

```shell
java -jar target/datarover-1.0-SNAPSHOT.jar \
  --tables "snowflake_sample_data.tpch_sf1.orders" \
  --report_html report.html \
  --metric_table_prefix datarovr.
```

This command will simultaneously generate an html report, and append the generated
data in metric tables in the "DATAROVR" schema (the schema must be created first).

## Configure it ##
There are several configuration options available, some of which are required.
Passing configuration parameters can be done on the command line, via the environment,
or via a config file. Prefix the parameter with a `--` and use it as a command line
option. Prefix it with a `DR_` and make it upper case to pass it in as an environment
variable. Finally, pass in `--config_file <some_file.conf>` and populate that file
with `<parameter> = <value>` lines. Some parameters have default values embedded
in the jar. The parameters are evaluated in the following order, where parameters
configured higher in the list will over-write parameters at the lower layers:

1. Command Line Options
2. Environment Variables
3. Configuration File
4. System Default

It should be noted that the `--config_file` parameter is never checked from a file
because of the inherent circular reference.

### Configuration Parameters ###

#### snowflake_url ####
**Required.** This is the URL to your snowflake account. For example,
`https://phdata.snowflakecomputing.com`

#### snowflake_db ####
**Required.** This is the default database for your connection.

#### snowflake_schema ####
**Required.** This is the default schema for your connection.

#### snowflake_warehouse ####
**Required.** This is the warehouse to use for your profiling.

#### snowflake_role ####
This is the role to connect as.

#### snowflake_user ####
**Required.** This is the user account to connect with.

#### snowflake_password ####
**Note:** This parameter is excluded from the command line. Use environment variables or files.

This is the clear text password for your `snowflake_user`.

#### snowflake_private_key_file ####
This is the path to the private RSA key for your `snowflake_user`.

#### snowflake_private_key_file_pwd ####
This is the clear text private key file password.

#### snowflake_privatekey ####
**Note:** This parameter is excluded from the command line. Use environment variables or files.

This is the clear text private key.

>At least one of `snowflake_password`, `snowflake_privatekey`, or the 
> `snowflake_private_key_file*` pair are required

#### config_file ####
This is the path to a configuration file from which to parse these options.

**System Default:** datarovr.conf

#### tables ####
Pass a glob triplet to this parameter to match any number of tables your 
`snowflake_user` has access to. For example, you can pass `*.*.*` to match
all tables. Pass `dev.accounting.*` to match all tables in the `"DEV"."ACCOUNTING"` schema.
Pass `"dev"."acc*".tbl_?` to match `"dev"."accounting"."TBL_A"` and 
`"dev"."accessibility"."TBL_1"`. A missing database or database *and* schema will default
with the related snowflake parameters to provide relative scoping of tables. You get the idea.
You can escape glob characters with `\` to match literal special characters in table names.

**System Default:** "*"

#### metric_table_prefix ####
Populate this parameter to append metrics to individual tables named with this as a prefix. 
For example `--metric_table_prefix metrics_` will create a `"METRICS_DATE_DESCRIPTION_METRIC"` 
in the connection database and schema, if you run with the `DateDescription` metric.

If you have a `.` in the prefix, the system will attempt to write to a schema named with 
everything before the `.`. Please ensure that schema exists prior to running DataRovr.
Running with `--metric_table_prefix metrics.` will attempt to write to the table
`"METRICS"."DATE_DESCRIPTION_METRIC"` table, if the `"METRICS"` schema already exist 
in your connection database.

#### metric_dump_table ####
Populate this parameter to normalize all metrics into a single set of fields 
(using a JSON `VALUES` column) and write those normalized metrics into a single table
named with this value.

#### metric_csv_directory ####
Populate this paramter to download the individual metrics tables as CSV files in the provided
directory. The job will fail if any file already exists.

#### metrics ####
This is a comma separated list of metrics to generate. Currently the following metrics are
supported.

 * NumberDescription
 * MaskProfile
 * DateDescription
 * CorrelationMatrix
 * Entropy

**System Default:** "NumberDescription,CorrelationMatrix,DateDescription,MaskProfile,Entropy"

#### log_level ####
Use this to set the logging level of the snowpark client.

**System Default:** "warn"

#### report_html ####
This parameter will generate an HTML report that can be viewed in a browser to visualize the
profile data.