# DataRovr #

## Build it ##
1. `git clone git@bitbucket.org:phdata/datarovr.git`
2. `cd datarovr`
3. `mvn package` or `mvn -DskipTests package`

## Run it ##
`java -jar target/datarovr-<version>.jar [options]`

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

#### metrics_csv_directory ####
Populate this paramter to download the individual metrics tables as CSV files in the provided
directory. The job will fail if any file already exists.

#### metrics ####
This is a comma separated list of metrics to generate. Currently the following metrics are
supported.

 * NumberDescription
 * MaskProfile
 * DateDescription
 * CorrelationMatrix

**System Default:** "NumberDescription,CorrelationMatrix,DateDescription,MaskProfile"

#### log_level ####
Use this to set the logging level of the snowpark client.

**System Default:** "warn"