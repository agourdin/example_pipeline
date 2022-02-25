# PYTHON IMPORTS
from datetime import datetime, timedelta

# AIRFLOW IMPORTS
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator

# COMMON IMPORTS
# Change imported schedule interval as needed
from constants import DAILY_SCHEDULE_INTERVAL_L3 as schedule_interval
from sensors import AhistoricSensor
from utils import create_is_school_year, create_skip_day_task

# OTHER IMPORTS

# DEFAULT ARGS #################################################################
default_args = {
    'owner': 'howard',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2022, 2, 9),
    'email': ['warehouse@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_exponential_backoff': True,
    'retry_delay': timedelta(minutes=5),
}

# CONSTANTS AND VARIABLES SETUP ################################################
destination_schema = 'public'
view_name = 'daily_attendance'
table_name = view_name + '_historical'

# DAG DEFINITION ###############################################################
version = '3.1'
this_dag = table_name + '_v' + version

dag = DAG(
    this_dag,
    max_active_runs=5,
    default_args=default_args,
    schedule_interval=schedule_interval,
)

# SHORTCIRCUIT IF IT'S THE SUMMER ##############################################
is_school_year = create_is_school_year(dag)

# SKIP DAY TASKS ###############################################################
skip_days = [6, 7]

skip_day = create_skip_day_task(dag, skip_days)

# WAIT TASK(S) #################################################################
period_attendance_source_dag_id = 'period_attendance_historical_v%'
period_attendance_wait_task_id = 'wait_period_attendance'
wait_period_attendance = AhistoricSensor(
    dag=dag,
    task_id=period_attendance_wait_task_id,
    source_dag_id=period_attendance_source_dag_id,
    poke_interval=5*60,  # every 5 minutes
    timeout=30*60,  # half an hour
    mode='reschedule' # don't take up a worker slot
)

# CREATE TASK ##################################################################
create = SnowflakeOperator(
    dag=dag,
    task_id='create',
    sql="""
CREATE TABLE IF NOT EXISTS {destination_schema}.{table_name} (
    as_of DATE
    , school_date DATE
    , student_id INTEGER
    , site_id INTEGER
    , is_present BOOLEAN
    , is_unreconciled_absence BOOLEAN
    , is_excused BOOLEAN
    , is_tardy BOOLEAN
    , include_student BOOLEAN
    , academic_year INTEGER
    , is_regular_year BOOLEAN
    , is_expeditions BOOLEAN
    , num_periods_today INTEGER
    , num_periods_present_today INTEGER
    , num_periods_present_tardy_today INTEGER
    , num_periods_present_not_tardy_today INTEGER
    , num_periods_absent_today INTEGER
    , num_periods_absent_unreconciled_today INTEGER
    , num_periods_absent_excused_today INTEGER
    , num_periods_absent_unexcused_today INTEGER
    , pct_days_present_ytd FLOAT
    , pct_days_tardy_ytd FLOAT
    , pct_days_present_prior_5_school_days FLOAT
    , pct_days_tardy_prior_5_school_days FLOAT
    , pct_periods_present_ytd FLOAT
    , pct_periods_present_tardy_ytd FLOAT
    , pct_periods_absent_ytd FLOAT
    , pct_periods_absent_excused_ytd FLOAT
    , pct_periods_absent_unexcused_ytd FLOAT
    , pct_periods_present_prior_5_school_days FLOAT
    , pct_periods_present_tardy_prior_5_school_days FLOAT
    , pct_periods_absent_prior_5_school_days FLOAT
    , pct_periods_absent_excused_prior_5_school_days FLOAT
    , pct_periods_absent_unexcused_prior_5_school_days FLOAT
)
    """.format(
        destination_schema=destination_schema,
        table_name=table_name
    )
)

# DELETE TASK ##################################################################
delete = SnowflakeOperator(
    dag=dag,
    task_id='delete',
    sql="""
DELETE FROM {destination_schema}.{table_name}
WHERE school_date = '{today}'
    """.format(
        destination_schema=destination_schema,
        table_name=table_name,
        today="{{ ds }}",
    )
)

# INSERT TASK(S) ###############################################################
insert_sql = """
INSERT INTO {destination_schema}.{table_name}
WITH
schools_in_session AS (
    SELECT *
    FROM public.calendar
    WHERE calendar_date = '{today}' AND is_school_day
)
, students_by_state AS (
    SELECT *
    FROM public.students_historical
    WHERE
        state = '{state}'
        AND school_date = '{today}'
)
, first_period_attendance AS (
    SELECT period_attendance.*
    FROM schools_in_session AS calendar
    LEFT JOIN public.period_attendance_historical AS period_attendance
        ON calendar.site_id = period_attendance.site_id
        AND calendar.calendar_date = period_attendance.school_date
    WHERE
        period_attendance.period_order = 1
        AND school_date = '{today}'
)
, todays_attendance AS (
    SELECT *
    FROM {sis_schema}.att_adaadm_defaults_all_latest
    WHERE
        TRY_TO_DATE(_calendardate) = '{today}'
)
, daily_attendance_today AS (
    SELECT DISTINCT
        DATEADD(day, -1, CURRENT_DATE) AS as_of
        , '{today}' AS school_date
        , students.student_id
        , students.site_id
        , {state_present_calc} AS is_present
        , NULL AS is_unreconciled_absence
        , NULL AS is_excused
        , first_period_attendance.is_tardy
        , students.include_student
        , students.academic_year
        , students.is_regular_year
        , IFF(
            students.school_type = 'middle_school'
            , schools_in_session.is_ms_expeditions
            , schools_in_session.is_hs_expeditions
        ) AS is_expeditions
    FROM schools_in_session
    INNER JOIN students_by_state AS students
        ON schools_in_session.site_id = students.site_id
    LEFT JOIN todays_attendance AS attendance
        ON students.sis_student_id = attendance._studentid
    LEFT JOIN first_period_attendance
        ON students.student_id = first_period_attendance.student_id
)
, daily_attendance_ytd AS (
    SELECT *
    FROM daily_attendance_today
    
    UNION ALL
    
    SELECT
        as_of
        , school_date
        , student_id
        , site_id
        , is_present
        , is_unreconciled_absence
        , is_excused
        , is_tardy
        , include_student
        , academic_year
        , is_regular_year
        , is_expeditions
    FROM main.public.daily_attendance_historical
    WHERE
        school_date < '{today}'
        AND academic_year = main.public.get_academic_year('{today}')
)
, day_level_ytd AS (
    SELECT DISTINCT
        *
        , SUM(IFF(is_present = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_days_present_ytd
        , ROUND(num_days_present_ytd / SUM(1) OVER (PARTITION BY student_id) * 100, 1)
          AS pct_days_present_ytd
        , SUM(IFF(is_present = TRUE AND is_tardy = TRUE  , 1, 0))
          OVER (PARTITION BY student_id)
          AS num_days_tardy_ytd
        , ROUND(num_days_tardy_ytd / SUM(1) OVER (PARTITION BY student_id) * 100, 1)
          AS pct_days_tardy_ytd
    FROM daily_attendance_ytd
    WHERE school_date <= '{today}'
)
, day_level_prior_5_school_days AS (
    SELECT DISTINCT
        *
        , COUNT(school_date) OVER (
            PARTITION BY student_id
        ) AS num_days_enrolled_prior_5_days
        , SUM(IFF(is_present = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_days_present_prior_5_school_days
        , ROUND(
            num_days_present_prior_5_school_days
            / num_days_enrolled_prior_5_days
            * 100
          , 1
        ) AS pct_days_present_prior_5_school_days
        , SUM(IFF(is_present = TRUE AND is_tardy = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_days_tardy_prior_5_school_days
        , ROUND(
            num_days_tardy_prior_5_school_days
            / num_days_enrolled_prior_5_days
            * 100
          , 1
        ) AS pct_days_tardy_prior_5_school_days
    FROM daily_attendance_ytd
    WHERE main.public.calculate_school_days_between('{today}', school_date, site_id) < 5
)
, period_level_today AS (
    SELECT DISTINCT
        as_of
        , school_date
        , student_id
        , site_id
        , SUM(1) OVER (PARTITION BY student_id) AS num_periods_today
        , SUM(IFF(is_present = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_present_today
        , SUM(IFF(is_present = TRUE AND is_tardy, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_present_tardy_today
        , SUM(IFF(is_present = TRUE AND (is_tardy IS NULL OR is_tardy = FALSE), 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_present_not_tardy_today
        , SUM(IFF(is_present = FALSE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_today
        , SUM(IFF(is_present = FALSE AND is_unreconciled_absence = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_unreconciled_today
        , SUM(IFF(is_present = FALSE AND is_excused = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_excused_today
        , SUM(IFF(is_present = FALSE AND is_excused = FALSE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_unexcused_today
    FROM public.period_attendance_historical
    WHERE
        school_date = '{today}'
)
, period_level_ytd AS (
    SELECT DISTINCT
        as_of
        , school_date
        , student_id
        , site_id
        , SUM(1) OVER (PARTITION BY student_id) AS num_periods_ytd
        , SUM(IFF(is_present = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_present_ytd
        , ROUND(num_periods_present_ytd / num_periods_ytd * 100, 1)
          AS pct_periods_present_ytd
        , SUM(IFF(is_present = TRUE AND is_tardy = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_present_tardy_ytd
        , ROUND(num_periods_present_tardy_ytd / num_periods_ytd * 100, 1)
          AS pct_periods_present_tardy_ytd
        , SUM(IFF(is_present = FALSE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_ytd
        , ROUND(num_periods_absent_ytd / num_periods_ytd * 100, 1)
          AS pct_periods_absent_ytd
        , SUM(IFF(is_present = FALSE AND is_excused = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_excused_ytd
        , ROUND(
          num_periods_absent_excused_ytd / num_periods_ytd * 100
          , 1
        ) AS pct_periods_absent_excused_ytd
        , SUM(IFF(is_present = FALSE AND is_excused = FALSE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_unexcused_ytd
        , ROUND(
          num_periods_absent_unexcused_ytd / num_periods_ytd * 100
          , 1
        ) AS pct_periods_absent_unexcused_ytd
    FROM public.period_attendance_historical
    WHERE
        academic_year = main.public.get_academic_year('{today}')
        AND school_date <= '{today}'
    ORDER BY 1, 2 DESC, 3
)
, period_level_prior_5_school_days AS (
    SELECT DISTINCT
        as_of
        , school_date
        , student_id
        , site_id
        , SUM(1) OVER (PARTITION BY student_id)
          AS num_periods_prior_5_school_days
        , SUM(IFF(is_present = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_present_prior_5_school_days
        , ROUND(
            num_periods_present_prior_5_school_days
            / num_periods_prior_5_school_days
            * 100
          , 1
         ) AS pct_periods_present_prior_5_school_days
        , SUM(IFF(is_present = TRUE AND is_tardy = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_present_tardy_prior_5_school_days
        , ROUND(
            num_periods_present_tardy_prior_5_school_days
            / num_periods_prior_5_school_days
            * 100
          , 1
         ) AS pct_periods_present_tardy_prior_5_school_days
        , SUM(IFF(is_present = FALSE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_prior_5_school_days
        , ROUND(
            num_periods_absent_prior_5_school_days
            / num_periods_prior_5_school_days
            * 100
          , 1
        ) AS pct_periods_absent_prior_5_school_days
        , SUM(IFF(is_present = FALSE AND is_excused = TRUE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_absent_excused_prior_5_school_days
        , ROUND(
            num_periods_absent_excused_prior_5_school_days
            / num_periods_prior_5_school_days
            * 100
          , 1
        ) AS pct_periods_absent_excused_prior_5_school_days
        , SUM(IFF(is_present = FALSE AND is_excused = FALSE, 1, 0))
          OVER (PARTITION BY student_id)
          AS num_periods_unexcused_prior_5_school_days
        , ROUND(
            num_periods_unexcused_prior_5_school_days
            / num_periods_prior_5_school_days
            * 100
          , 1
        ) AS pct_periods_absent_unexcused_prior_5_school_days
    FROM public.period_attendance_historical
    WHERE
        academic_year = main.public.get_academic_year('{today}')
        AND school_date <= '{today}'
        AND main.public.calculate_school_days_between('{today}', school_date, site_id) < 5
)

SELECT DISTINCT
    daily_attendance_today.*
    , period_level_today.num_periods_today
    , period_level_today.num_periods_present_today
    , period_level_today.num_periods_present_tardy_today
    , period_level_today.num_periods_present_not_tardy_today
    , period_level_today.num_periods_absent_today
    , period_level_today.num_periods_absent_unreconciled_today
    , period_level_today.num_periods_absent_excused_today
    , period_level_today.num_periods_absent_unexcused_today
    , day_level_ytd.pct_days_present_ytd
    , day_level_ytd.pct_days_tardy_ytd
    , day_level_prior_5_school_days.pct_days_present_prior_5_school_days
    , day_level_prior_5_school_days.pct_days_tardy_prior_5_school_days
    , period_level_ytd.pct_periods_present_ytd
    , period_level_ytd.pct_periods_present_tardy_ytd
    , period_level_ytd.pct_periods_absent_ytd
    , period_level_ytd.pct_periods_absent_excused_ytd
    , period_level_ytd.pct_periods_absent_unexcused_ytd
    , period_level_prior_5_school_days.pct_periods_present_prior_5_school_days
    , period_level_prior_5_school_days.pct_periods_present_tardy_prior_5_school_days
    , period_level_prior_5_school_days.pct_periods_absent_prior_5_school_days
    , period_level_prior_5_school_days.pct_periods_absent_excused_prior_5_school_days
    , period_level_prior_5_school_days.pct_periods_absent_unexcused_prior_5_school_days
FROM daily_attendance_today
LEFT JOIN day_level_ytd
    ON daily_attendance_today.student_id = day_level_ytd.student_id
    AND daily_attendance_today.school_date = day_level_ytd.school_date
LEFT JOIN day_level_prior_5_school_days
    ON daily_attendance_today.student_id = day_level_prior_5_school_days.student_id
    AND daily_attendance_today.school_date = day_level_prior_5_school_days.school_date
LEFT JOIN period_level_today
    ON daily_attendance_today.student_id = period_level_today.student_id
    AND daily_attendance_today.school_date = period_level_today.school_date
LEFT JOIN period_level_ytd
    ON daily_attendance_today.student_id = period_level_ytd.student_id
    AND daily_attendance_today.school_date = period_level_ytd.school_date
LEFT JOIN period_level_prior_5_school_days
    ON daily_attendance_today.student_id = period_level_prior_5_school_days.student_id
    AND daily_attendance_today.school_date = period_level_prior_5_school_days.school_date
ORDER BY
    daily_attendance_today.site_id ASC
    , daily_attendance_today.student_id ASC
"""

# CA INSERT TASK ##########################################################
ca_present_calculation = "attendance._pctdayposattpct > 0"
ca_schema = 'powerschool_ca'

insert_ca = SnowflakeOperator(
    dag=dag,
    task_id='insert_ca',
    sql=insert_sql.format(
        destination_schema=destination_schema,
        sis_schema=ca_schema,
        state='CA',
        state_present_calc=ca_present_calculation,
        table_name=table_name,
        today='{{ ds }}',
    )
)

# WA INSERT TASK ##########################################################
wa_present_calculation = "attendance._attendancevalue >= 0.5"
wa_schema = 'powerschool_wa'

insert_wa = SnowflakeOperator(
    dag=dag,
    task_id='insert_wa',
    sql=insert_sql.format(
        destination_schema=destination_schema,
        sis_schema=wa_schema,
        state='WA',
        state_present_calc=wa_present_calculation,
        table_name=table_name,
        today='{{ ds }}',
    )
)

# CREATE VIEW TASK #############################################################
site_id_column_name = 'site_id'
create_view = SnowflakeOperator(
    dag=dag,
    task_id='create_view',
    sql="""
CREATE OR REPLACE VIEW {destination_schema}.{view_name} AS
SELECT {view_name}.*
FROM public.{table_name} AS {view_name}
INNER JOIN (
    SELECT
        {site_id_column_name}
        , MAX(school_date) AS last_school_day
    FROM public.{table_name}
    -- Filter out Rainier records
    WHERE {site_id_column_name} <> 1
    GROUP BY {site_id_column_name}
) AS most_recent_day
    ON {view_name}.{site_id_column_name} = most_recent_day.{site_id_column_name}
    AND {view_name}.school_date = most_recent_day.last_school_day
    """.format(
        destination_schema=destination_schema,
        site_id_column_name=site_id_column_name,
        table_name=table_name,
        view_name=view_name,
    )
)

# SUCCESS TASK #################################################################
success_task = DummyOperator(
    dag=dag,
    task_id='success_task',
)

# TASK ORDER ###################################################################
# Standard circuit breaker to stop pipeline if execution date does not fall during
# the regular school year.
is_school_year >> skip_day >> [create, wait_period_attendance] >> delete
delete >> [insert_ca, insert_wa] >> create_view >> success_task
