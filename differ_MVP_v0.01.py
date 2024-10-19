"""
Differ MVP version 0.01
© 2024 Mark Sabin <morboagrees@gmail.com>
Released under Apache 2.0 license. Please see https://github.com/pyxel/differ/blob/main/LICENSE.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col as dfcol
from snowflake.snowpark import Row


session = get_active_session()
qh = session.query_history()


#####################
## Local df functions
#####################
def ldf_count(df):
    return len(df)


def ldf_filter(df, cols = None, rename_cols = None, filters = None):

    def row_filter(row, filters):
        if filters is None: return True
        keep = [row[filter[0]] == filter[1] for filter in filters]
        return all(keep)

    if cols is None:
        cols = list(df[0].as_dict().keys())

    if rename_cols is None:
        rename_cols = cols
        
    new_row = Row(*rename_cols)
    return [new_row(*[row[col] for col in cols]) for row in df if row_filter(row, filters)]
            

##############################
## Snowflake Database access
##############################
@st.cache_data(ttl = 10)
def get_snowflake_databases():
    df = session.sql("show databases").select('"name"').collect()
    return [{"database": row.name} for row in df]


@st.cache_data(ttl = 10)
def get_snowflake_schemas(database):
    df = session.sql(f"show schemas in database {database}").select('"name"').collect()
    return [{"schema": row.name} for row in df]


@st.cache_data(ttl = 10)
def get_snowflake_tables(schema, include_views = True):
    try:
        df = session.sql(f"show tables in schema {schema}").select('"name"').collect()
        d = [{"table": row.name} for row in df]
        if include_views:
            df = session.sql(f"show views in schema {schema}").select('"name"').collect()
            d += [{"table": row.name} for row in df]
    except:
        d = {}
    return d


def get_snowflake_columns(sql):
    df = session.sql(sql)
    return [{"column": row} for row in df.columns]


def get_filtered_cte_sql(tables, wheres):
    where_filter_a = 'True' if wheres[0] == "" or wheres[0] is None else wheres[0]
    where_filter_b = 'True' if wheres[1] == "" or wheres[1] is None else wheres[1]

    sql = f"""with
a as ({tables[0]}),
b as ({tables[1]})
"""

    return sql
    

@st.cache_data(ttl = 10)
def get_snowflake_check_expression(expression, table, where = None):
    """Checks if a SQL expression is valid."""
    valid = True
    where_expr = "" if where is None or where.strip() == "" else f" and ({where})"
    sql = f"select {expression} from ({table}) where false{where_expr}"
    try:
        df = session.sql(sql).collect()
    except:
        valid = False
    return valid



def get_data(tables, cols, keys, wheres):
    
    l_select = []
    l_where = []
    for col in cols:
        l_select.append(f"a.{col} as {col}_a")
        l_select.append(f"b.{col} as {col}_b")
        l_select.append(f"case when a.{col} = b.{col} or (a.{col} is null and b.{col} is null) then false else true end as {col}_diff")
        l_where.append(f"{col}_diff")

    l_select.append(f"case when a.{keys[0]} = b.{keys[1]} then false else true end as key_diff")

    select = ',\n'.join(l_select)
    where_diff = '\nor '.join(l_where)

    sql = get_filtered_cte_sql(tables, wheres)
    sql += f"""

select {select}
from a
full join b on b.{keys[1]} = a.{keys[0]}
where
    ({where_diff})
    --and not key_diff
"""

    df = session.sql(sql).collect() #.to_pandas()
    
    return df


def get_pk_summary(tables, cols, keys, labels, wheres):

    sql = get_filtered_cte_sql(tables, wheres)
    
    sql += f"""
select
    'Total unique key values' as metric, count(distinct ifnull(a.{keys[0]}, b.{keys[1]})) as number
from a
full join b on b.{keys[1]} = a.{keys[0]}
group by 1
union all
select
    'Matching key values' as metric, sum(case when a.{keys[0]} = b.{keys[1]} then 1 else 0 end) as number
from a
full join b on b.{keys[1]} = a.{keys[0]}
union all
select
    'Keys only in {labels[0]}', sum(case when a.{keys[0]} is not null and b.{keys[1]} is null then 1 else 0 end) as number
from a
full join b on b.{keys[1]} = a.{keys[0]}
union all
select
    'Keys only in {labels[1]}', sum(case when b.{keys[1]} is not null and a.{keys[0]} is null then 1 else 0 end) as number
from a
full join b on b.{keys[1]} = a.{keys[0]}
"""

    df = session.sql(sql).collect()

    return df


def get_row_summary(tables, cols, keys, labels, wheres):

    sql = get_filtered_cte_sql(tables, wheres)
    
    sql += f"""
select
    'Total rows in {labels[0]}' as metric, count(*) as number
from a
union all
select
    'Total rows in {labels[1]}' as metric, count(*) as number
from b
"""

    df = session.sql(sql).collect()

    return df
    

###########
## Page
###########
st.title('Differ')
st.write("Compare two datasets to find differences.")


# Initialise variables
if 'run_diff' not in st.session_state:
    st.session_state.run_diff = False
if st.session_state.get('btn_run_diff', False) == True:
    st.session_state.run_diff = True
if st.session_state.get('btn_new_diff', False) == True:
    st.session_state.run_diff = False

    
tab_define, tab_summary, tab_columns, tab_rows = st.tabs(['Define', 'Summary', 'Columns', 'Rows'])

#############
## tab_define
#############
dataset_labels_internal = ['A', 'B']
if 'dataset_labels' not in st.session_state:
    st.session_state.dataset_labels = dataset_labels_internal

queries = [
    "select * from snowflake_sample_data.tpch_sf10.region",
    "select * from differ.differ.region"
]
cols = tab_define.columns(2)
i = 0

for col in cols:
    label = dataset_labels_internal[i].lower()
    col.text_input(
        label = "Dataset label",
        value = st.session_state.dataset_labels[i],
        key = f"dataset_label_{label}",
        disabled = st.session_state.run_diff,
        help = "A descriptive label for the dataset, such as new/old or dev/prod."
    )
    
    sql = col.text_area(
        label = f"SQL query",
        key = f"sql_{label}",
        disabled = st.session_state.run_diff,
        help = "SQL query to fetch the dataset for comparison. Both datasets must have the same schema."
    )   
    
    key_text = col.text_input(
        label = "Join key",
        key = f"key_{label}",
        disabled = st.session_state.run_diff,
        help = "Enter the name of the key column used to join the two datasets."
    )
    i += 1

run_diff = cols[0].button('Run differ', key = 'btn_run_diff', disabled = st.session_state.run_diff)
new_diff = cols[1].button('New diff', key = 'btn_new_diff', disabled = not st.session_state.run_diff)

if new_diff:
    st.session_state.run_diff = False


if run_diff:
    with st.spinner('Running differ...'):
        # Get input values from the form.
        st.session_state.run_diff = True
        sql_a = st.session_state.sql_a
        sql_b = st.session_state.sql_b
        key_a = st.session_state.key_a.upper()
        key_b = st.session_state.key_b.upper()
        if key_b.strip() == "" or key_b is None: key_b = key_a
        label_a = st.session_state.dataset_label_a
        label_b = st.session_state.dataset_label_b

        # Validate tables and keys entered.
        validations = [
            ('1', sql_a, f'Invalid SQL query: {sql_a}.'),
            ('1', sql_b, f'Invalid SQL query: {sql_b}.'),
            (key_a, sql_a, f'Invalid key for SQL {label_a}: {key_a}.'),
            (key_b, sql_b, f'Invalid key for SQL {label_b}: {key_b}.'),
        ]
        valid = True
        for i in range(len(validations)):
            validation = validations[i]
            # Don't do further checks if tables are not valid.
            if i >= 2 and not valid: break
            if not get_snowflake_check_expression(
                expression = validation[0],
                table = validation[1]
            ):
                tab_define.error(validation[2], icon="❌")
                run_diff = False
                st.session_state.run_diff = False
                valid = False

        # Run the diff
        if run_diff:
            columns = [col['column'] for col in get_snowflake_columns(sql_a)]
            data = get_data(
                tables = [sql_a, sql_b],
                cols = columns,
                keys = [key_a, key_b],
                wheres = [None, None]
            )
            pk_summary = get_pk_summary(
                tables = [sql_a, sql_b],
                cols = columns,
                keys = [key_a, key_b],
                labels = [label_a, label_b],
                wheres = [None, None]
            )
            row_summary = get_row_summary(
                tables = [sql_a, sql_b],
                cols = columns,
                keys = [key_a, key_b],
                labels = [label_a, label_b],
                wheres = [None, None]
            )
            if len(data) == 0:
                st.info('Datasets match!', icon="✅")
            else:
                tab_define.info('Differences found. Go to the Summary or Columns tabs for details.', icon="ℹ️")

##############
## tab_summary
##############

if st.session_state.run_diff:
    tab_summary.subheader("Primary key summary")

    df = pk_summary
    tab_summary.dataframe(df, hide_index = True)

    tab_summary.subheader("Row summary:")
    df = row_summary
    if len(data) == 0:
        different_rows = 0
    else:
        different_rows = len(ldf_filter(df = data, filters = [('KEY_DIFF', False)]))
    identical_rows = ldf_filter(
        df = pk_summary,
        cols = ['NUMBER'],
        filters = [('METRIC', 'Matching key values')]
    )[0][0] - different_rows
    
    df2 = session.create_dataframe([["Identical rows", int(identical_rows)], ["Different rows", int(different_rows)]], schema=["METRIC", "NUMBER",]).collect()
    df += df2
    tab_summary.dataframe(df, hide_index = True)    
else:
    tab_summary.write("Summary will be displayed here. Use the define tab to run a diff.")


##############
## tab_columns
##############
if st.session_state.run_diff and len(data) > 0:
    if different_rows == 0:
        tab_columns.info('All rows with matching keys are identical.', icon="✅")
    else:
        for column in [column for column in columns if column not in [key_a]]:
            df = ldf_filter(
                df = data,
                cols = [f"{key_a}_A", f"{column}_A", f"{column}_B"],
                rename_cols = [key_a, label_a, label_b],
                filters = [(f'{column}_DIFF', True), ('KEY_DIFF', False)]
            )
            row_count = len(df)
            if row_count == 0: continue
            
            columns_expander = tab_columns.expander(label = f"{column} ({row_count} rows)")
             
            columns_expander.dataframe(df, hide_index = True)

else:
    tab_columns.write("Column diff details will be displayed here. Use the define tab to run a diff.")


##############
## tab_rows
##############
if st.session_state.run_diff:

    # Get rows that are only in A
    df = ldf_filter(
        df = data,
        cols = [f"{col}_A" for col in columns],
        rename_cols = columns,
        filters = [('KEY_DIFF', True)]
    )
    # Workaround the lack of "not null" filter
    new_row = Row(*columns)
    df = [new_row(*[row[col] for col in columns]) for row in df if row[key_a] is not None]
    row_count = len(df)
    rows_expander = tab_rows.expander(label = f"Keys only in {label_a} ({row_count} rows)")
    rows_expander.dataframe(df, hide_index = True)

    # Get rows that are only in B
    df = ldf_filter(
        df = data,
        cols = [f"{col}_B" for col in columns],
        rename_cols = columns,
        filters = [('KEY_DIFF', True)]
    )
    # Workaround the lack of "not null" filter
    new_row = Row(*columns)
    df = [new_row(*[row[col] for col in columns]) for row in df if row[key_b] is not None]
    row_count = len(df)
    rows_expander = tab_rows.expander(label = f"Keys only in {label_b} ({row_count} rows)")
    rows_expander.dataframe(df, hide_index = True)
    
else:
    tab_rows.write("Row diff details will be displayed here. Use the define tab to run a diff.")


st.divider()
l, c, r = st.columns([22,40,30])

