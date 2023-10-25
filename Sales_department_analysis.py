import pandas as pd
import psycopg2 as ps

import datetime as dt

import seaborn as sns
import matplotlib.pyplot as plt
#connect to database
try:
    con = ps.connect(
    database="quest-db",
    user="rouser",
    password="ZI6MVnmi",
    host="178.62.242.91",
    port="5433"
    )
    display('It works! Connection was successful')
except:
    display('Unfortunately... it does not work...')

# all tables from data base
cur = con.cursor()
cur.execute("SELECT * FROM pg_catalog.pg_tables")
for table in cur.fetchall():
    if table[0] == 'public':
        display(table)
# Save to DF data from the table payments
sql = "SELECT * from payments"
cur.execute(sql)
data_pay = pd.read_sql(sql, con)

# convert the date and time attribute into datetime format and select the date as a separate attribute
data_pay['transaction_created_at'] = pd.to_datetime(data_pay['transaction_created_at'])
data_pay['transaction_date'] = data_pay['transaction_created_at'].apply(lambda x: x.date())

# Save data  DF 
sql = "SELECT * from events"
cur.execute(sql)
data_event = pd.read_sql(sql, con)

# convert attribute into datetime and select date as a separate attribute
data_event['happened_at'] = pd.to_datetime(data_event['happened_at'])
data_event['happened_date'] = data_event['happened_at'].apply(lambda x: x.date())

# Data from events_dict
sql = "SELECT * from events_dict order by id"
cur.execute(sql)
pd.read_sql(sql, con)

#Intelligence Data Analysis

#exapmle
data_event[data_event['_user_id'] == '12233445']

# Delete duplicates

dupl_cols = ['event_id', '_user_id', 'happened_at']

mask = data_event.duplicated(subset=dupl_cols)
event_dupl = data_event[mask]
print(f'Number of duplicates found: {event_dupl.shape[0]}')

data_event = data_event.drop_duplicates(subset=dupl_cols)
print(f'Result number of records: {data_event.shape[0]}')

# Delete duplicates payments

dupl_cols = ['transaction_created_at', 'transaction_id', 'currency', '_user_id', 'amount', 'is_gift']

mask = data_pay.duplicated(subset=dupl_cols)
pay_dupl = data_pay[mask]
print(f'Number of duplicates found: {event_dupl.shape[0]}')

data_pay = data_pay.drop_duplicates(subset=dupl_cols)
print(f'Result number of records: {data_pay.shape[0]}')

#events that often appear in the events table
data_event.groupby(['event_id'], as_index=False).nunique()

# How often does the value is gift = true occur in the table

data_pay['is_gift'].value_counts(normalize=True)

#Definition and calculation of metrics

# Create a dataframe for reports

beg_date = dt.date(2022, 3, 1) 
end_date = dt.date(2022, 3, 7)

data = pd.DataFrame(columns=['happened_date', 'requests_cum', 'calls_cum', 'intro_cum', 'pays_cum'])

for i in range(0, 7):
    rep_date = beg_date + dt.timedelta(days=i)

    # calculate cumulatively: the number of unique users who created applications, answered calls and completed demo lessons
    filtered_ev = data_event[data_event['happened_date'] <= rep_date]
    grouped_ev = filtered_ev.groupby(['event_id'], as_index=False).nunique()

    reqs = grouped_ev['_user_id'].values[5] 
    calls = grouped_ev['_user_id'].values[3] + grouped_ev['_user_id'].values[6] # event_id=4 + event_id=13 - звонки первой линии
    intros = grouped_ev['_user_id'].values[1] + grouped_ev['_user_id'].values[9] # event_id=11 + event_id=7 - вводные уроки

    # The number of unique users in the payment table for each date
    filtered_pay = data_pay[data_pay['transaction_date'] <= rep_date]
    pays = filtered_pay['_user_id'].nunique()

    new_row = {'happened_date': rep_date,
               'requests_cum': reqs,
               'calls_cum': calls,
               'intro_cum': intros,
               'pays_cum': pays}
    data = data.append(new_row, ignore_index=True)

    # Cumulative conversion
data['cr_request_cum'] = data['calls_cum']/data['requests_cum']
data['cr_call_cum'] = data['intro_cum']/data['calls_cum']
data['cr_intro_cum'] = data['pays_cum']/data['intro_cum']

# Daily number of active users - those who created applications and paid for classes
data['DAU_request'] = data['requests_cum'] - data['requests_cum'].shift()
data['DAU_payments'] = data['pays_cum'] - data['pays_cum'].shift()

# Bring the conversion to percentage form
data['cr_request_cum'] = data['cr_request_cum'].apply(lambda x: round(x*100, 2))
data['cr_call_cum'] = data['cr_call_cum'].apply(lambda x: round(x*100, 2))
data['cr_intro_cum'] = data['cr_intro_cum'].apply(lambda x: round(x*100, 2))

data

# Cumulative conversion chart
# save to the data folder for use by the bot as CR.png

fig, ax = plt.subplots(figsize=(8, 5))
sns.set_theme()

req = sns.lineplot(data = data, x='happened_date', y='cr_request_cum', label='User requests')
cals = sns.lineplot(data = data, x='happened_date', y='cr_call_cum', label='First line calls')
req = sns.lineplot(data = data, x='happened_date', y='cr_intro_cum', label='Introductory lessons')

ax.set_title('Cumulative conversion', fontdict={'fontsize': 20})
ax.xaxis.set_tick_params(rotation=45)
ax.set_xlabel('')
ax.set_ylabel('Percentage')

sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))

# Info about users activity
# After output, save to the data folder for use by the bot as DAU.png

fig, ax = plt.subplots(figsize=(8, 5))
sns.set_theme()

req = sns.barplot(data = data, x='happened_date', y='DAU_request', color="b", label='DAU - applications')
paid = sns.barplot(data = data, x='happened_date', y='DAU_payments', color="r", label='DAU - payment')

ax.set_title('Active users', fontdict={'fontsize': 20})
ax.xaxis.set_tick_params(rotation=45)
ax.set_xlabel('')
ax.set_ylabel('Number of persons')

fig.legend(loc='upper left', bbox_to_anchor=(0.65, 0.95))


# Daily Conversion

daily_filtered = events_daily[events_daily['happened_date'] > dt.date(2021, 6, 1)]

fig = plt.figure(figsize=(8, 4))
ax = fig.add_axes([1, 1, 1, 1])
ax.set_title('Daily Conversion', fontdict={'fontsize': 20})
ax.xaxis.set_tick_params(rotation=45)
ax.set_xlabel('')
ax.set_ylabel('Percentage')

sns.scatterplot(data=daily_filtered, x='happened_date', y='cr_requests_daily', ax=ax)
