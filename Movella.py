#!/usr/bin/env python
# coding: utf-8

# # Assumptions
# 1) if GA (Goals against is Null; GA is set to 0  
# 2) if SA (Shots against) is Null; SA is set to GA  
# 3) if any of GP, Min, W, L is Null; the column is set to 0
# 

# ### copy CSV file from
# https://www.kaggle.com/open-source-sports/professional-hockey-database?select=Goalies.csv
# 
# into same directory as this ipynb

# In[1]:


import numpy as np
import pandas as pd
# Connect to sqlite
import sqlite3
con = sqlite3.connect('movella.db')


# In[2]:


# read CSV into a dataframe
df = pd.read_csv('Goalies.csv')
# move to table
df.to_sql('Goalies', con, if_exists='replace', index=False)


# In[3]:


cur = con.cursor()


# In[4]:


# Data scrubbing
# assumptions declared up top
GoaliesClean = pd.DataFrame(cur.execute('''select tmID    as 'tmID'
                                        ,year             as 'Year'    
                                        ,playerid         as 'playerid'
                                        ,COALESCE(W,0)    as 'W'
                                        ,COALESCE(L,0)    as 'L'
                                        ,COALESCE(GP,0)   as 'GP'
                                        ,COALESCE(Min,0)  as 'Min'
                                        ,COALESCE(GA,0)   as 'GA'
                                        ,CASE
                                             WHEN SA is NULL and GA is NULL then 0
                                             WHEN SA is Null and GA is not NULL then GA
                                             ELSE SA
                                         END              as 'SA'

                                        FROM Goalies
                                       order by tmID
                                               ,year'''),
                    columns = ['tmID'
                              ,'Year'
                              ,'playerid'
                              ,'W'
                              ,'L'
                              ,'GP'
                              ,'Min'
                              ,'GA'
                              ,'SA'])


# In[5]:


# Display scrubbed data
GoaliesClean


# In[6]:


# Write scrubbed data to table
GoaliesClean.to_sql('GoaliesClean', con, if_exists='replace', index=False)


# In[7]:


for row in cur.execute('''select *
                           FROM GoaliesClean
                           where tmID in ('ANA')
                           order by 1,2'''):
    print(row)


# In[8]:


# create initial aggregate table --> AggT
AggT = pd.DataFrame(cur.execute('''select tmID    as 'tmID'
                                ,year             as 'Year'    
                                ,count(playerid)  as 'TotPlayers'
                                ,sum(W)           as 'TotWins'
                                ,sum(L)           as 'TotLoses'
                                ,sum(GP)          as 'TotGamesPlayed'
                                ,sum(Min)         as 'TotMins'
                                ,sum(GA)          as 'TotGoalsagainst'
                                ,sum(SA)          as 'TotShotsagainst'
                           FROM GoaliesClean
                           group by tmid
                                   ,year
                            order by 1'''),
                    columns = ['tmID'
                              ,'Year'
                              ,'TotPlayers'
                              ,'TotWins'
                              ,'TotLoses'
                              ,'TotGamesPlayed'
                              ,'TotMins'
                              ,'TotGoalsAgainst'
                              ,'TotShotAgainst'])


# In[9]:


# Display the initial aggregate 
AggT


# In[10]:


# save to table
AggT.to_sql('AggT', con, if_exists='replace', index=False)


# In[11]:


# answers to questions 1 - 8
# Coalesce on aggregates with possible divide by zero to avoid Null results
# Null SA's is defaulted to GA as per assumption declaration up top
Ans_1_to_8 = pd.DataFrame(cur.execute('''select tmID
      ,Year
      ,coalesce(round(TotWins/TotPlayers,2),0)             as 'Win_agg'
      ,coalesce(round(TotLoses/TotPlayers,2),0)            as 'Losses_agg'
      ,coalesce(round(TotGamesPlayed/TotPlayers,2),0)      as 'GP_agg'
      ,coalesce(round(TotMins/TotGoalsAgainst,2),0)        as 'Mins_over_GA_agg'
      ,coalesce(round(TotGoalsAgainst/TotShotAgainst,2),0) as 'GA_over_SA_agg'
      ,coalesce(round(((TotWins/TotPlayers) 
       / ((TotWins/TotPlayers)  + TotLoses/TotPlayers)),2),0) 
       * 100                                               as avg_percentage_wins

from Aggt
'''),
                            columns = ['tmID'
                              ,'Year'
                              ,'Win_agg'
                              ,'Losses_agg'
                              ,'GP_agg'
                              ,'Mins_over_GA_agg'
                              ,'GA_over_SA_agg'
                              ,'avg_percentage_wins'
                               ])


# In[12]:


Ans_1_to_8


# In[13]:


# save to table
Ans_1_to_8.to_sql('Ans_1_to_8', con, if_exists='replace', index=False)


# In[14]:


# aggregate on Player level
# Goals stopped calculated as Shots against - Goals against
PlayerAGG = pd.DataFrame(cur.execute('''select playerid
                                       ,sum(SA - GA)       as goals_stopped
                                       ,sum(Min)           as Mins_played  
                                        from GoaliesClean
                                        group by playerid
                                        order by 1 '''),
                             columns = ['playerid'
                                       ,'goals_stopped'
                                       ,'Mins_played'])
("")


# In[15]:


PlayerAGG


# In[16]:


# save to table
PlayerAGG.to_sql('PlayerAGG', con, if_exists='replace', index=False)


# In[17]:


MAX_goals_stopped = pd.DataFrame(cur.execute('''select playerid
                                                      , max(goals_stopped)
                                                  from PlayerAGG'''))


# In[18]:


MAX_goals_stopped


# In[19]:


# Unpacking
values = MAX_goals_stopped.values.tolist()
values


# In[20]:


# test
print(values[0][0])


# In[21]:


most_goals_stopped = {'playerID': (values[0][0]), 'goals_stopped': (values[0][1])} 


# In[22]:


print(most_goals_stopped)


# In[23]:


MAX_Efficient = pd.DataFrame(cur.execute('''select playerid
                                          , max(goals_stopped/Mins_played)
                                            from PlayerAGG'''))


# In[24]:


print(MAX_Efficient)


# In[25]:


# Unpack into dictionary
values2 = MAX_Efficient.values.tolist()
values2


# In[26]:


most_efficient_player = {'playerID': (values2[0][0]), 'goals_stopped': (values2[0][1])} 


# In[27]:


most_efficient_player


# In[28]:


con.commit()
con.close()


# # --- Answers ---

# # Questions 1-8
# 

# In[29]:


Ans_1_to_8


# # Questions 9

# In[30]:


most_goals_stopped


# # Questions 10

# In[31]:


most_efficient_player


# In[ ]:




