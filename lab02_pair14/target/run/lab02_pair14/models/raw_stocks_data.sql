
  create or replace   view USER_DB_BOA.analytics.raw_stocks_data
  
   as (
    SELECT * FROM user_db_boa.raw.stocks_data
  );

