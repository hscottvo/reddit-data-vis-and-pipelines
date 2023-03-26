select 
  name 
where 
  date > current_date - interval '7 days' 
limit 100;