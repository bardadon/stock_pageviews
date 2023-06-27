select 
  r.company,
  r.rate,
  p.views,
  r.date
from
(
  select 
    company, 
    case
      when company = 'Amazon' then 'AMZN'
      when company = 'Microsoft' then 'MSFT'
      when company = 'Apple' then 'AAPL'
      when company = 'Google' then 'GOOGL'
      when company = 'Facebook' then 'META'
    end as quote,
    views,
    date
  from 
    `pageviews-390416.pageviews.pageviews`
) as p
join 
  `pageviews-390416.pageviews.rates` as r on
  p.quote = r.company and
  p.date = r.date
order by 
  date desc
