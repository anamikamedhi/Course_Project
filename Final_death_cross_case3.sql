
 -- Case 3
 -- find death cross counts over all symbols and save the table as D_count
 --Query_1 start
create table d_count as
with f_g as
(with grp_s as
(with grp_stock as
(select *,
avg(close) over(partition by symbol order by date rows between 49 preceding and current row) as DMA50,
avg(close) over(partition by symbol order by date rows between 199 preceding and current row) as DMA200
 from stocks2)
 select symbol,date,close,DMA50,DMA200,case when DMA50-DMA200>0 then 1 else 0
 end as diff,
 rank() over(partition by symbol order by date) as row_num
 from grp_stock)
 select symbol,close,
 case when diff-lag(diff) over(partition by symbol order by date) = -1 then 1 else 0
 end as dc
 from grp_s)
 select symbol, count(dc) as dc_count
 from f_g
 where dc=1
 group by symbol
 order by dc_count desc;
 
 --Query_1 end
 
 
 -- Finding the percentage of golden cross which resulted in a price decreaseof 1%,2%,...
 -- Creating a table with all date corresponding to the death cross and and dates after one week,two week,
 -- three week, one month, two month, three month, four month, five month and 6th month.
 
 --Query_2 start
CREATE TABLE dc_all_dates as
with f_g as
(with grp_s as
(with grp_stock as
(select *,
avg(close) over(partition by symbol order by date rows between 49 preceding and current row) as DMA50,
avg(close) over(partition by symbol order by date rows between 199 preceding and current row) as DMA200
 from stocks2)
 select symbol,date,close,DMA50,DMA200,case when DMA50-DMA200>0 then 1 else 0
 end as diff,
 rank() over(partition by symbol order by date) as row_num
 from grp_stock)
 select symbol,close,date,
 case when diff-lag(diff) over(partition by symbol order by date) = -1 then 1 else 0
 end as dc
 from grp_s)
 select symbol,close, date, date + integer '7' as week_1,date + integer '14' as week_2,
 date + integer '21' as week_3,date + interval '1 month' as month_1,date + interval '2 month' as month_2,
 date + interval '3 month' as month_3,date + interval '4 month' as month_4,date + interval '5 month' as month_5,
 date + interval '6 month' as month_6
 from f_g
 where dc=1;
 
--Query_2 end

-- CHeck the all_dates table
select * from dc_all_dates;


-- Changing data type of month columns from timestamp tp date
alter table dc_all_dates 
alter column month_1 type DATE,
alter column month_2 type DATE,
alter column month_3 type DATE,
alter column month_4 type DATE,
alter column month_5 type DATE,
alter column month_6 type DATE;

--see the changes

-- Calculating price decrease corresponding to each week and month separately.
-- First_week price decrease for all death cross across all symbols.
-- Compiling them into a single table 
--Query_3 start
create table d_price_change_over_time as
with w_1 as
(with w_11 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as week_status
from all_dates a, stocks2 s 
where a.week_1=s.date and a.symbol=s.symbol
order by percent_increase desc)
select count(symbol) as Company_counts,week_status as week_1 from w_11
group by week_status),

-- 2nd_week price decrease for all death cross across all symbols.
w_2 as
(with w_21 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as week_status
from all_dates a, stocks2 s 
where a.week_2=s.date and a.symbol=s.symbol
order by percent_increase desc)
select count(symbol) as Company_counts,week_status as week_2 from w_21
group by week_status),

-- 3rd_week price decrease for all death cross across all symbols.
w_3 as
(with w_31 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as week_status
from all_dates a, stocks2 s 
where a.week_3=s.date and a.symbol=s.symbol
order by percent_increase desc )
select count(symbol) as Company_counts,week_status as week_3 from w_31
group by week_status),


-- 1st month price decrease for all death cross across all symbols.
mon_1 as
(with m_11 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as month_status
from all_dates a, stocks2 s 
where a.month_1=s.date and a.symbol=s.symbol
order by percent_increase desc )
select count(symbol) as Company_counts,month_status as month_1 from m_11
group by month_status),


-- 2nd month price decrease for all death cross across all symbols.
mon_2 as
(with m_21 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as month_status
from all_dates a, stocks2 s 
where a.month_2=s.date and a.symbol=s.symbol
order by percent_increase desc )
select count(symbol) as Company_counts,month_status as month_2 from m_21
group by month_status),


-- 3rd month price decrease for all death cross across all symbols.
mon_3 as
(with m_31 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as month_status
from all_dates a, stocks2 s 
where a.month_3=s.date and a.symbol=s.symbol
order by percent_increase desc )
select count(symbol) as Company_counts,month_status as month_3 from m_31
group by month_status),


-- 4th month price decrease for all death cross across all symbols.
mon_4 as
(with m_41 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as month_status
from all_dates a, stocks2 s 
where a.month_4=s.date and a.symbol=s.symbol
order by percent_increase desc )
select count(symbol) as Company_counts,month_status as month_4 from m_41
group by month_status),



-- 5th month price decrease for all death cross across all symbols.
mon_5 as
(with m_51 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as month_status
from all_dates a, stocks2 s 
where a.month_5=s.date and a.symbol=s.symbol
order by percent_increase desc )
select count(symbol) as Company_counts,month_status as month_5 from m_51
group by month_status),


-- 6th month price decrease for all death cross across all symbols.
mon_6 as
(with m_61 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0
end as month_status
from all_dates a, stocks2 s 
where a.month_6=s.date and a.symbol=s.symbol
order by percent_increase desc )
select count(symbol) as Company_counts,month_status as month_6 from m_61
group by month_status)

-- Combine all the tables by percent wise column and week wise output
select a.week_1 as percent_increase,a.Company_counts as week_1,b.Company_counts as week_2,c.Company_counts as week_3,
d.Company_counts as month_1,e.Company_counts as month_2,f.Company_counts as month_3,g.Company_counts as month_4,
h.Company_counts as month_5,i.Company_counts as month_6
from w_1 a,w_2 b,w_3 c,mon_1 d,mon_2 e,mon_3 f,mon_4 g,mon_5 h,mon_6 i
where 
a.week_1=b.week_2 and 
a.week_1=c.week_3 and 
a.week_1=d.month_1 and 
a.week_1=e.month_2 and
a.week_1=f.month_3 and
a.week_1=g.month_4 and
a.week_1=h.month_5 and
a.week_1=i.month_6 ;

--Query_3 end


 ----  Calculate the  percentage of death cross result in a % price decrease for 1st week
 
 --Query_4 start
create table dc_week_1_price_change as 
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks s
where a.week_1=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;

 --Query_4 end
 
 
 ----  Calculate the  percentage of death cross result in a % price decrease for 2nd week
 --Query_5 start
 create table dc_week_2_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.week_2=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;

  --Query_5 end

 ----  Calculate the  percentage of death cross result in a % price decrease for 3rd week
 --Query_6 start
 create table dc_week_3_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.week_3=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;

--Query_6 end

 ----  Calculate the  percentage of death cross result in a % price decrease for 1st month
--Query_7 start 
create table dc_month_1_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.month_1=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;

--Query_7 end

 
 ----  Calculate the  percentage of death cross result in a % price decrease for 2nd month
--Query_8 start
create table dc_month_2_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.month_2=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;
--Query_8 end

----  Calculate the  percentage of death cross result in a % price decrease for 3rd month
 
 --Query_9 start
create table dc_month_3_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.month_3=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;
 --Query_9 end

----  Calculate the  percentage of death cross result in a % price decrease for 4th month
--Query_10 start 
create table dc_month_4_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.month_4=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;
--Query_10 end


----  Calculate the  percentage of death cross result in a % price decrease for 5th month
 --Query_11 start
create table dc_month_5_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.month_5=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;
 --Query_11 end


----  Calculate the  percentage of death cross result in a % price decrease for 6th month
 --Query_12 start
create table dc_month_6_price_change as
with test_2 as
(with test_1 as
(select a.symbol,((s.close-a.close)/a.close)*100 as percent_increase,
case 
when ((s.close-a.close)/a.close)*100 >1.0 and ((s.close-a.close)/a.close)*100 <2  then 1
when ((s.close-a.close)/a.close)*100 >2.0 and ((s.close-a.close)/a.close)*100 <3  then 2
when ((s.close-a.close)/a.close)*100 >3.0 and ((s.close-a.close)/a.close)*100 <4  then 3
when ((s.close-a.close)/a.close)*100 >4.0 and ((s.close-a.close)/a.close)*100 <5  then 4
when ((s.close-a.close)/a.close)*100 >5.0 and ((s.close-a.close)/a.close)*100 <10  then 5
when ((s.close-a.close)/a.close)*100 >10.0 and ((s.close-a.close)/a.close)*100 <15  then 10
when ((s.close-a.close)/a.close)*100 >15.0 and ((s.close-a.close)/a.close)*100 <20  then 15
when ((s.close-a.close)/a.close)*100 >20.0 and ((s.close-a.close)/a.close)*100 <25 then 20
when ((s.close-a.close)/a.close)*100 >25.0  then 25
else 0 end as percent_status
from all_dates a, stocks2 s
where a.month_6=s.date and a.symbol=s.symbol)
select distinct(symbol), 
 count(percent_status) filter( where percent_status=1) over(partition by symbol) as "1%",
 count(percent_status) filter( where percent_status=2) over(partition by symbol) as "2%",
 count(percent_status) filter( where percent_status=3) over(partition by symbol) as "3%",
 count(percent_status) filter( where percent_status=4) over(partition by symbol) as "4%",
 count(percent_status) filter( where percent_status=5) over(partition by symbol) as "5%",
 count(percent_status) filter( where percent_status=10) over(partition by symbol) as "10%",
 count(percent_status) filter( where percent_status=15) over(partition by symbol) as "15%",
 count(percent_status) filter( where percent_status=20) over(partition by symbol) as "20%",
 count(percent_status) filter( where percent_status=25) over(partition by symbol) as "25%"
from test_1  )
select t.symbol, 
CAST((CAST(t."1%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_1%",
CAST((CAST(t."2%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_2%",
CAST((CAST(t."3%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_3%",
CAST((CAST(t."4%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_4%",
CAST((CAST(t."5%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_5%",
CAST((CAST(t."10%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_10%",
CAST((CAST(t."15%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_15%",
CAST((CAST(t."20%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_20%",
CAST((CAST(t."25%" as DECIMAL(5,2))/g.gc_count )*100 as DECIMAL(5,2))  as "gc_above_25%"
from test_2 t, d_count g
where t.symbol=g.symbol
order by symbol;
--Query_12 end


