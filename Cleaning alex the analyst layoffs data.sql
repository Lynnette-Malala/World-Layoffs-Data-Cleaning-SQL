Use world_layoffs;

select *
from `layoffs (1)`;

rename table `layoffs (1)` to layoffs;

create table layoffs_2
like layoffs;

insert into layoffs_2
select * 
from layoffs;

select * 
from layoffs_2;

select *,
row_number () over(partition by 
	company, industry, total_laid_off, 
	percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_2;

with duplicates_cte as 
(
select *,
row_number () over(partition by 
	company, industry, total_laid_off, 
	percentage_laid_off, `date`) as row_num
from layoffs_2
)
select * 
from duplicates_cte
where row_num > 1;

create table layoffs_2_staging 
like layoffs_2;

alter table layoffs_2_staging
add column row_num int;

insert into layoffs_2_staging
select *,
row_number () over( 
	partition by company, location, industry, total_laid_off, percentage_laid_off, 
    `date`, stage, country, funds_raised_millions) as row_num
	from layoffs_2 ;

select * 
from layoffs_2_staging;

set sql_safe_updates = 0;
delete from layoffs_2_staging
where row_num > 1;
set sql_safe_updates = 1;

select *
from layoffs_2_staging;

select company, trim(company)
from layoffs_2_staging;


set sql_safe_updates = 0;
update layoffs_2_staging
SET industry = trim(industry),
	country = trim(country),
    `date` = trim(`date`);
set sql_safe_updates = 1;


select distinct industry
from layoffs_2_staging
order by 1;

select *
from layoffs_2_staging
where industry
like 'crypto%';

set sql_safe_updates = 0;
update layoffs_2_staging
set industry = 'crypto'
where industry 
like 'crypto%';
set sql_safe_updates = 1;

Select distinct industry 
from layoffs_2_staging;


select distinct industry
from layoffs_2_staging
order by 1;

select distinct country
from layoffs_2_staging
order by 1;

select distinct country
from layoffs_2_staging
where country like 'United States%';

set sql_safe_updates = 0;
update layoffs_2_staging
set country = trim(trailing '.' from country)
where country 
like 'United States%';
set sql_safe_updates = 1;

select distinct country
from layoffs_2_staging;

select * 
from layoffs_2_staging;

select distinct `date`
from layoffs_2_staging;

select `date`,
str_to_date(`date`, '%m/%d/%Y') 
from layoffs_2_staging;

set sql_safe_updates = 0;

update layoffs_2_staging
set `date` = str_to_date(`date`, '%m/%d/%Y')
where `date` like '%/%';

set sql_safe_updates = 1;

select `date` from layoffs_2_staging limit 5;

alter table layoffs_2_staging
modify column `date` date;

select * 
from layoffs_2_staging;

set sql_safe_updates = 0;

select *
from layoffs_2_staging
where total_laid_off is null
and percentage_laid_off is null;


set sql_safe_updates = 0;

update layoffs_2_staging 
set industry = null
where industry = ''; 

set sql_safe_updates = 1;

select *
from layoffs_2_staging
where industry is null
or industry = '';


select *
from layoffs_2_staging as t1
join layoffs_2_staging as t2
	 on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

set sql_safe_updates = 0;

update layoffs_2_staging as t1
join layoffs_2_staging as t2
	 on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null; 

set sql_safe_updates = 1;

select *
from layoffs_2_staging
where industry = '';

select *
from layoffs_2_staging
where company = 'Airbnb';

set sql_safe_updates = 0;

delete 
from layoffs_2_staging 
where total_laid_off is null
and percentage_laid_off is null;

set sql_safe_updates = 1; 

select *
from layoffs_2_staging;


alter table layoffs_2_staging
drop column row_num;







































