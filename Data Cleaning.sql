--- DATA CLEANING

select *
from layoffs;

-- steps 
-- 1. remove duplicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns



create table layoffs_staging
like layoffs;


select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- removing duplicates
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) as row_num
from layoffs_staging
)
select*
from duplicate_cte
where row_num > 1;


with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num`int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;


-- standardizing data

select distinct (company)
from layoffs_staging2;

select distinct (industry)
from layoffs_staging2
order by 1;

select distinct (location)
from layoffs_staging2
order by 1;

select distinct (country)
from layoffs_staging2
order by 1;

select date
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

-- removing null and blank values
select *
from layoffs_staging2
where total_laid_off = ''
and percentage_laid_off = '';

select *
from layoffs_staging2
where industry = '';

select *
from layoffs_staging2
where company = 'Appsmith';

delete 
from layoffs_staging2 where industry = '';

delete 
from layoffs_staging2
where total_laid_off = ''
and percentage_laid_off = '';

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;