-- DATA CLEANING 

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT  layoffs_staging
SELECT  * 
FROM layoffs;

-- REMOVING DUPLICATES 

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;



CREATE TABLE  layoffs_staging2 (
company TEXT,
location TEXT,
industry TEXT, 
total_laid_off TEXT,
percentage_laid_off TEXT,
`date` TEXT,
stage TEXT,
country TEXT,
funds_raised_millions INT DEFAULT NULL,
row_num INT
);

INSERT INTO layoffs_staging2 
(company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num )
SELECT
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, `date`, stage, country, total_laid_off, percentage_laid_off, funds_raised_millions
ORDER BY company) as row_num
FROM lay_offs_staging;


DELETE
FROM layoffs_staging2
WHERE  row_num > 1;

SELECT * FROM layoffs_staging2;


-- STANDRAZING DATA 
   
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE INDUSTRY LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT DISTINCT (location)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT (country)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT TRIM( TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM( TRAILING '.' FROM country)
WHERE country LIKE 'unitedstates%';

SELECT `date`,
STR_TO_DATE (`date` , '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date` , '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- WORKING WITH NULL AND BLANK VALUES 

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL AND 
percentage_laid_off IS NULL;

-- HOW TO POPULATE DATA 

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR
industry =  '';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry ='';


SELECT *
FROM layoffs_staging2
WHERE company ='Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company =t2.company 
AND t1.location = t2.location
WHERE t1.industry IS NULL AND 
t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company =t2.company 
AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND 
t2.industry IS NOT NULL;


--  REMOVING COLUMNS AND ROWS THAT WE NEED TO 

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;


SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- EXPLORATORY DATA ANALYSIS 

SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off) as total_laidoff
FROM layoffs_staging2;

--  CONVERT VARCHAR INTO INT

SELECT MAX(CAST(total_laid_off AS UNSIGNED )) AS total_laidoff, MAX(percentage_laid_off) AS max_percentage
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY total_laid_off INT;

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


-- INDIVIDUAL DATE LAID_OFF

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

-- YEAR 

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


-- ROLLING TOTAL_LAIDOFF PER MONTH

SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE  SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 asc;


with rolling_total as
(
 SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) as total_off
FROM layoffs_staging2
WHERE  SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 asc
)
SELECT `month`, total_off,
 SUM(total_off) OVER(ORDER BY `month`) AS rolling_total
FROM rolling_total;


SELECT company, sum(total_laid_off)
FROM layoffs_staging
group by company
ORDER BY 2 DESC;


SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`DATE`)
ORDER BY 3 DESC;


WITH company_year ( company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`DATE`)
), company_year_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT * 
FROM company_year_rank 
where ranking <= 5;



 



















































































































