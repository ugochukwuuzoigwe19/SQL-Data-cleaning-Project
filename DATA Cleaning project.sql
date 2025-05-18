				
					#DATA CLEANING / DATA ANALYSIS
                    
                   #STEP 1 - CREATE A STAGING TABLE
SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging     #create staging table for the raw data
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging   #insert data into from raw data into staging table
SELECT *
FROM layoffs;

					#STEP 2 - REMOVE DUPLICATE

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


CREATE TABLE `layoffs_staging2` (                  #new table created 
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  
SELECT *
FROM layoffs_staging2;
  
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
  
SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

						#STEP 3 - STANDARDIZING DATA

#country column hass some issues

#SELECT company, TRIM(company)
#FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;          #There's a period(.) at the end of one of the UNITED STATES, we need to fix that.

#SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
#FROM layoffs_staging2
#ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`
FROM layoffs_staging2;     #We need change date to the date format and modify the column to change the data trype to DATE

#SELECT `date`,
#STR_TO_DATE(`date`, '%m/%d/%Y')
#FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

#DELETE THE ROW_NUM COLUMN

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


  




