				
					-- DATA CLEANING
-- Remove Duplicates
-- Standardize Data (Null Values/Blank Values
-- Remove Unwanted Columns and Rows
                    
                   -- Remove Duplicates
SELECT *
FROM layoffs;

-- * Create a staging table (it is best practice to work with a staging data)
CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- *insert data into from raw data into staging table
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- *Remove the duplicate
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- *Create a CTE
WITH duplicate_cte AS
(
SELECT *
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- *Create another staging
CREATE TABLE `layoffs_staging2` (                  
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
SELECT *
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
  
SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

						-- STANDARDIZING DATA
-- *Country Column 

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- *Industry Column
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- *Country Column
SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Date Column (Changing the date format from text)
SELECT `date`
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- *Delete Unwanted Column and rows

-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;

  




