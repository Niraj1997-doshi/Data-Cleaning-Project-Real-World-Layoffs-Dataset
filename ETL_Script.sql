-- Data Cleaning

USE world_layoffs; 
SELECT * 
FROM layoffs;

 -- 1. Remove Duplicate
 -- 2. Standardize the Date
 -- 3. Null Values or Blank Values
 -- 4. Remove Any Columns
 
 CREATE TABLE layoffs_staging
 LIKE layoffs;
 
SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. Remove Duplicates

# Check for duplicates

SELECT *
FROM layoffs_staging;

SELECT company, industry, total_laid_off, 'date',
           ROW_NUMBER() OVER(
           PARTITION BY company, industry, total_laid_off, 'date' ) AS row_num
           FROM layoffs_staging;
           
SELECT *
FROM ( 
      SELECT company, industry, total_laid_off, 'date',
           ROW_NUMBER() OVER(
           PARTITION BY company, industry, total_laid_off, 'date' ) AS row_num
           FROM layoffs_staging) duplicates
WHERE row_num > 1;

# test the results

SELECT *
FROM layoffs_staging
WHERE company ='oda';

# in oda we have found that all the entries are legitimate so improvision the CTE

SELECT *
FROM ( 
       SELECT company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country, funds_raised,
              ROW_NUMBER() OVER(
                       PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country, funds_raised) AS row_num
		FROM layoffs_staging
        
	) duplicates
WHERE row_num >1;

# here we have created one additional columnm to identify the duplicates
# Now performing same activity by using CTE

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country, funds_raised) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

# test the results

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

# to remove the duplicate we can create new data table and then delete the the data where row numbers are over 2, then that column

CREATE TABLE `layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised` INT,
    `row_num` INT
);

INSERT INTO `layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised
			) AS row_num
	FROM 
		layoffs_staging;
        
SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

# here this query is giveing me error cause its in the safe mode, to disable temporary one can use below code

SET SQL_SAFE_UPDATES = 0;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 1; -- Re-enable safe updates

-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

# here industry is distinct

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
 
 ### here data coulmn is in text we need to fix this for exploratory analysis or time series analysis ###
 
 SELECT `date`,
 STR_To_DATE(`date`,'%m/%d/Y')
 FROM layoffs_staging2;
 
### Null removal ops ###

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

### Removing the missing values ###

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

## Checking the Nulls or empty value from the data set ##
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- writing a query that if there is another row with the same company name, it will update it to the non-null industry values

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Cross check the results

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
ORDER BY industry;

-- Now we need to populate those nulls 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT*
FROM layoffs_staging2
WHERE industry IS NULL
AND industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- fixing the date column

## UPDATE layoffs_staging2
## SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3 Removing Null values
-- 4 remove anu coulmn and row

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- delete useless data

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;




















 
 















        
           







 
 