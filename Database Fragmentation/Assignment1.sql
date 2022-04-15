
/*
Creating the table and loading the dataset
*/
DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (userid INT, temp1 VARCHAR(10),  movieid INT , temp3 VARCHAR(10),  rating REAL, temp5 VARCHAR(10), timestamp INT);
COPY ratings FROM 'test_data1.txt' DELIMITER ':';
ALTER TABLE ratings DROP COLUMN temp1, DROP COLUMN temp3, DROP COLUMN temp5, DROP COLUMN timestamp;

-- Do not change the above code except the path to the dataset.
-- make sure to change the path back to default provided path before you submit it.

-- Part A
/* Write the queries for Part A*/



-- Part B
/* Create the fragmentations for Part B1 */


/* Write reconstruction query/queries for Part B1 */


/* Write your explanation as a comment */


/* Create the fragmentations for Part B2 */


/* Write your explanation as a comment */


/* Create the fragmentations for Part B3 */


/* Write reconstruction query/queries for Part B3 */


/* Write your explanation as a comment */



-- Part C
/* Write the queries for Part C */

