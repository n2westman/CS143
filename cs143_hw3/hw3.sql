/* CS 143 Spring 2014, Homework 3 - Federal Government Shutdown Edition */

/*******************************************************************************
 For each of the queries below, put your SQL in the place indicated by the
 comment.  Be sure to have all the requested columns in your answer, in the
 order they are listed in the question - and be sure to sort things where the
 question requires them to be sorted, and eliminate duplicates where the
 question requires that.  We will grade the assignment by running the queries on
 a test database and eyeballing the SQL queries where necessary.  We won't grade
 on SQL style, but we also won't give partial credit for any individual question
 - so you should be confident that your query works. In particular, your output
 should match our example output in hw3trace.txt
********************************************************************************/

/*******************************************************************************
 Q1 - Return the statecode, county name and 2010 population of all counties who
 had a population of over 2,000,000 in 2010. Return the rows in descending order
 from most populated to least
 ******************************************************************************/

SELECT statecode, name, population_2010
FROM counties
WHERE population_2010 > 2000000
ORDER BY population_2010 DESC;

/*******************************************************************************
 Q2 - Return a list of statecodes and the number of counties in that state,
 ordered from the least number of counties to the most 
*******************************************************************************/

SELECT statecode, COUNT(*) as numCounties
FROM counties C
GROUP BY statecode
ORDER BY numCounties DESC; --Sanity check, there are 50 rows returned

/*******************************************************************************
 Q3 - On average how many counties are there per state (return a single real
 number) 
*******************************************************************************/

SELECT -- woooo
	AVG(sc.county_count)
FROM (
	SELECT COUNT(*) as county_count
	FROM counties C 
	GROUP BY C.statecode ) as sc;

/*******************************************************************************
 Q4 - return a count of how many states have more than the average number of
 counties
*******************************************************************************/

SELECT COUNT(*)
FROM (
	SELECT COUNT(*) as county_count
	FROM counties C 
	GROUP BY C.statecode 
	HAVING county_count > ( 
		SELECT AVG(sc.county_count)
		FROM (
			SELECT COUNT(*) as county_count
			FROM counties C 
			GROUP BY C.statecode ) as sc) ) test;


/*******************************************************************************
 Q5 - Data Cleaning - return the statecodes of states whose 2010 population does
 not equal the sum of the 2010 populations of their counties
*******************************************************************************/

SELECT S.statecode, S.population_2010
FROM states S
WHERE S.population_2010 <> (	SELECT SUM(C.population_2010)
						FROM counties C
						GROUP BY C.statecode
						HAVING C.statecode = S.statecode);


/*******************************************************************************
 Q6 - How many states have at least one senator whose first name is John,
 Johnny, or Jon? Return a single integer
*******************************************************************************/

SELECT COUNT(DISTINCT S.statecode)
FROM senators S
WHERE S.name LIKE 'John %' OR S.name LIKE 'Johnny %' OR S.name LIKE 'Jon %';


/*******************************************************************************
Q7 - Find all the senators who were born in a year before the year their state
was admitted to the union.  For each, output the statecode, year the state was
admitted to the union, senator name, and year the senator was born.  Note: in
SQLite you can extract the year as an integer using the following:
"cast(strftime('%Y',admitted_to_union) as integer)"
*******************************************************************************/

SELECT St.statecode, St.admitted_to_union, Se.name, Se.born
FROM states St, senators Se
WHERE St.statecode = Se.statecode AND YEAR(St.admitted_to_union) > Se.born;

/*******************************************************************************
Q8 - Find all the counties of West Virginia (statecode WV) whose population
shrunk between 1950 and 2010, and for each, return the name of the county and
the number of people who left during that time (as a positive number).
*******************************************************************************/

SELECT C.name, (C.population_1950 - C.population_2010) AS Fewer_People
FROM counties C
WHERE C.statecode LIKE 'WV' and C.population_1950 > C.population_2010;


/*******************************************************************************
Q9 - Return the statecode of the state(s) that is (are) home to the most
committee chairmen
*******************************************************************************/

SELECT Se.statecode, count(*), Co.chairman, Co.name
FROM (senators Se JOIN committees Co ON Se.name = Co.chairman)
GROUP BY Se.statecode
HAVING count(*) = ( SELECT MAX(num_sen)
				FROM (	SELECT COUNT(*) as num_sen
						FROM senators Se, committees Co
						WHERE Se.name = Co.chairman
						GROUP BY Se.statecode ) CCCP);

/*******************************************************************************
Q10 - Return the statecode of the state(s) that are not the home of any
committee chairmen
*******************************************************************************/

SELECT S.statecode
FROM states S
WHERE S.statecode NOT IN (    SELECT Se.statecode
						FROM senators Se, committees Co
						WHERE Se.name = Co.chairman
						GROUP BY Se.statecode);

/*******************************************************************************
Q11 Find all subcommittes whose chairman is the same as the chairman of its
parent committee.  For each, return the id of the parent committee, the name of
the parent committee's chairman, the id of the subcommittee, and name of that
subcommittee's chairman
*******************************************************************************/

SELECT parent.id, parent.chairman, child.id, child.chairman
FROM committees child, committees parent
WHERE child.parent_committee = parent.id AND child.chairman = parent.chairman;


/*******************************************************************************
Q12 - For each subcommittee where the subcommittee’s chairman was born in an
earlier year than the chairman of its parent committee, Return the id of the
parent committee, its chairman, the year the chairman was born, the id of the
submcommittee, it’s chairman and the year the subcommittee chairman was born.
********************************************************************************/

SELECT parentc.id, parentc.chairman, parents.born, childc.id, childc.chairman, childs.born
FROM committees childc JOIN senators childs ON childc.chairman=childs.name, committees parentc JOIN senators parents ON parentc.chairman=parents.name
WHERE childc.parent_committee = parentc.id AND childs.born < parents.born;


