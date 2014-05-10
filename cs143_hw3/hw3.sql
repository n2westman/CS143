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

/* Put your SQL for Q1 here */

/*******************************************************************************
 Q2 - Return a list of statecodes and the number of counties in that state,
 ordered from the least number of counties to the most 
*******************************************************************************/

/* Put your SQL for Q2 here */

/*******************************************************************************
 Q3 - On average how many counties are there per state (return a single real
 number) 
*******************************************************************************/

/* Put your SQL for Q3 here */

/*******************************************************************************
 Q4 - return a count of how many states have more than the average number of
 counties
*******************************************************************************/

/* Put your SQL for Q4 here */

/*******************************************************************************
 Q5 - Data Cleaning - return the statecodes of states whose 2010 population does
 not equal the sum of the 2010 populations of their counties
*******************************************************************************/

/* Put your SQL for Q5 here */

/*******************************************************************************
 Q6 - How many states have at least one senator whose first name is John,
 Johnny, or Jon? Return a single integer
*******************************************************************************/

/* Put your SQL for Q6 here */

/*******************************************************************************
Q7 - Find all the senators who were born in a year before the year their state
was admitted to the union.  For each, output the statecode, year the state was
admitted to the union, senator name, and year the senator was born.  Note: in
SQLite you can extract the year as an integer using the following:
"cast(strftime('%Y',admitted_to_union) as integer)"
*******************************************************************************/


/*******************************************************************************
Q8 - Find all the counties of West Virginia (statecode WV) whose population
shrunk between 1950 and 2010, and for each, return the name of the county and
the number of people who left during that time (as a positive number).
*******************************************************************************/

/* Put your SQL for Q8 here */

/*******************************************************************************
Q9 - Return the statecode of the state(s) that is (are) home to the most
committee chairmen
*******************************************************************************/

/* Put your SQL for Q9 here */

/*******************************************************************************
Q10 - Return the statecode of the state(s) that are not the home of any
committee chairmen
*******************************************************************************/

/* Put your SQL for Q10 here */

/*******************************************************************************
Q11 Find all subcommittes whose chairman is the same as the chairman of its
parent committee.  For each, return the id of the parent committee, the name of
the parent committee's chairman, the id of the subcommittee, and name of that
subcommittee's chairman
*******************************************************************************/

/*Put your SQL for Q11 here */

/*******************************************************************************
Q12 - For each subcommittee where the subcommittee’s chairman was born in an
earlier year than the chairman of its parent committee, Return the id of the
parent committee, its chairman, the year the chairman was born, the id of the
submcommittee, it’s chairman and the year the subcommittee chairman was born.
********************************************************************************/

/* Put your SQL for Q12 here */
