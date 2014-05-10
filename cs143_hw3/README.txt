CS143 SQL Assignment. Due: May 14, 11:59pm.

In this assignment, you will complete the 12 sql queries described in
hw3.sql. Here is some background information to get you started with
MySQL for Ubuntu. You should be able to easily find
online resources to install mysql for windows or OS X. If there is
trouble with those platforms, perhaps students can help each other on Piazza.

We use $ to denote the command prompt in a linux terminal. Do not type
the $.

-----------------------
1. Install MySQL Server
-----------------------

From a terminal, type:

   $ sudo apt-get install mysql-server

You will be prompted to enter a password for the root user, DO NOT
FORGET THIS PASSWORD!

Detailed Instructions here: https://help.ubuntu.com/13.10/serverguide/mysql.html

--------------------------------
2. Install Squirrel MySQL client
--------------------------------

	Installation Instructions:  http://keithxm23.blogspot.com/2012/07/set-up-squirrelsql-for-ubuntu.html
	NOTE: The latest version can be downloaded from here: http://www.squirrelsql.org/#installation

	Tutorial here: http://www.squirrelsql.org/kulvir/tutorial.html

	(Squirrel is a java program so should work with both windows and os
	x as well.)

  You can also use the command line mysql client:

  $ mysql -uroot -p
  [mysql will prompt you to enter your password]
	instructions: http://dev.mysql.com/doc/refman/5.6/en/mysql.html

	There are many other mysql clients including mysql-workbench which
	is the official client maintained by the same people who maintain
	MySQL. It is much more complicated and buggy than squirrel.


------------------
3. Getting started
------------------

After you have installed MySQL and Squirrel, you can manipulate your
database from within squirrel.

- Click on Aliases and double click the alias you created in step 2.
- Press Connect on the box that pops up.
- There are two tabs: Objecs and SQL. Click on SQL.
- Press the open folder icon (7th from left)
- Open the populate.sql file in this directory.
- Press ctrl-a to select the entire file.
- Press ctrl-enter to execute the entire file.
- You should now have a populated database!

Now lets try running the examples.sql. 

- Press the open folder icon (7th from left)
- Open the examples.sql file in this directory.
- On the top left where it says "Catalog:", select the sqlhw database
  you just created from the dropdown.
- Put your cursor somewhere on the first line:
  "select statecode from states where population_2010 > 10000000;"
- Now press ctrl-enter to execute the query.
- Try executing all of the queries.

Now you are ready to work on the assignment! Open hq3.sql and begin.
