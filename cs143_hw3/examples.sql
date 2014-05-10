
SELECT 
	statecode
FROM 
	states 
WHERE 
	population_2010 > 10000000;

	
SELECT 
	statecode 
FROM 
	states 
WHERE 
	population_2010 > 20000000;


SELECT 
	statecode, max(population_2010) 
FROM 
	states;


SELECT 
	statecode, count(*) 
FROM 
	counties 
GROUP BY 
	statecode;

SELECT 
	statecode, max(sc.state_count) 
FROM 
	(SELECT 
		statecode, count(*) AS state_count 
	 FROM 
	 	counties 
	 	GROUP BY statecode) AS sc;

SELECT 
	affiliation, count(*) 
FROM 
	senators 
GROUP BY 
	affiliation;


SELECT 
	ranking_member, count(*) AS count 
FROM 
	committees 
GROUP BY 
	ranking_member;


SELECT 
	ranking_member, max(rc.rm_count) 
FROM 
	(SELECT 
		ranking_member, count(*) AS rm_count 
	 FROM 
	 	committees 
	 GROUP BY 
	 	ranking_member) AS rc;

