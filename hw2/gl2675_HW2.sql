DROP TABLE IF EXISTS JOHNS;
DROP VIEW IF EXISTS AverageHeightWeight, AverageHeight;

/*QUESTION 0
EXAMPLE QUESTION
What is the highest salary in baseball history?
*/

/*SAMPLE ANSWER*/
SELECT MAX(salary) AS Max_Salary
FROM Salaries;

/*QUESTION 1
Select the first name, last name, and given name of players who are taller than 6 ft
[hint]: Use "People"
*/
SELECT nameFirst, nameLast, nameGiven
  FROM People
 WHERE height > 72;

/*QUESTION 2
Create a Table of all the distinct players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
Add a column called nameFull that is a concatenated version of first and last
[hint] Use a Join between People and CollegePlaying
*/
CREATE TABLE JOHNS
AS
    SELECT DISTINCT nameFirst, nameLast, p.playerID, birthState, CONCAT(nameFirst, " ", nameLast) AS nameFull
      FROM People AS p
      JOIN CollegePlaying AS c
        ON p.playerID = c.playerID
     WHERE namefirst = "John"
       AND birthCountry = "USA"
       AND schoolID = "fordham";

/*QUESTION 3
Delete all Johns from the above table whose total career runs batted in is less than 2
[hint] use a subquery to select these johns from people by playerid
[hint] you may have to set sql_safe_updates = 1 to delete without a key
*/
SET sql_safe_updates = 0;
DELETE FROM JOHNS
 WHERE JOHNS.playerID IN
       (SELECT playerID 
          FROM (SELECT p.playerID, SUM(RBI) AS totalRBI
                  FROM People AS p 
                  JOIN Batting AS b
					ON p.playerID = b.playerID
				 WHERE nameFirst = "John"
				 GROUP BY p.playerID
			    HAVING totalRBI < 2) AS tmp);
SET sql_safe_updates = 1;

/*QUESTION 4
Group together players with the same birth year, and report the year, 
 the number of players in the year, and average height for the year
 Order the resulting by year in descending order. Put this in a view
 [hint] height will be NULL for some of these years
*/
CREATE VIEW AverageHeight(birthYear, playerCount, avgHeight)
AS
    SELECT birthYear, COUNT(playerID), AVG(height)
      FROM People
     GROUP BY birthYear
     ORDER BY birthYear DESC;

/*QUESTION 5
Using Question 3, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
CREATE VIEW AverageHeightWeight(birthYear, playerCount, avgHeight, avgWeight)
AS
    SELECT h.birthYear, h.playerCount, h.avgHeight, w.avgWeight
      FROM (SELECT birthYear, AVG(weight) AS avgWeight
              FROM People
             GROUP BY birthYear) AS w
      JOIN AverageHeight AS h
        ON w.birthYear = h.birthYear
     WHERE avgWeight > 180
  ORDER BY birthYear ASC;

/*QUESTION 6
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Order the players by School alphabetically.
Update all entries with full name Columbia University to 'Columbia University!' in the schools table
*/
SELECT p.playerID, nameFirst, nameLast, c.schoolID
  FROM People AS p
  JOIN Halloffame AS h
    ON p.playerID = h.playerID
  JOIN Collegeplaying AS c
    ON p.playerID = c.playerID
  JOIN Schools AS s
    ON c.schoolID = s.schoolID
 WHERE s.state = "NY"
 ORDER BY playerID ASC;

SET sql_safe_updates = 0;
UPDATE Schools
   SET name_full = "Columbia University!"
 WHERE name_full = "Columbia University";
SET sql_safe_updates = 1;

/*QUESTION 7
Find the team id, yearid and average HBP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values
[hint] be careful to only include entries where AB is > 0
*/
SELECT teamID, yearID, AVG(HBP) AS avgHBP
  FROM (SELECT teamID, yearID, HBP
          FROM Batting
         WHERE AB > 0) AS tmp
 GROUP BY teamID, yearID
 ORDER BY avgHBP DESC
 LIMIT 100;
