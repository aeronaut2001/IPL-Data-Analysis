create database IPL_PROJECT_SQL

use IPL_PROJECT_SQL


select * from Dim_Country
select * from Dim_Team
select * from Dim_Batting_Style
select * from Dim_Bowling_Style
select * from Dim_Extra_Type
select * from Dim_Role
select * from Dim_Toss_Decision
select * from Dim_Win_Type
select * from Dim_Outcome
select * from fuzzy_player_data
select * from Dim_Out_Type
select * from Dim_city
select * from Dim_Venue
select * from Dim_batsman_scored
select * from Dim_Player
Select * from Dim_player_match
select * from Dim_Season
Select * from Dim_wicket_taken
select * from Dim_extra_run
Select * from Dim_ball_by_ball
Select * from Dim_match
select * from fuzzy_player_data
select * from Error_Handling
select * from [dbo].[sysssislog]
select * from Fact_Match_Details
Select * from Dim_match
select * from Dim_Team


select t.team_name,m.Match_Date,m.Win_Margin
from Dim_match m 
join Dim_Team t
on m.Match_Winner = t.Team_Id
where m.Season_Id = 9


select distinct t.team_name,m.Match_Date,m.Win_Margin
from Dim_match m 
join Fact_Match_Details f
on m.Match_Winner = f.Team_Id
join Dim_Team t
on f.Team_Id = t.Team_Id
where m.Season_Id = 9
order by Match_Date



TRUNCATE TABLE Dim_ball_by_ball;
TRUNCATE TABLE Dim_match;
TRUNCATE TABLE Dim_wicket_taken;
TRUNCATE TABLE Dim_player_match;
TRUNCATE TABLE Dim_extra_run;
TRUNCATE TABLE Dim_batsman_scored;
TRUNCATE TABLE Dim_city;
TRUNCATE TABLE Dim_Country;
TRUNCATE TABLE Dim_Venue;
TRUNCATE TABLE Dim_Team;
TRUNCATE TABLE Dim_Batting_Style;
TRUNCATE TABLE Dim_Bowling_Style;
TRUNCATE TABLE Dim_Extra_Type;
TRUNCATE TABLE Dim_Season;
TRUNCATE TABLE Dim_Role;
TRUNCATE TABLE Dim_Toss_Decision;
TRUNCATE TABLE Dim_Win_Type;
TRUNCATE TABLE Dim_Out_Type;
TRUNCATE TABLE Dim_Outcome;
TRUNCATE TABLE Dim_Player;




-- 1. Dim_Team: 
CREATE TABLE Dim_Team (
    Team_Id INT PRIMARY KEY,
    Team_Name VARCHAR(100)
);

-- 2. Dim_Country:
CREATE TABLE Dim_Country (
    Country_Id INT PRIMARY KEY,
    Country_Name VARCHAR(100)
);

-- 3. Dim_City:
CREATE TABLE Dim_City (
    City_Id INT PRIMARY KEY,
    City_Name VARCHAR(100),
    Country_Id INT,
    FOREIGN KEY (Country_Id) REFERENCES Dim_Country(Country_Id)
);

-- 4.  Dim_Batting_Style:
CREATE TABLE Dim_Batting_Style (
    Batting_Hand_Id INT PRIMARY KEY,
    Batting_Hand VARCHAR(100)
);

-- 5. Dim_Bowling_Style:
CREATE TABLE Dim_Bowling_Style (
    Bowling_Skill_Id INT PRIMARY KEY,
    Bowling_Skill VARCHAR(100)
);

-- 6. Dim_Extra_Type:
CREATE TABLE Dim_Extra_Type (
    Extra_Type_Id INT PRIMARY KEY,
    Extra_Name VARCHAR(100)
);

-- 7. Dim_Role:
CREATE TABLE Dim_Role (
    Role_Id INT PRIMARY KEY,
    Role_Desc VARCHAR(100)
);

-- 8. Dim_Player:
CREATE TABLE Dim_Player (
    Player_Id INT PRIMARY KEY,
    Player_Name VARCHAR(200),
    DOB DATE,
    Batting_Hand_Id INT,
    Bowling_Skill_Id INT,
    Country_Id INT,
    FOREIGN KEY (Batting_Hand_Id) REFERENCES Dim_Batting_Style(Batting_Hand_Id),
    FOREIGN KEY (Bowling_Skill_Id) REFERENCES Dim_Bowling_Style(Bowling_Skill_Id),
    FOREIGN KEY (Country_Id) REFERENCES Dim_Country(Country_Id)
);

-- 9. Dim_Season:
CREATE TABLE Dim_Season (
    Season_Id INT PRIMARY KEY,
    Man_of_the_Series INT,
    Orange_Cap INT,
    Purple_Cap INT,
    Season_Year INT,
    FOREIGN KEY (Man_of_the_Series) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Orange_Cap) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Purple_Cap) REFERENCES Dim_Player(Player_Id)
);

-- 10. Dim_Toss_Decision:
CREATE TABLE Dim_Toss_Decision (
    Toss_Decision_Id INT PRIMARY KEY,
    Toss_Decision_Name VARCHAR(100)
);

-- 11. Dim_Venue:
CREATE TABLE Dim_Venue (
    Venue_Id INT PRIMARY KEY,
    Venue_Name VARCHAR(100),
    City_Id INT,
    FOREIGN KEY (City_Id) REFERENCES Dim_City(City_Id)
);

-- 12. Dim_Win_Type:
CREATE TABLE Dim_Win_Type (
    Win_Type_Id INT PRIMARY KEY,
    Win_Type VARCHAR(100)
);




-- 13. Dim_Out_Type:
CREATE TABLE Dim_Out_Type (
    Kind_Out_Id INT PRIMARY KEY,
    Out_Name VARCHAR(100)
);

-- 14. Dim_Outcome:
CREATE TABLE Dim_Outcome (
    Outcome_Type_Id INT PRIMARY KEY,
    Outcome_Type VARCHAR(100)
);

-- 15. Dim_batsman_scored:
CREATE TABLE Dim_batsman_scored (
    Match_Id INT,
    Over_Id INT,
    Ball_Id INT,
    Runs_Scored INT,
    Innings_No INT,
    PRIMARY KEY (Match_Id, Over_Id, Ball_Id, Innings_No)
);

-- 16. Dim_extra_run:
CREATE TABLE Dim_extra_run (
    Match_Id INT,
    Over_Id INT,
    Ball_Id INT,
    Extra_Type_Id INT,
    Extra_Runs INT,
    Innings_No INT,
    PRIMARY KEY (Match_Id, Over_Id, Ball_Id, Innings_No),
    FOREIGN KEY (Extra_Type_Id) REFERENCES Dim_Extra_Type(Extra_Type_Id)
);

-- 17. Dim_player_match:
CREATE TABLE Dim_player_match (
    Match_Id INT,
    Player_Id INT,
    Role_Id INT,
    Team_Id INT,
    PRIMARY KEY (Match_Id, Player_Id),
    FOREIGN KEY (Player_Id) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Role_Id) REFERENCES Dim_Role(Role_Id),
    FOREIGN KEY (Team_Id) REFERENCES Dim_Team(Team_Id)
);




-- 18. Dim_wicket_taken:
CREATE TABLE Dim_wicket_taken (
    Match_Id INT,
    Over_Id INT,
    Ball_Id INT,
    Player_Out INT,
    Kind_Out INT,
    Fielders INT,
    Innings_No INT,
    PRIMARY KEY (Match_Id, Over_Id, Ball_Id, Innings_No),
    FOREIGN KEY (Player_Out) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Kind_Out) REFERENCES Dim_Out_Type(Kind_Out_Id)
);

-- 19. Dim_match:
CREATE TABLE Dim_match (
    Match_Id INT PRIMARY KEY,
    Team_1 INT,
    Team_2 INT,
    Match_Date DATE,
    Season_Id INT,
    Venue_Id INT,
    Toss_Winner INT,
    Toss_Decide INT,
    Win_Type INT,
    Win_Margin INT,
    Outcome_type INT,
    Match_Winner INT,
    Man_of_the_Match INT,
    FOREIGN KEY (Team_1) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Team_2) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Season_Id) REFERENCES Dim_Season(Season_Id),
    FOREIGN KEY (Venue_Id) REFERENCES Dim_Venue(Venue_Id),
    FOREIGN KEY (Toss_Winner) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Toss_Decide) REFERENCES Dim_Toss_Decision(Toss_Decision_Id),
    FOREIGN KEY (Win_Type) REFERENCES Dim_Win_Type(Win_Type_Id),
    FOREIGN KEY (Outcome_Type) REFERENCES Dim_Outcome(Outcome_Type_Id),
    FOREIGN KEY (Match_Winner) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Man_of_the_Match) REFERENCES Dim_Player(Player_Id)
);









-- 20. Dim_ball_by_ball:
CREATE TABLE Dim_ball_by_ball (
    Match_Id INT,
    Over_Id INT,
    Ball_Id INT,
    Innings_No INT,
    Team_Batting INT,
    Team_Bowling INT,
    Striker_Batting_Position INT,
    Striker INT,
    Non_Striker INT,
    Bowler INT,
    PRIMARY KEY (Match_Id, Over_Id, Ball_Id, Innings_No),
    FOREIGN KEY (Team_Batting) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Team_Bowling) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Striker) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Non_Striker) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Bowler) REFERENCES Dim_Player(Player_Id)
);



-- Fuzzy File:
CREATE TABLE [dbo].[fuzzy_player_data](
	[Player_Id] [int] NOT NULL,
	[Player_Name] [varchar](200) NOT NULL,
	[Player_fuzzy_reference_Name] [varchar](200) NOT NULL
)






-- Fact table


CREATE TABLE Fact_Match_Details (
    MatchDetail_Id INT PRIMARY KEY identity,
    Match_Id INT,
    Over_Id INT,
    Ball_Id INT,
    Innings_No INT,
    Team_Batting INT,
    Team_Bowling INT,
    Striker_Batting_Position INT,
    Striker INT,
    Non_Striker INT,
    Bowler INT,
    Runs_Scored INT,
    Player_Out INT,
    Kind_Out INT,
    Fielders INT,
    Extra_Type_Id INT,
    Extra_Runs INT,
    Season_Id INT,
    Venue_Id INT,
    Toss_Decide INT,
    Win_Type INT,
    Outcome_Type INT,
    Player_Id INT,
    Role_Id INT,
    Team_Id INT,
    Batting_Hand_Id INT,
    Bowling_Skill_Id INT,
    Country_Id INT,
    City_Id INT,
    FOREIGN KEY (Match_Id) REFERENCES Dim_match(Match_Id),
    FOREIGN KEY (Team_Batting) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Team_Bowling) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Striker) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Non_Striker) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Bowler) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Player_Out) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Kind_Out) REFERENCES Dim_Out_Type(Kind_Out_Id),
    FOREIGN KEY (Extra_Type_Id) REFERENCES Dim_Extra_Type(Extra_Type_Id),
    FOREIGN KEY (Season_Id) REFERENCES Dim_Season(Season_Id),
    FOREIGN KEY (Venue_Id) REFERENCES Dim_Venue(Venue_Id),
    FOREIGN KEY (Toss_Decide) REFERENCES Dim_Toss_Decision(Toss_Decision_Id),
    FOREIGN KEY (Win_Type) REFERENCES Dim_Win_Type(Win_Type_Id),
    FOREIGN KEY (Outcome_Type) REFERENCES Dim_Outcome(Outcome_Type_Id),
    FOREIGN KEY (Player_Id) REFERENCES Dim_Player(Player_Id),
    FOREIGN KEY (Role_Id) REFERENCES Dim_Role(Role_Id),
    FOREIGN KEY (Team_Id) REFERENCES Dim_Team(Team_Id),
    FOREIGN KEY (Batting_Hand_Id) REFERENCES Dim_Batting_Style(Batting_Hand_Id),
    FOREIGN KEY (Bowling_Skill_Id) REFERENCES Dim_Bowling_Style(Bowling_Skill_Id),
    FOREIGN KEY (Country_Id) REFERENCES Dim_Country(Country_Id),
    FOREIGN KEY (City_Id) REFERENCES Dim_City(City_Id)
);






-- Query 










DELETE FROM Error_Handling;
DELETE FROM [dbo].[sysssislog];
DELETE FROM Fact_Match_Details;
DELETE FROM Dim_match;
DELETE FROM Dim_player_match;
DELETE FROM Dim_Season;
DELETE FROM Dim_Role;
DELETE FROM Dim_Toss_Decision;
DELETE FROM Dim_Win_Type;
DELETE FROM Dim_Outcome;
DELETE FROM Dim_match;
DELETE FROM Dim_ball_by_ball;
DELETE FROM Dim_extra_run;
DELETE FROM Dim_wicket_taken;
DELETE FROM Dim_Player;
DELETE FROM Dim_batsman_scored;
DELETE FROM Dim_Venue;
DELETE FROM Dim_Out_Type;
DELETE FROM Dim_city;
DELETE FROM Dim_Team;
DELETE FROM Dim_Country;
DELETE FROM Dim_Batting_Style;
DELETE FROM Dim_Bowling_Style;
DELETE FROM Dim_Team;
DELETE FROM Dim_Extra_Type;

