### GoalBet
 
GoalBet is a sports data analytics company that analyzes historic sports data. We build predictive models to predict sports scores for insight analysis & Betting purposes.

This project builds an end-to-end Extract, Transform & Load (ETL) pipeline that pulls data from https://www.football-data.co.uk/englandm.php

We then use PostgreSQL for persisting the transformed data.

To handle the automation, I am leveraging on Windows Task Scheduler, so I have a bat file that is running the get_new-data script.
