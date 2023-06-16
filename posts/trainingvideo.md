---
title: "trainingvideo"
subtitle: "Implementing Pokemon-like combat mechanics in PICO-8."
date: "2020-12-22"
---

I'd like to talk about non MDM dimensional processing, 
focusing on Spark based processing and pattern based processing. 
So I created simple scripts to load all the non MDM dimensions using one standard Python script that is highly configurable. 
It allows a config file to be passed to the script. 
It uses the configurations in that config file to figure out what dimension it's supposed to load,
how many queries it's supposed to execute to process this dimension. 
So the way I split it out is I took each and every processing step that happens in the dimension load. 
And I put each of these steps into single queries because spark can only process one query at a time,
it can't do like a whole batch of queries. 
So each script was independent of another one and each script was its own logic flow. 
That helped me identify how many scripts I need to run in sequence and what sequence they need to run. 
And I just did in the config file that we want to run 10 scripts and the like the solution itself the python script is smart enough to realize where to find the scripts what the scripts name or code and execute them. 
So this is actually allowed us to be dynamic in our loading of non dimensional process and also allow reusability of code. 
We can reuse the same script over and over and over again. 
And just to give you a little bit of context, 
I'm going to open the Python script that runs, 
show you what the config file looks like. 
I'm going to use the TG dimensions, tg dimension for sobeys, this is an example for this just to show how everything is being processed in the back end. 
So the script location might change but you can find this and Ctrl M. 
When you look at your non mdm flow. 
Right now it's sitting in this non mdm directory. 
And the file name that's running depends on Spark name is this, "spark_non_mdm_load.py". 
So if we open this script, right, you can see from the onset that it's looking for specific parameters, right? 
It's looking for three parameters specified here. 
There's the config file path, which is the configuration file I was talking about. 
The environment is very important. 
The environment helps us identify which set of configurations to pick. 
So you can specify multiple configurations, one for production, one for testing, one for QA one for dev so that you can point to different in a different configuration settings. 
And you don't miss a prod. 
That's the only reason why I did this. 
The first one is the script part. 
So this is pretty straightforward. 
This script imports the standard libraries that you need to run Spark source by spark Sql, I have some outdated stuff, but you can go through the code later on. 
But basically just to give you context, what this script is doing. 
So it uses the arguments that we've passed. 
So these arguments, and in particular, these last two, determine what is the dimension it's loading and what is supposed to be run, right. 
So it starts basically here. 
Where it uses a configReader. 
Okay, so the configReader reads admin list want to pick up the argument for the config file, and then it uses argument_list number two position to pick up the environment, 
whether it's prod and this is config files. 
So for this script, if you go to this directory, the "gen_scrpt/preprcs/sobeys", there is this "tgViewCreate.cfg" config file. 
And you can also find this in the Control M script. 
So if you open the Control scripts, you will find the pipe the path to the python script and the path to the config file. 

Maybe I need to open

this is the name of config pass bug Tiki not in download. So if we open this configuration file, you can actually see in here, we have different environments. This is what I meant by environment, which is argument list number two here so you will pass PRD if you want this batch of configurations or TST or Dev, if you want the other pages right. So based on these, you can see what the query is doing if it picks the prod parameter parameters, it starts so it starts populating these values with whatever is in the config right so this conflict rate against number of periods to run the team abbreviation team suffix, the client, the view Create config path. And I'll explain to you what each of these means. So the demon abbreviation TG, it tells us which dimension we're going to run. So if you want to set this up for consumer you can use cm for example, whatever your team abbreviation is, item is it promo period. So it depends on whatever non MDM dimension you're loading. You use the variable for that year. And then this suffix is just used to create the source stage table in the source stage table is created using the script. So what this script does is it just takes whatever has been loaded in the source stage table, your source stage table should be defined first. You should have a Hadoop a hive table that is our toolPak where your source data sitting. So this script takes the data the source from the file, what has been loaded, and makes sure that it's being utilized in our queries, right there, teaching view Create config file. This one posts, all the queries that you're going to run that requires you to create a view. So if you want to just run a select, so you're not doing an insert into this table or you're not doing a delete or update or whatever. If it's just a select and you want to temporarily import that data in a view so that you can use it later. This part will allow you it's a pipe delimited file, which allow you to say in query number four, I want to create a view code. So this Tiki stage in query number 10. I want to create a few code top service T stage backup or something like that. And whatever query is executed in that query is going to be stored in memory in that temporary view that you specified, and you can use it later in your periods. Right. So that's pretty standard. How to so right now what we're going to do is we're going to look into these two periods, right, there's two files. So first to start with, to point the conflict power, just to see what is in this or to or to


see what so you see we have a specific query ID and name so in theory for I'm going to create a view code. So this Tiki stage, okay, that's basically what that callback for config file is says we'll run six queries. What are those queries, so the only way to find out the queries is specified in this script, this script, the main python script, will look for queries and the way it looks for queries is it is DEM. build directory, Team abbreviation, do query and the counter which is a loop if we say we're going to wrap six queries, it will look 123456. So it's looking for all those queries. And it's just append in dynamically switch knows I'm going to run TG dim query one ticket in query two, etc. For example, I'll show you here exactly what these curves look like CF. TGT and query one query to query 3456. So those are the six credits that are going to be executed in sequence. So just to break down quickly what this clip will do, it will start first by reading the config file which have gone through it picks up all the required what the required variables including this source, if create which are mentioned and they all say it will create the best it will pull the data from the source stage table. Okay. So if we look in our config file, it is this guy. So this is the query that will pull the data from our state


this so it says select star from warehouse service, data source data. So it's just pulling data from the source stage. Table, okay, that's what it's doing. And once that's done, that's the first query that's going to run right. So it's executed. See.


See the first one is so stiff create so that SELECT query is executed and the data is stored into this data frame, source df and we create a view out of that right. The fuel name PAC has been created so it to be client dem abbreviation and team suffix. Once that view has been created, we get the list of aqueous that will require us to be created. Remember I talked about this query here. So we're going to go read this file, the separator is pipe. So it will create a temp view that is all the dimensions sir all the temp fields that need to be created in which query number is responsible for creating that temp table. So here's query for so if I look at this query for whatever is being executed here, this select is going to be stored temporarily, in a view called solve this teacher stage. The python script will just loop like I said, it has a counter to use the number of queries that have been configured configuration four to six and it will call and check if the query exist in the views. So it will look does the query number exist in this file? If not, so if it's query number one, obviously only the query number two will be there. So if it's not there, it will pass it will sorry if it's not there, it'll run this query, which doesn't require creating a view. So it's just a straight query, that spark is just going to run. If a view needs to be created, it'll run a different query. So if it gets to count the number four, it will find the view that needs to be created and it will run a different thing. So it will it will go in create this temp, replace a creative view and it will use the view name that was specified in the file, in this case service TG state to create a template view in memory and once all the queries are run, query, this script just terminates at the end finishes running queries. So the most important things, I mean, you can follow the query on your own time. But the most important thing to take note of here is just to make sure that your queries are singular, okay? You cannot run multiple queries and don't worry about putting the semicolon so you can't say DROP TABLE IF EXISTS service TG master, and then immediately after that, you run insert something this query will fail. So they have to be broken up. So if you take one on HQ, hell, you can take each step that HQ is executing, you put it into different SQL queries, and you just need to label them appropriately if it's consumer they need to be seen in query one, two cm clear whatever number of queries you have. And once that is done, you need to create in Hadoop, a file this it will be cm, view, create whatever your dimension is called. And this needs to host all the views that you're going to create in which query means to create a view so if you're not doing an insert, update or any DDL who need to go it's just a select statement. You will need to go and create a view for it and you need to specify what appears creating that view. Once that is done, you just need to copy this config file this one and just leverage appropriately spark cn or whatever dimension it is and inside your specify your dimension CL and to specify everything's specific to your C and dimension. The number of queries are going to run where the view Create config file is an audit. I hope this was helpful. Thank you so much for tuning in. Thanks