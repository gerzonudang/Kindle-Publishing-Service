#  Unit 7 Project: ATA Advertising Service

## Preliminaries: The More Things Change

### Ambiguity, Complexity, and Scope

Ambiguity will be increasing from previous projects. Tasks instructions will contain fewer details, as we expect you to 
reference documentation and deep dive into the code yourself to understand how things work. Even if there isn't a 
specific task for it, you're of course welcome to create any diagrams or other notes as a reference for yourself or 
others!

There will be some increasing complexity as we work with ExecutorServices for the first time.

You'll have your fellow participants in the same situation as you, so remember to collaborate: rely on each other for 
assistance, and share your own knowledge.

## Unit 7 Project Progress and Tracking

### Doneness checklist

You're done with the project when: 

* You have successfully passed all tests in CodeGrade

### cloudformation commands

You'll want to run the following commands to setup your DynamoDB tables for this project (note that you will need to wait for the first command's stack to finish building before running the next commands):

```
aws cloudformation create-stack --region us-west-2 --stack-name advertisingservice-createtables --template-body file://configurations/cloudFormation/ddb_tables.template.yml --capabilities CAPABILITY_IAM
aws dynamodb batch-write-item --request-items file://configurations/cloudFormation/content_table.json
aws dynamodb batch-write-item --request-items file://configurations/cloudFormation/targeting_group_table.json
aws dynamodb batch-write-item --request-items file://configurations/cloudFormation/targeting_group_table2.json
```

## The Problem: ATA Advertising

ATA's AdvertisingService serves advertisements for ATA. These advertisements show up on the retail website and use 
targeting to present different ATA advertisements to each individual. The targeting tries to take advantage of what 
Amazon knows about you to show you the particular ad that is most likely to appeal to you.

An overview of the service is covered in the [design document](DESIGN_DOCUMENT.md). We encourage you to read that now
before continuing below.

## Project Mastery Tasks

### [Mastery Task 1: Filter out the noise](tasks/project-mastery-tasks/MasteryTask01.md)
### [Mastery Task 2: Concurrent Tasks](tasks/project-mastery-tasks/MasteryTask02.md)
### [Mastery Task 3: Ads don't grow on trees (or do they?)](tasks/project-mastery-tasks/MasteryTask03.md)


