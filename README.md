# Homework 1
Author: Mattia Chiarle, mchiar2@uic.edu

## How to execute my program

I will report a list of all the steps needed to run my program locally.

    1. Generate the .ngs files
    2. Update application.conf with the desired parameters
    3. Generate the shards by running CreateShards in GraphSharding.scala
    4. Run the Main. Be sure that you properly set the arguments in Run/Edit configurations
    5. Run ComputeStatistics in Statistics.scala to get the statistics on the output of the Map Reduce program

## How to deploy my program on AWS

[Link to the Youtube Video](https://youtu.be/ThSlo1KE4wQ)

## Idea behind the comparison algorithm

In all the comments and in this README I will refer to the result of the comparison as similarity. Strictly speaking, since I compute how different two nodes are, the most appropriate term would be dissimilarity. However, since it's just a matter of notation, I decided to leave similarity since it expressed in a clearer way my reasoning.

The whole idea is to compute the difference between each relevant parameter between two nodes, normalize it (to give the same weight to all the parameters) and multiply it by a given coefficient.
The values provided for coefficients and the threshold came both from an analysis of NetGameSim and by using a trial-and-fix approach.

Initially, I tried to perform only a comparison of two nodes to compute the similarity. However, the results weren't great, especially when the number of nodes in the graph increased. Due to this, I thought about including also the first level neighbors in the comparison. With a little additional overhead, I was able to obtain far better results.
This approach could be iteratively extended, i.e. including also the second-level neighbors and so on. However, this becomes more a matter of balancing computational and performance aspects, which go beyond from the scope of the project.
I also don't exclude that, by performing fine-graded tests, it could be possible to achieve better results with a more precise tuning of my parameters. In my tests, the values stored in application.conf provided the best results.

## Mapper and reducers logic

### Sharding

I decided to create one file for each node (both in the original and in the perturbed graph). This is in the middle between creating a unique file and creating a file for each comparison between two nodes (which would have resulted in around 180.000 files for a graph of 300 nodes instead of around 600).
Again, the precise decision about the dimension of the sharding is strictly related to the actual implementation of the Map Reduce, or in other words it depends on the practical situation that goes beyond the scope of the project. 

### Mapper

There are two main tasks for the mapper. In fact, the goal is to compute which nodes has been added, modified, removed and which remained the same.
By performing a comparison original-perturbed, it is easy to retrieve information about added, modified and equal nodes, but it's impossible to get information about the added nodes. This is what I defined in the code as Task 1.
To retrieve the added nodes, task 2 was introduced, which basically is a comparison perturbed-original.
Since we need to achieve elastic parallelism, i.e. each node will be analyzed by different mappers, I needed to find a relevance order so that the reducer will be able to understand what is the state of the node. The orger is:

    0 = added
    1 = removed
    2 = modified
    3 = same

In the code, I also saved the node that provided that result in the other graph. This has been useful to evaluate the Traceability Links in the statistics.

### Reducer

The reducer will then take the maximum among all the values provided by the mapper.
For task 1, same has of course the highest priority. Modified is immediately after. The idea is that almost all mappers will provide 1 as result for a certain node (we have at most one correspondence), but if at least one provided 2 or 3 we take that value.
For task 2 instead, all the nodes will be marked with added (again, we have at most one correspondence). If we have at least one comparison below the threshold, the node is marked as "removed" and it won't be placed among the added nodes.
