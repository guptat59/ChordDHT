Team Members:

1. Rakesh Dammalapati  29938403
2. Tarun Gupta Akirala 43394921

-------------------------------------------
What is working?

1. We have been able to simulate sequential joining of actors to the Chord. Our Hash space is constant. It is upto the largest integer value possible (generally 2^30).
2. After all the nodes are joined, akka scheduler will schedule each actor to make one request per second (numRequests will be controlled using CL variables).
3. Each node will perform designated numRequests and will increment the global variable of files found so far.
4. System will print the average number of hops after all the file search requests are over. System will exit after this.

-------------------------------------------

How to run ?

>sbt
>compile
>run numNodes numRequests 

To run with bonus,

>run numNodes numRequests deletionEnabled

To run the bonus part, set the value of deletionEnabled argument to True.

-------------------------------------------
Largest network ? 

19000 Nodes.

Detailed output:
Input NumNodes, NumReq:19000,10
Total time to join:    20015
Total search time :    84860
Files found:           190000
Total number of hops:  1697091
Average number of hops:8

*Time in ms
-------------------------------------------

[BONUS]

If you randomly shutdown a node, system will still be able to search the given file after it recovers using stabilize routine.

1. Bonus is implemented in the same file Project3.scala.
2. The flag Constants.deletionEnabled is set to false by default. To run the bonus part, SET THIS FLAG TO TRUE. 
3. The number of nodes to be deleted will be calculated using Math.ceil(numNodes/10) by default. This can be supressed if you use a third CL argument as explained above.

How the failure model is implemented?

Instead of having a single successor, we maintain multiple successors (successorList as explained in the paper) for every node to stabilize the system in case a node dies.

In our case, we have two successors for every node, which are immediate successor and supersuccessor(immediate successor's successor). Every node watches their successors. Consider the case when a node dies, the successor and predecessor relation between two nodes on the Chord is BROKEN at this point. Thanks to the watch on the successor nodes, each node in system will know when its successor has died and it will update its successor to the next available successor in the successorList. It will also intimate its new successor to update its stale predecessor information. By doing this the system will be JOINED again. After which the finger table is modified accordingly.

We have tested our logic by deleting up to ten percent of the nodes in a huge (~15000) Chord topology. The file search took relatively longer number of hops (due to some outdated information in finger entries initially, which will be updated eventually) but the system is resilient and was able to find the files in logarthmic number of hops.

Observations: 
Nodes keeping the information of multiple successors is crucial.
The robust nature of chord lies in its ability to find hashKeys in logarthimic time even when multiple nodes join/leave the network

-------------------------------------------