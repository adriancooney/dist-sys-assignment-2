The goal of this assignment is to implement and test some modifications for the Map Reduce Application covered in class.

The application should include the following features:

1: The program takes a list of text files to be processed. The file list can be passed to the program via the command line. Use a number of large text or log files for testing your program.

2: Modify the main part of the program to assign the Map or Reduce functions (in Approach Three) to a Thread Pool with a configurable number of threads. Lookup the Java concurrency utilities for examples of using Thread Pools. The actual number of threads can be passed to the program as a command line parameter. Run the program and implement some method within the program to measure as accurately as possible how long it takes to process the full set of large input text files used for testing.

3: Create another version of the program that implements exactly the same functionality using the thread safe versions of the Map and List classes that are available in the java.util.concurrent package. In this case you should not have to use callback functions from each thread but should be able to directly update the shared output data structures within the threads. In particular, you should use the CopyOnWriteArrayList<E> and ConcurrentHashMap<K,V> implementation classes for the shared output data structures.

4: Compare the two versions of the program you have implemented. Which versi    on provides the better performance?

This assignment can be done either individually or in groups of no more than two students. When completed you should submit copies of the code you have written for the assignment as well as screen shots of the application running. These screen shots should include evidence of having tested your application with some large text files. If you are submitting for a group then don't forgot to supply the full name and ID number of the other group member. All submissions should be done via Blackboard and if you submit more than one attempt then only the final attempt will be marked.