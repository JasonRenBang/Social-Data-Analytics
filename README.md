# Social-Data-Analytics
In this project, Message Passing Interface(MPI) is used to finish the communication between the processor. we import the mpi4py library to help the later parallelized processing operations. The core idea for our program to reading the big Twitter file by using parallelized processing method is that we separated the whole big Twitter file into several blocks and read each block for different processors. And then, we gathered the data from all processors and used these data in the rank = 0 processor to operate them to finish three missions.

It is better to load extremely large datasets. And we analyze the top 10 cities with the most tweets number, top 10 users with the most tweets number made, and top 10 users with the most tweets location number.

In conclusion, parallel computing allows more work to be done per unit of time since multiple processors can work on different parts of a problem at the same time, which can maximise resource usage and reduce running time. It is useful and efficient to process big data sets or do complete computations and analyses

How to start it: mpiexec -np 8 python3 main.py
cores: 8
