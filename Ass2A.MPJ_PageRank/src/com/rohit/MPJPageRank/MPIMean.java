package com.rohit.MPJPageRank;

import mpi.MPI;

public class MPIMean {
    public static final int ARRAY_SIZE = 1000;

    public static void main(String args[]) throws Exception {
        MPI.Init(args);

        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        int values[] = new int[ARRAY_SIZE];

        // generate an array with some random values
        if (rank == 0) {
            for (int i = 0; i < values.length; i++) {
                values[i] = (int) (Math.random() * 100);
            }
        }

        int sizeBuf[] = new int[1];
        int localSize = 0;

        // divide the array in to equal chunks of number of processes.
        // This assumes size of array is divisible by the number of processes.
        int chunkSize = ARRAY_SIZE / size;
        if (rank == 0) {
            // first send each process the size that it should expect
            for (int i = 1; i < size; i++) {
                sizeBuf[0] = chunkSize;
                MPI.COMM_WORLD.Send(sizeBuf, 0, 1, MPI.INT, i, 1);
            }
            localSize = chunkSize;
        } else {
            // receive the size of the array to expect
            MPI.COMM_WORLD.Recv(sizeBuf, 0, 1, MPI.INT, 0, 1);
            localSize = sizeBuf[0];
        }

        int localValues[] = new int[localSize];
        if (rank == 0) {
            // send the actual array parts to processes
            for (int i = 1; i < size; i++) {
                MPI.COMM_WORLD.Send(values, i * ARRAY_SIZE / size, chunkSize, MPI.INT, i, 1);
            }
            // for process 0 we can get the local from the data itself
            for (int i = 0; i < chunkSize; i++) {
                localValues[i] = values[i];
            }
        } else {
            // receive the local array
            MPI.COMM_WORLD.Recv(localValues, 0, chunkSize, MPI.INT, 0, 1);
        }

        // everyone calculates their local means
        double localMean = MPIMean.culculateMean(localValues);

        double localMeanBuf[] = new double[1];
        localMeanBuf[0] = localMean;
        double globalSum = localMean;
        // now sum up all the means
        // this has to be done only in one machine, lets do it in rank 0 machine
        // every process other than 0 should send its local mean to process 0
        // 0th process should sum up these values and divide that sum by size
        
        // you can do all the above using one MPI call, allreduce, have a look at that method and try to do it
        // if your sum is correct, the following two should be same
        double sum[] = new double[1];
        if (rank ==0) {
           for (int i = 1; i < size; i++) {
               MPI.COMM_WORLD.Recv(sum, 0, 1, MPI.DOUBLE, i, 1);
               globalSum += sum[0];
           }
        } else {
           sum[0] = localMean;
           MPI.COMM_WORLD.Send(sum, 0, 1, MPI.DOUBLE, 0, 1);		
	}       
    
        MPI.COMM_WORLD.Allreduce(localMeanBuf, 0, sum, 0, 1, MPI.DOUBLE, MPI.SUM);
         
        double []bcast = new double[5]; 
        // rank 0 only has the values, because it is broadcasting
        if (rank == 0) {
            for (int i = 0; i < bcast.length; i++) {
                bcast[i] = i + .5;
            }		
        }     
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bcast.length; i++) {
            sb.append(bcast[i]).append(" ");
        }
        System.out.println("Process :" + rank + " bcast array: " + sb.toString());
        // every process invokes the bcast
        MPI.COMM_WORLD.Bcast(bcast, 0, 5, MPI.DOUBLE, 0);
        // now everybody should have the same values in the bcast array
        sb = new StringBuilder();
        for (int i = 0; i < bcast.length; i++) {
            sb.append(bcast[i]).append(" ");
        }
        System.out.println("Process :" + rank + " bcast array: " + sb.toString());
        if (rank == 0) {
            // display the mean calculated using parallel processing
            System.out.println("Mean calculated using MPI: " + sum[0]/size);
            // display the mean calculated using all the values
            System.out.println("Real Mean calculated using the global values: " + MPIMean.culculateMean(values));
        }

        MPI.Finalize();
    }

    public static double culculateMean(int []num) {
        int sum = 0;
        for (int i = 0; i < num.length; i++) {
            sum += num[i];
        }
        return sum / (double) num.length;
    }
}
