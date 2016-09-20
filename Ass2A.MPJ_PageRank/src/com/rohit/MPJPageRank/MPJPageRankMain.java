package com.rohit.MPJPageRank;

import mpi.*;

public class MPJPageRankMain {

	public static void main(String[] args) {
		MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		System.out.println("Process " + rank + " of " + size + " processes");

		MPI.Finalize();
	}

}
