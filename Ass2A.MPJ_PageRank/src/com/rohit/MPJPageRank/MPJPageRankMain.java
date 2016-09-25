package com.rohit.MPJPageRank;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import mpi.MPI;

public class MPJPageRankMain {

	// adjacency list read from file as a string/page
	private ArrayList<String> adjListOfStrings = new ArrayList<String>();

	// adjacency matrix read from file
	private HashMap<Integer, ArrayList<Integer>> adjList = new HashMap<Integer, ArrayList<Integer>>();
	// input file name
	private String inputFile = "";
	// output file name
	private String outputFile = "";
	// number of iterations
	private int iterations = 10;
	// damping factor
	private double dampingFactor = 0.85;

	// number of URLs
	private int size = 0;
	// calculating rank values
	private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();

	// rank array
	private double rankArray[];

	public void parseArgs(String[] args) {
		inputFile = args[0];
		outputFile = args[1];
		iterations = Integer.parseInt(args[2]);
		dampingFactor = Double.parseDouble(args[3]);
	}

	// Read the input from the file and populate the adjacency matrix
	public int loadInput(int rank) throws IOException {
		// System.out.println("\nRead " + inputFile);
		try {
			Scanner in = new Scanner(new FileReader(inputFile));

			while (in.hasNextLine()) {
				this.size++;
				adjListOfStrings.add(in.nextLine());
			}
			in.close();

		} catch (IOException e) {
			System.out.println("Exception " + e + "\n\nStack Trace:");
			e.printStackTrace();
		}
		return this.size;
	}

	private void display(ArrayList<String> adjListOfStrings2, int rank) {
		System.out.println("\nAdj at rank " + rank + ":");
		for (String s : adjListOfStrings2) {
			System.out.println(s);
		}
	}

	public void calculatePageRank() {
		HashMap<Integer, Double> nextRankValues = new HashMap<Integer, Double>();

		// initialize rank values
		double avg = 1.0 / size;
		for (int i = 0; i <= size; i++) {
			rankValues.put(i, avg);
			// nextRankValues.put(i, avg);
		}
		double constantFactor = (1 - dampingFactor) / size;
		double myRankContribution = 0.0;
		ArrayList<Integer> outLinks;

		while (iterations-- > 0) {
			nextRankValues = (HashMap<Integer, Double>) rankValues.clone();
			for (int i = 0; i <= size; i++) {
				outLinks = adjList.get(i);
				// My contribution towards each page
				myRankContribution = rankValues.get(i) / outLinks.size();

				for (int page : outLinks) {
					nextRankValues.replace(page, myRankContribution + nextRankValues.get(page));
				}
			}
			for (int i = 0; i <= size; i++) {
				double rank = constantFactor + dampingFactor * rankValues.get(i);
				rankValues.replace(i, rank);
			}
			rankValues = nextRankValues;

		}
	}

	// Print top 10 pagerank values in descending order.
	public void printValues() throws IOException {
		System.out.println("Ranks:");
		for (int i = 0; i < size; i++) {
			System.out.print(rankValues.get(i));
		}
	}

	private void display() {
		System.out.println("input: " + inputFile);
		System.out.println("output: " + outputFile);
		System.out.println("iterations: " + iterations);
		System.out.println("dampingFactor: " + dampingFactor);
	}

	private static void display(double[] a, int rank) {
		System.out.println("pageranks at process " + rank + ": ");
		for (int i = 0; i < a.length; i++) {
			System.out.println(i + ": " + a[i]);
		}
	}

	private void displayAdjList(int rank) {
		System.out.println("\nAdj at rank " + rank + ":");
		for (int i = 0; i < adjList.size(); i++) {
			System.out.print("key: " + i + ", " + adjList.get(i).size() + " url(s): ");
			for (int j = 0; j < adjList.get(i).size(); j++) {
				System.out.print(adjList.get(i).get(j) + " ");
			}
			System.out.println();
		}
	}

	private static void displayHash(HashMap<Integer, ArrayList<Integer>> h, int rank) {
		System.out.println("\nHash at rank " + rank + ":");
		for (int i = 0; i < h.size(); i++) {
			System.out.print("key: " + i + ", " + h.get(i).size() + " url(s): ");
			for (int j = 0; j < h.get(i).size(); j++) {
				System.out.print(h.get(i).get(j) + " ");
			}
			System.out.println();
		}
	}

	public static void main(String[] args) throws IOException {
		// Read command line args along with MPI initiation
		String inputArgs[] = MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		// System.out.println("Process " + rank + " of " + size + " processes");

		int numPages = 0;
		MPJPageRankMain mpjPR = new MPJPageRankMain();
		// decide what u want only in rank 0 n what in all nodes
		if (rank == 0) {
			mpjPR.parseArgs(inputArgs);
			// mpjPR.display();
			numPages = mpjPR.loadInput(rank);
			// mpjPR.display(mpjPR.adjListOfStrings, rank);
			// System.out.println("numPages = " + numPages);
			// mpjPR.displayAdjList(rank);
			// mpjPR.calculatePageRank();
			// mpjPR.printValues();
		}

		int numOfPages[] = new int[1];
		int localChunkSize = 0;
		int localNumPages = 0;
		int remoteChunkSize = 0;

		if (rank == 0) {
			// first send each process the size that it should expect
			numOfPages[0] = mpjPR.size;
			for (int i = 1; i < size; i++) {
				MPI.COMM_WORLD.Send(numOfPages, 0, 1, MPI.INT, i, 1);
			}
			localNumPages = mpjPR.size;
			remoteChunkSize = mpjPR.size / size;
			// process 0 takes up extra pages if unevenly divided
			localChunkSize = mpjPR.size - remoteChunkSize * (size - 1);
		} else {
			// receive the size of the array to expect
			MPI.COMM_WORLD.Recv(numOfPages, 0, 1, MPI.INT, 0, 1);
			localNumPages = numOfPages[0];
			localChunkSize = localNumPages / size;
		}

		// testing
		{
			// System.out.println("process " + rank + " localNumPages = " +
			// localNumPages);
			// System.out.println("process " + rank + " localChunkSize = " +
			// localChunkSize);
		}

		if (rank == 0) {
			// send adj list
			for (int processNumber = 1; processNumber < size; processNumber++) {
				int pagesFrom = processNumber * remoteChunkSize;
				if (localChunkSize - remoteChunkSize > 0) {
					pagesFrom += localChunkSize - remoteChunkSize;
				}
				int pagesTo = pagesFrom + remoteChunkSize;
				// System.out.println(
				// "sending to process " + processNumber + ": pages " +
				// pagesFrom + " to " + (pagesTo - 1));
				for (int page = pagesFrom; page < pagesTo; page++) {
					int l = mpjPR.adjListOfStrings.get(page).length();
					if (processNumber == 1) {
						// System.out.println(mpjPR.adjListOfStrings.get(page).toCharArray());
						// System.out.println("length = " + l);
					}
					MPI.COMM_WORLD.Send(mpjPR.adjListOfStrings.get(page).toCharArray(), 0, l, MPI.CHAR, processNumber,
							1);
				}
			}
			// To-do: remove all but <localchunksize> elements for rank 0

		} else {
			// System.out.println(
			// "accepting at process " + rank + ": pages " + 0 * remoteChunkSize
			// + " to " + localChunkSize);
			int len = 0;
			for (int page = 0; page < localChunkSize; page++) {
				char[] pageOutLinks = new char[localNumPages];
				MPI.COMM_WORLD.Recv(pageOutLinks, 0, localNumPages, MPI.CHAR, 0, 1);
				StringBuffer sb = new StringBuffer();
				if (rank == 1) {
					// System.out.println("len = " + pageOutLinks.length + "
					// recv par3 = " + localChunkSize);
				}
				for (int i = 0; i < pageOutLinks.length; i++) {
					if (rank == 1) {
						// System.out.print(pageOutLinks[i]);
					}
					sb.append(pageOutLinks[i]);
				}
				if (rank == 1) {
					// System.out.println("*** "+ sb);
				}
				mpjPR.adjListOfStrings.add(sb.toString());
				// System.out.println(pageOutLinks.toString().charAt(0));
			}
		}

		if (rank == 0) {
			// mpjPR.display(mpjPR.adjListOfStrings, rank);
		}

		// To-do: dangling nodes

		if (rank == 0) {
			// *********** generate rank array **********
			// Each element is by default initialized to 0.0
			mpjPR.rankArray = new double[mpjPR.size];
			double avg = 1.0 / mpjPR.size;
			for(int i = 0; i < mpjPR.size; i++) {
				mpjPR.rankArray[i] = avg;
			}
			// send rank array to all processes
			for (int processNumber = 1; processNumber < size; processNumber++) {
				MPI.COMM_WORLD.Send(mpjPR.rankArray, 0, mpjPR.size, MPI.DOUBLE, processNumber, 1);
			}

		} else {
			mpjPR.rankArray = new double[localNumPages];
			MPI.COMM_WORLD.Recv(mpjPR.rankArray, 0, localNumPages, MPI.DOUBLE, 0, 1);
		}

		if (rank == 4) {
			 display(mpjPR.rankArray, rank);
		}

		// *** at every process, update the local copy of rankArray
		

		MPI.Finalize();

	}

}
