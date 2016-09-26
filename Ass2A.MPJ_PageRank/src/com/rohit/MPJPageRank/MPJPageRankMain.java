package com.rohit.MPJPageRank;

import java.io.*;
import java.util.*;

import mpi.MPI;

public class MPJPageRankMain {

	// adjacency list read from input file as a string per page
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

	// Print top 10 pagerank values in descending order.
	public void printValues() throws IOException {
		// To maintain page numbers
		int ind[] = new int[size];
		for (int i = 0; i < size; i++) {
			ind[i] = i;
		}

		// Sort the ranks, keeping track of their page numbers
		for (int i = 0; i < size; i++) {
			for (int j = 1; j < size; j++) {
				if (rankArray[j - 1] < rankArray[j]) {
					double t = rankArray[j - 1];
					rankArray[j - 1] = rankArray[j];
					rankArray[j] = t;
					int t2 = ind[j - 1];
					ind[j - 1] = ind[j];
					ind[j] = t2;
				}
			}
		}

		StringBuffer outString = new StringBuffer();
		outString.append("Number of Iterations = " + iterations + "\n");
		for (int i = 0; i < size && i < 10; i++) {
			outString.append("Page: " + ind[i] + ": " + rankArray[i] + "\n");
		}
		// Print to console
		System.out.println(outString);

		// Write to outPutFile
		File fileHandle = new File(outputFile);
		if (!fileHandle.exists()) {
			fileHandle.createNewFile();
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileHandle.getAbsoluteFile()));
		bw.write(outString.toString());
		bw.close();
	}

	public static void main(String[] args) throws IOException {
		// Read command line args along with MPI initiation
		String inputArgs[] = MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();

		int numPages = 0;
		MPJPageRankMain mpjPR = new MPJPageRankMain();
		if (rank == 0) {
			mpjPR.parseArgs(inputArgs);
			numPages = mpjPR.loadInput(rank);
		}

		// send damping factor
		double d[] = new double[1];
		if (rank == 0) {
			d[0] = mpjPR.dampingFactor;
			for (int i = 1; i < size; i++) {
				MPI.COMM_WORLD.Send(d, 0, 1, MPI.DOUBLE, i, 1);
			}
		} else {
			MPI.COMM_WORLD.Recv(d, 0, 1, MPI.DOUBLE, 0, 1);
			mpjPR.dampingFactor = d[0];
		}

		// send number of iterations
		int its[] = new int[1];
		if (rank == 0) {
			its[0] = mpjPR.iterations;
			for (int i = 1; i < size; i++) {
				MPI.COMM_WORLD.Send(its, 0, 1, MPI.INT, i, 1);
			}
		} else {
			MPI.COMM_WORLD.Recv(its, 0, 1, MPI.INT, 0, 1);
			mpjPR.iterations = its[0];
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

		if (rank == 0) {
			// send adj list
			for (int processNumber = 1; processNumber < size; processNumber++) {
				int pagesFrom = processNumber * remoteChunkSize;
				if (localChunkSize - remoteChunkSize > 0) {
					pagesFrom += localChunkSize - remoteChunkSize;
				}
				int pagesTo = pagesFrom + remoteChunkSize;
				for (int page = pagesFrom; page < pagesTo; page++) {
					int l = mpjPR.adjListOfStrings.get(page).length();
					MPI.COMM_WORLD.Send(mpjPR.adjListOfStrings.get(page).toCharArray(), 0, l, MPI.CHAR, processNumber,
							1);
				}
			}
			
			/*
			 * done distributing the adjacency list, remove all but
			 * <localchunksize> elements for rank 0
			 */
			ArrayList<String> tempAdjStrings = new ArrayList<String>();
			for (int i = 0; i < localChunkSize; i++) {
				tempAdjStrings.add(mpjPR.adjListOfStrings.get(i));
			}
			mpjPR.adjListOfStrings = tempAdjStrings;

		} else {
			for (int page = 0; page < localChunkSize; page++) {
				char[] pageOutLinks = new char[localNumPages];
				MPI.COMM_WORLD.Recv(pageOutLinks, 0, localNumPages, MPI.CHAR, 0, 1);
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < pageOutLinks.length; i++) {
					sb.append(pageOutLinks[i]);
				}
				mpjPR.adjListOfStrings.add(sb.toString());
			}
		}

		// prepare the all pages string
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < mpjPR.size; i++) {
			sb.append(" " + i);
		}

		// append to dangling pages
		for (int i = 0; i < mpjPR.adjListOfStrings.size(); i++) {
			if (mpjPR.adjListOfStrings.get(i).length() < 2) {
				mpjPR.adjListOfStrings.set(i, mpjPR.adjListOfStrings.get(i) + sb);
			}
		}

		// convert adjListOfStrings to adjList
		for (int i = 0; i < mpjPR.adjListOfStrings.size(); i++) {
			String temp[] = mpjPR.adjListOfStrings.get(i).split(" ");
			ArrayList<Integer> outLinks = new ArrayList<Integer>();
			for (int j = 1; j < temp.length; j++) {
				outLinks.add(Integer.parseInt(temp[j].trim()));
			}
			mpjPR.adjList.put(Integer.parseInt(temp[0].trim()), outLinks);
		}

		boolean firstRun = true;

		int iterations = mpjPR.iterations;
		while (iterations-- > 0) {
			if (rank == 0) {
				// *********** generate rank array **********
				if (firstRun) {
					firstRun = false;
					mpjPR.rankArray = new double[mpjPR.size];
					double avg = 1.0 / mpjPR.size;
					for (int i = 0; i < mpjPR.size; i++) {
						mpjPR.rankArray[i] = avg;
					}
				}
				// send rank array to all processes
				for (int processNumber = 1; processNumber < size; processNumber++) {
					MPI.COMM_WORLD.Send(mpjPR.rankArray, 0, mpjPR.size, MPI.DOUBLE, processNumber, 1);
				}

			} else {
				mpjPR.rankArray = new double[localNumPages];
				MPI.COMM_WORLD.Recv(mpjPR.rankArray, 0, localNumPages, MPI.DOUBLE, 0, 1);
			}

			// *** at every process, update the local copy of rankArray
			mpjPR.size = localNumPages; // do this up
			double localRanks[] = new double[localNumPages];
			localRanks = Arrays.copyOf(mpjPR.rankArray, localNumPages);
			localRanks = calculateLocalRanks(localRanks, mpjPR);

			// *** send locally calculated ranks back to parent process
			if (rank != 0) {
				MPI.COMM_WORLD.Send(localRanks, 0, mpjPR.size, MPI.DOUBLE, 0, 1);
			} else {
				mpjPR.rankArray = new double[mpjPR.size];
				double[] remoteLocalRanks = new double[mpjPR.size];
				for (int processNumber = 1; processNumber < size; processNumber++) {
					MPI.COMM_WORLD.Recv(remoteLocalRanks, 0, mpjPR.size, MPI.DOUBLE, processNumber, 1);
					// aggregate ranks at parent process
					for (int i = 0; i < mpjPR.size; i++) {
						mpjPR.rankArray[i] += remoteLocalRanks[i];
					}
				}
				// update parent process's values to rankArray
				for (int i = 0; i < mpjPR.size; i++) {
					mpjPR.rankArray[i] += localRanks[i];
				}
			}
		}

		if (rank == 0) {
			mpjPR.printValues();
		}
		MPI.Finalize();

	}

	private static double[] calculateLocalRanks(double[] localRanks, MPJPageRankMain mpjPR) {
		double[] nextLocalRanks = new double[mpjPR.size];
		double constantFactor = (1 - mpjPR.dampingFactor) / mpjPR.size;
		double myRankContribution = 0.0;
		Iterator it = mpjPR.adjList.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			int pageNumber = (int) pair.getKey();
			ArrayList<Integer> outLinks = mpjPR.adjList.get(pageNumber);
			// System.out.println(pageNumber + ": " + outLinks);

			myRankContribution = localRanks[pageNumber] / outLinks.size();

			for (int page : outLinks) {
				nextLocalRanks[page] = myRankContribution + nextLocalRanks[page];
			}

		}

		for (int i = 0; i < mpjPR.size; i++) {
			if (nextLocalRanks[i] > 0) {
				nextLocalRanks[i] = constantFactor + mpjPR.dampingFactor * nextLocalRanks[i];
			}
		}

		return nextLocalRanks;

	}

}
