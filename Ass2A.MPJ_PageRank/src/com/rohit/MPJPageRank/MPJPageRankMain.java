package com.rohit.MPJPageRank;

import java.io.*;
import java.util.*;
import mpi.MPI;

public class MPJPageRankMain {

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

	public void parseArgs(String[] args) {
		inputFile = args[0];
		outputFile = args[1];
		iterations = Integer.parseInt(args[2]);
		dampingFactor = Double.parseDouble(args[3]);
	}

	// Read the input from the file and populate the adjacency matrix
	public void loadInput() throws IOException {
		System.out.println("\nRead " + inputFile);
		try {
			String line = "";

			// 'key' is not necessary, but just for better readability
			int key = 0;
			ArrayList<Integer> outLinks;

			Scanner in = new Scanner(new FileReader(inputFile));
			String[] lineStringArray;
			ArrayList<Integer> outLinksForDanglingPages = new ArrayList<Integer>();

			while (in.hasNextLine()) {
				outLinks = new ArrayList<Integer>();
				line = in.nextLine();
				lineStringArray = line.split(" ");

				// handle non int exception
				key = Integer.parseInt(lineStringArray[0]);
				outLinksForDanglingPages.add(key);
				if (lineStringArray.length > 0) {
					for (int i = 1; i < lineStringArray.length; i++) {
						outLinks.add(Integer.parseInt(lineStringArray[i]));
					}
				} else {
					// Dangling page: handled after the loop
					continue;
				}

				adjList.put(key, outLinks);
			}
			in.close();
			size = key;

			// displayAdjList();

			for (int i = 0; i <= size; i++) {
				if (adjList.get(i).size() == 0) {
					adjList.replace(i, outLinksForDanglingPages);
				}
			}

			displayAdjList();

		} catch (IOException e) {
			System.out.println("Exception " + e + "\n\nStack Trace:");
			e.printStackTrace();
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

	private void displayAdjList() {
		System.out.println("\nAdj:");
		for (int i = 0; i < adjList.size(); i++) {
			System.out.print("key: " + i + ", " + adjList.get(i).size() + " url(s): ");
			for (int j = 0; j < adjList.get(i).size(); j++) {
				System.out.print(adjList.get(i).get(j) + " ");
			}
			System.out.println();
		}
	}

	public static void main(String[] args) throws IOException {
		// Read command line args along with MPI initiation
		String inputArgs[] = MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		System.out.println("Process " + rank + " of " + size + " processes");

		// decide what u want only in rank 0 n what in all nodes
		if (rank == 0) {
			MPJPageRankMain mpjPR = new MPJPageRankMain();
			mpjPR.parseArgs(inputArgs);
			mpjPR.display();
			mpjPR.loadInput();
//			mpjPR.calculatePageRank();
//			mpjPR.printValues();
		}

		MPI.Finalize();

	}

}
