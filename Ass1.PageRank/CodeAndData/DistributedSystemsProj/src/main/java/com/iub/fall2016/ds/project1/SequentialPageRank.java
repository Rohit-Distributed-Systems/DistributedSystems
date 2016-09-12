package com.iub.fall2016.ds.project1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SequentialPageRank {
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
	private Set<Integer> allSource = new HashSet<Integer>();
	// adjacency matrix read from file
	private HashMap<Integer, ArrayList<Integer>> incomingAdj = new HashMap<Integer, ArrayList<Integer>>();

	private HashMap<Integer, Double> baseRank = new HashMap<Integer, Double>();

	private StringBuffer finalOutput = new StringBuffer();

	/**
	 * Parse the command line arguments and update the instance variables.
	 * Command line arguments are of the form <input_file_name>
	 * <output_file_name> <num_iters> <damp_factor>
	 *
	 * @param args
	 *            arguments
	 */
	public void parseArgs(String[] args) {
		inputFile = args[0];
		outputFile = args[1];
		iterations = Integer.parseInt(args[2]);
		dampingFactor = Double.parseDouble(args[3]);
	}

	/**
	 * Read the input from the file and populate the adjacency matrix
	 *
	 * The input is of type
	 *
	 * 0 1 2 2 1 3 0 1 4 1 3 5 5 1 4 6 1 4 7 1 4 8 1 4 9 4 10 4 The first value
	 * in each line is a URL. Each value after the first value is the URLs
	 * referred by the first URL. For example the page represented by the 0 URL
	 * doesn't refer any other URL. Page represented by 1 refer the URL 2.
	 *
	 * @throws java.io.IOException
	 *             if an error occurs
	 */
	public void loadInput() throws IOException {

		boolean isSourceLink = true;
		int source = 0;
		double initialRank = 0.0;
		ArrayList<Integer> linkList = new ArrayList<Integer>();
		ArrayList<Integer> incomingLinkList = new ArrayList<Integer>();
		String line = null;
		String[] lineString = null;

		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(inputFile)), Charset.forName("UTF-8")));
		int c = 0;
		while ((line = reader.readLine()) != null) {
			char character = (char) c;
			lineString = line.split(" ");

			for (int i = 0; null != lineString && i < lineString.length; i++) {

				if (i == 0) {

					source = Integer.parseInt(lineString[i]);
					adjList.put(source, linkList);
					allSource.add(source);
				} else {

					linkList.add(Integer.parseInt(lineString[i]));
				}
			}

			linkList = new ArrayList<Integer>();

			// Do something with your character
		}

		// Set initial Pagerank

		Iterator it = adjList.entrySet().iterator();
		initialRank = 1.0 / adjList.size();

		ArrayList<Integer> allSourceList = new ArrayList<Integer>(allSource);
		double tempVal = 0.0;

		while (it.hasNext()) {

			Map.Entry pair = (Map.Entry) it.next();

			linkList = (ArrayList<Integer>) pair.getValue();

			if (0 == linkList.size()) {
				adjList.put((Integer) pair.getKey(), allSourceList);
				linkList = allSourceList;
			}

			rankValues.put((Integer) pair.getKey(), initialRank);

			for (int val : linkList) {

				tempVal = 0.0;

				if (baseRank.containsKey(val))
					tempVal = baseRank.get(val);

				tempVal += initialRank / linkList.size();

				baseRank.put(val, tempVal);
			}

		}

	}

	/**
	 * Do fixed number of iterations and calculate the page rank values. You may
	 * keep the intermediate page rank values in a hash table.
	 */
	public void calculatePageRank() {
		Iterator it = null;
		Map.Entry pair = null;
		List<Integer> tempLinks = null;
		int source = 0, bufInt = 0;
		ArrayList<Integer> allSourceList = new ArrayList<Integer>(allSource);
		double tempValue = 0.0;
		double initialRank = 0.0;

		double tempPageRank = 0.0;

		for (int i = 0; i < iterations; i++) {

			it = adjList.entrySet().iterator();

			while (it.hasNext()) {

				pair = (Map.Entry) it.next();

				source = (Integer) pair.getKey();

				tempPageRank = baseRank.get(source);

				tempPageRank += (1 - dampingFactor) * 1.0 / adjList.size();

				rankValues.put(source, tempPageRank);

				tempPageRank = 0.0;

			}

			it = adjList.entrySet().iterator();

			baseRank = new HashMap<Integer, Double>();
			while (it.hasNext()) {

				pair = (Map.Entry) it.next();

				tempLinks = (ArrayList<Integer>) pair.getValue();

				initialRank = rankValues.get(pair.getKey());

				for (int val : tempLinks) {

					tempValue = 0.0;

					if (baseRank.containsKey(val))
						tempValue = baseRank.get(val);

					tempValue += initialRank / tempLinks.size();

					baseRank.put(val, tempValue);
				}

			}
		}
	}

	/**
	 * Print the pagerank values. Before printing you should sort them according
	 * to decreasing order. Print all the values to the output file. Print only
	 * the first 10 values to console.
	 *
	 * @throws IOException
	 *             if an error occurs
	 */
	public void printValues() throws IOException {

		Map<Integer, Double> sortedMap = sortByComparator(rankValues, false);
		Iterator it = sortedMap.entrySet().iterator();
		Map.Entry pair = null;

		int count = 0;

		finalOutput.append("\nTop 10 Page Ranks: \n");
		while (it.hasNext() && count < 10) {

			count++;
			pair = (Map.Entry) it.next();

			finalOutput.append(pair.getKey());
			finalOutput.append(" : ");
			finalOutput.append(pair.getValue());
			finalOutput.append("\n");
			System.out.println(pair.getKey() + " : " + pair.getValue());
		}
		
		writeToFile();

	}

	public void writeToFile() {

		FileWriter fileWriter = null;
		BufferedWriter bufferedWriter = null;

		try {
			fileWriter = new FileWriter(outputFile);
			bufferedWriter = new BufferedWriter(fileWriter);

			bufferedWriter.write(finalOutput.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			/*try {
				if (null != fileWriter)
					fileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/

			try {
				if (null != bufferedWriter)
					bufferedWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void display() {

		finalOutput.append("Iterations: ");
		finalOutput.append(iterations);
		System.out.println("input: " + inputFile);
		System.out.println("output: " + outputFile);
		System.out.println("iterations: " + iterations);
		System.out.println("dampingFactor: " + dampingFactor);
	}

	public static void main(String[] args) throws IOException {

		long start_time = System.nanoTime();
		SequentialPageRank sequentialPR = new SequentialPageRank();

		sequentialPR.parseArgs(args);
		sequentialPR.display();

		sequentialPR.loadInput();
		sequentialPR.calculatePageRank();
		sequentialPR.printValues();
		long end_time = System.nanoTime();

		System.out.println(((end_time - start_time) / 1e6));
	}

	private static Map<Integer, Double> sortByComparator(Map<Integer, Double> unsortMap, final boolean order) {

		List<Entry<Integer, Double>> list = new LinkedList<Entry<Integer, Double>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<Integer, Double>>() {
			public int compare(Entry<Integer, Double> o1, Entry<Integer, Double> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		// Maintaining insertion order with the help of LinkedList
		Map<Integer, Double> sortedMap = new LinkedHashMap<Integer, Double>();
		for (Entry<Integer, Double> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

}
