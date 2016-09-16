import java.io.*;
import java.util.*;

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
		System.out.println("\nRead " + inputFile);
		try {
			String line = "";

			// 'key' is not necessary, but just for better readability
			int key = 0;
			ArrayList<Integer> outLinks;

			// assuming there's no missing key
			Scanner in = new Scanner(new FileReader(inputFile));
			String[] lineStringArray;
			ArrayList<Integer> outLinksForDanglingPages = new ArrayList<Integer>();

			while (in.hasNextLine()) {
				outLinks = new ArrayList<Integer>();
				line = in.nextLine();
				// To-do: implement split function to avoid the intermediate
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

//			displayAdjList();

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

	/**
	 * Do fixed number of iterations and calculate the page rank values. You may
	 * keep the intermediate page rank values in a hash table.
	 */
	public void calculatePageRank() {
//		private HashMap<Integer, ArrayList<Integer>> adjList = new HashMap<Integer, ArrayList<Integer>>();
//		private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();
		HashMap<Integer, Double> nextRankValues = new HashMap<Integer, Double>();
		
		//initialize rank values
		double avg = 1.0/size;
		for(int i = 0; i <= size; i++){
			rankValues.put(i, avg);
//			nextRankValues.put(i, avg);
		}
		double constantFactor = (1-dampingFactor)/size;
		double myRankContribution = 0.0;
//		double rankContribution = 0.0;
		ArrayList<Integer> outLinks;
		
		while(iterations-- > 0){
			nextRankValues = (HashMap<Integer, Double>)rankValues.clone();
			for(int i = 0; i <= size; i++){
				outLinks = adjList.get(i);
				// My contribution towards each page
				myRankContribution = rankValues.get(i) / outLinks.size();
				
				for(int page: outLinks){
//					double temp = myRank + nextRankValues.get(page);
					nextRankValues.replace(page, myRankContribution + nextRankValues.get(page));
				}
			}
			for(int i = 0; i <= size; i++){
				double rank = constantFactor + dampingFactor * rankValues.get(i);
				rankValues.replace(i, rank);
			}
//			rankValues = (HashMap<Integer, Double>)nextRankValues.clone();
			rankValues = nextRankValues;
			
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
		System.out.println("Ranks:");
		for(int i = 0; i < size;i++){
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
		SequentialPageRank sequentialPR = new SequentialPageRank();

		sequentialPR.parseArgs(args);
//		sequentialPR.display();

		sequentialPR.loadInput();
		sequentialPR.calculatePageRank();
		sequentialPR.printValues();
	}

}
