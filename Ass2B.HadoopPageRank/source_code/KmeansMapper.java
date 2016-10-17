package edu.iu.km;

import edu.iu.harp.example.DoubleArrPlus;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.CollectiveMapper;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class KmeansMapper extends CollectiveMapper<String, String, Object, Object> {

	private int numofIterations;
	private int numCenPartitions;
	private int numCentroids;
	private int jobID;
	private int vectorSize;
	private int numMappers;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		LOG.info("begin setup : " + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		long startTime = System.currentTimeMillis();
		Configuration configuration = context.getConfiguration();
		jobID = configuration.getInt(KMeansConstants.JOB_ID, 0);
		numMappers = configuration.getInt(KMeansConstants.NUM_MAPPERS, 10);
		numCentroids = configuration.getInt(KMeansConstants.NUM_CENTROIDS, 20);
		numCenPartitions = numMappers;
		vectorSize = configuration.getInt(KMeansConstants.VECTOR_SIZE, 20);
		numofIterations = configuration.getInt(KMeansConstants.NUM_ITERATONS, 1);

		long endTime = System.currentTimeMillis();
		LOG.info("config (ms) :" + (endTime - startTime));
	}

	public void mapCollective(KeyValReader reader, Context context) throws IOException, InterruptedException {
		LOG.info("Start kmean collective mapper ...");
		long startTime = System.currentTimeMillis();
		List<String> pointFiles = new ArrayList<String>();
		while (reader.nextKeyValue()) {
			String currentKey = reader.getCurrentKey();
			String currentValue = reader.getCurrentValue();
			LOG.info("Key: " + currentKey + ", Value: " + currentValue);
			pointFiles.add(currentValue);
		}
		Configuration conf = context.getConfiguration();
		runKmeans(pointFiles, conf, context);
		LOG.info("Total times in master view: " + (System.currentTimeMillis() - startTime));
	}

	public void broadcastCentroids(Table<DoubleArray> cenTable) throws IOException {
		// broadcast centroids to all other worker nodes
		boolean successFlag = false;
		try {
			successFlag = broadcast("main", "broadcast-centroids", cenTable, this.getMasterID(), false);
		} catch (Exception e) {
			LOG.error("Failed to broadcast ...", e);
		}
		if (successFlag == false) {
			throw new IOException("Failed to broadcast ...");
		}
	}

	public void kmeanComputation(Table<DoubleArray> newCenTable, Table<DoubleArray> currentCenTable,
			ArrayList<DoubleArray> dataPoints) {

		for (DoubleArray aPoint : dataPoints) {
			// For each data point, find the nearest centroid
			double minDist = Double.MAX_VALUE;
			double tempDist = 0;

			int nearestPartitionID = -1;
			for (Partition aCentroidPartition : currentCenTable.getPartitions()) {
				DoubleArray aCentroid = (DoubleArray) aCentroidPartition.get();
				tempDist = calcEuclideanDistance(aPoint, aCentroid, vectorSize);
				if (tempDist < minDist) {
					minDist = tempDist;
					nearestPartitionID = aCentroidPartition.id();
				}
			}

			// For a certain data point, found the nearest centroid. Add it to a
			// partition in the new cenTable.
			double[] partial = new double[vectorSize + 1];
			for (int j = 0; j < vectorSize; j++) {
				partial[j] = aPoint.get()[j];
			}
			partial[vectorSize] = 1;

			// Create new partition for the centroid table if nearestPartitionID
			// is null
			if (newCenTable.getPartition(nearestPartitionID) == null) {
				Partition<DoubleArray> tmpAp = new Partition<DoubleArray>(nearestPartitionID,
						new DoubleArray(partial, 0, vectorSize + 1));
				newCenTable.addPartition(tmpAp);
			} else {
				Partition<DoubleArray> apInCenTable = newCenTable.getPartition(nearestPartitionID);
				for (int i = 0; i < vectorSize + 1; i++) {
					apInCenTable.get().get()[i] += partial[i];
				}
			}
		}
	}

	public void runKmeans(List<String> fileNames, Configuration conf, Context context) throws IOException {

		// load data points
		ArrayList<DoubleArray> dataPoints = loadData(fileNames, vectorSize, conf);

		// Load centroids. For every partition in the centroid table, we will
		// use the last element to store the number of points which are
		// clustered to the particular partitionID
		Table<DoubleArray> cenTable = new Table<>(0, new DoubleArrPlus());
		if (this.isMaster() == true) {
			loadCentroids(cenTable, vectorSize, conf.get(KMeansConstants.centroid_file), conf);
		}

		System.out.println("After loading centroids ...");
		printTable(cenTable);

		broadcastCentroids(cenTable);

		System.out.println("After brodcasting centroids ...");
		printTable(cenTable);

		Table<DoubleArray> newCenTable = new Table<>(0, new DoubleArrPlus());

		System.out.println("Iteraton No." + jobID);

		// Compute new partial centroid table using previousCentroid Table and
		// Data points
		kmeanComputation(newCenTable, cenTable, dataPoints);

		allreduce("main", "allreduce_" + jobID, newCenTable);

		System.out.println("After allreduce");
		printTable(newCenTable);

		// New centroids
		calculateCentroids(newCenTable);

		if (this.isMaster() == true) {
			updateCentroidFile(newCenTable, conf, conf.get(KMeansConstants.centroid_file));

			// Last iteration
			if (jobID == numofIterations - 1) {
				outputCentroids(newCenTable, conf, context);
			}
		}

	}

	// After every iteration, update centroid file
	public void updateCentroidFile(Table<DoubleArray> cenTable, Configuration conf, String cFileName)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path(cFileName), true);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String output = "";
		for (Partition<DoubleArray> part : cenTable.getPartitions()) {
			double result[] = part.get().get();
			for (int i = 0; i < vectorSize; i++)
				output += result[i] + "\t";
			output += "\n";
		}

		try {
			bw.write(output);
			bw.flush();
			bw.close();
			System.out.println("Written updated centroids " + "to file " + KMeansConstants.centroid_file);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void outputCentroids(Table<DoubleArray> cenTable, Configuration conf, Context context) {
		String output = "";
		for (Partition<DoubleArray> part : cenTable.getPartitions()) {
			double result[] = part.get().get();
			for (int i = 0; i < vectorSize; i++) {
				output += result[i] + "\t";
			}
			output += "\n";
		}

		try {
			context.write(null, new Text(output));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Calculate mean to compute new updated centroids
	// After allreduce operation, compute mean on centroid table to calculate
	// new centroids.

	public void calculateCentroids(Table<DoubleArray> cenTable) {
		for (Partition<DoubleArray> partialCenTable : cenTable.getPartitions()) {
			double[] doubles = partialCenTable.get().get();
			for (int h = 0; h < vectorSize; h++) {
				doubles[h] /= doubles[vectorSize];
			}

			doubles[vectorSize] = 0;
		}
		System.out.println("after calculate new centroids");
		printTable(cenTable);
	}

	// calculate Euclidean distance.
	private double calcEuclideanDistance(DoubleArray aPoint, DoubleArray aCentroidPoint, int vectorSize) {
		double euclideanDistanceSquare = 0;
		for (int i = 0; i < vectorSize; i++) {
			euclideanDistanceSquare += Math.pow(aPoint.get()[i] - aCentroidPoint.get()[i], 2);
		}
		return Math.sqrt(euclideanDistanceSquare);
	}

	// Load centroids from HDFS
	public void loadCentroids(Table<DoubleArray> cenTable, int vectorSize, String cFileName,
			Configuration configuration) throws IOException {
		Path cPath = new Path(cFileName);
		FileSystem fs = FileSystem.get(configuration);
		FSDataInputStream in = fs.open(cPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = "";
		String[] splitLine = null;
		int partitionId = 0;
		while ((line = br.readLine()) != null) {
			splitLine = line.split("\\s+");
			if (splitLine.length != vectorSize) {
				System.out.println("Errors while loading the centroids .");
				System.exit(-1);
			} else {
				double[] aCen = new double[vectorSize + 1];

				for (int i = 0; i < vectorSize; i++) {
					aCen[i] = Double.parseDouble(splitLine[i]);
				}
				aCen[vectorSize] = 0;
				Partition<DoubleArray> ap = new Partition<DoubleArray>(partitionId,
						new DoubleArray(aCen, 0, vectorSize + 1));
				cenTable.addPartition(ap);
				partitionId++;
			}
		}
	}

	// load data form HDFS
	public ArrayList<DoubleArray> loadData(List<String> fileNames, int vectorSize, Configuration conf)
			throws IOException {
		ArrayList<DoubleArray> data = new ArrayList<DoubleArray>();
		for (String filename : fileNames) {
			FileSystem fs = FileSystem.get(conf);
			Path dPath = new Path(filename);
			FSDataInputStream in = fs.open(dPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = "";
			String[] splitLine = null;
			while ((line = br.readLine()) != null) {
				splitLine = line.split("\\s+");

				if (splitLine.length != vectorSize) {
					System.out.println("Errors while loading data.");
					System.exit(-1);
				} else {
					double[] aDataPoint = new double[vectorSize];

					for (int i = 0; i < vectorSize; i++) {
						aDataPoint[i] = Double.parseDouble(splitLine[i]);
					}
					DoubleArray da = new DoubleArray(aDataPoint, 0, vectorSize);
					data.add(da);
				}
			}
		}
		return data;
	}

	public void printTable(Table<DoubleArray> dataTable) {
		for (Partition<DoubleArray> ap : dataTable.getPartitions()) {
			double res[] = ap.get().get();
			System.out.print("ID: " + ap.id() + ":");
			for (int i = 0; i < res.length; i++) {
				System.out.print(res[i] + "\t");
			}
			System.out.println();
		}
	}
}