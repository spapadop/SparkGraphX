package exercise_4;

import com.clearspring.analytics.util.Lists;
import exercise_2.Exercise_2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.desc;


public class Exercise_4 {

	public static java.util.List<Row> readFile(String path) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line;

		java.util.List<Row> list = new ArrayList<Row>();
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split("\t");
			list.add(RowFactory.create(tokens[0], tokens[1]));
		}

		return list;
	}


	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws IOException {
		java.util.List<Row> vertices_list = readFile("src/main/resources/wiki-vertices.txt");
		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build()),
		});
		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		// edges creation
		java.util.List<Row> edges_list = readFile("src/main/resources/wiki-edges.txt");
		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);
		GraphFrame gf = GraphFrame.apply(vertices,edges);

		GraphFrame results = gf.pageRank().resetProbability(0.15).maxIter(1).run();
		System.out.println(results.vertices());
		for(String s: results.vertexColumns()){
            System.out.println(s);
        }

        results.vertices().select("id", "title", "pagerank").orderBy(desc("pagerank")).limit(10).show();
		//results.vertices().select("id,title,pagerank").orderBy("pagerank").col("pagerank").desc();
		//
        System.out.println(results);
	}
	
}
