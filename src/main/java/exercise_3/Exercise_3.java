package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static Info infoMin (Info o, Info o2){
        if(o.getWeight() < o2.getWeight()){
            return o;
        } else {
            return o2;
        }
    }

    private static class VProg extends AbstractFunction3<Long,Info,Info,Info> implements Serializable {

        @Override
        public Info apply(Long vertexID, Info vertexValue, Info message) {
            System.out.println("-----(Apply-code) VProg: vertexID: " + vertexID + " with value: " + vertexValue + " and message: " + message.toString());
            if (message.getWeight() == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                return infoMin(vertexValue,message);
            }
        }

    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Info,Integer>, Iterator<Tuple2<Object,Info>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Info>> apply(EdgeTriplet<Info, Integer> triplet) {
            Tuple2<Object,Info> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Info> dstVertex = triplet.toTuple()._2();
//            System.out.println("SourceVertex: " + sourceVertex);
//            System.out.println("dstVertex: " + dstVertex);
//            System.out.println("SourceVertex_1: " + sourceVertex._1());

            boolean flag = sourceVertex._2().getWeight() == Integer.MAX_VALUE;
            Integer valueEdge =  flag ? Integer.MAX_VALUE : sourceVertex._2().getWeight() + triplet.toTuple()._3();
            String path = sourceVertex._2.getPath() + " " + dstVertex._1(); //current_path + myself_node
//            List<Object> paths = new ArrayList<>();
//            paths.add(sourceVertex._1());

            System.out.println("Starting: sendMsg (Scatter)");
            System.out.println("source: " + sourceVertex);
            System.out.println("destination: " + dstVertex);
            System.out.println("attr: " + valueEdge);
            System.out.println("path: " + path);

            if ((dstVertex._2.getWeight() <= valueEdge)) {   // source vertex value is bigger than dst vertex?
                // do nothing
                System.out.println("do nothing");
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Info>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                System.out.println("propagate source vertex value");
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Info>(triplet.dstId(), new Info(valueEdge,path))).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Info,Info,Info> implements Serializable {
        @Override
        public Info apply(Info o, Info o2) {
            System.out.println("Starting: merge (Gather)");
            System.out.println("Taking the min of " + o.getWeight() + " of path " + o.getPath());
            System.out.println("and " + o2.getWeight() + " of path " + o2.getPath());
            return infoMin(o,o2);
            //return Math.min(new Info());
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {

        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object,Info>> vertices = Lists.newArrayList(
                new Tuple2<Object,Info>(1l,new Info(0,"1")),
                new Tuple2<Object,Info>(2l,new Info()),
                new Tuple2<Object,Info>(3l,new Info()),
                new Tuple2<Object,Info>(4l,new Info()),
                new Tuple2<Object,Info>(5l,new Info()),
                new Tuple2<Object,Info>(6l,new Info())
        );

        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Info>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Info,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Info(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Info.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Info.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(
                new Info(),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new Exercise_3.VProg(),
                new Exercise_3.sendMsg(),
                new Exercise_3.merge(),
                ClassTag$.MODULE$.apply(Info.class))
            .vertices()
            .toJavaRDD()
            .foreach(v -> {
                    Tuple2<Object,Info> vertex = (Tuple2<Object,Info>) v;

                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1())+" is "+vertex._2().getWeight() + " with path " + vertex._2().getPath());
                });
    }
	
}
