package eu.streamline.hackathon.flink.job;

import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.flink.operations.GDELTInputFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.view.Viewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;

@SuppressWarnings("deprecation")
public class FlinkJavaJob {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJavaJob.class);
	static final Graph graph = new MultiGraph("embedded");
	static Date minDate = new Date();;

	@SuppressWarnings({ "serial" })
	public static void main(String[] args) throws IOException {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String pathToGDELT = params.get("path");
		final String country = params.get("country", "DEU");

		Viewer viewer = graph.display(false);
		graph.addAttribute("ui.quality");
		graph.addAttribute("ui.antialias");
		Node rootNode =  graph.addNode(country);
		rootNode.setAttribute("ui.label", country);
		final GraphSink<Tuple4<String,Aggregate,Date,Date>> graphSink = new GraphSink<Tuple4<String,Aggregate,Date,Date>>(graph, rootNode, viewer);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<GDELTEvent> source = env
			.readFile(new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT).setParallelism(1);


		source.filter(new FilterFunction<GDELTEvent>() {
			@Override
			public boolean filter(GDELTEvent gdeltEvent) throws Exception {
				//return true;
				
				return Objects.equals(gdeltEvent.actor2Code_countryCode, country)
						&& StringUtils.equals(gdeltEvent.eventRootCode, "01");
			}
		})
		.assignTimestampsAndWatermarks(
			new BoundedOutOfOrdernessTimestampExtractor<GDELTEvent>(Time.seconds(0)) {
				@Override
				public long extractTimestamp(GDELTEvent element) {
					if (element.day != null) {
						if (minDate.getTime() > element.day.getTime()) {
							minDate = element.day;
						}

						return element.day.getTime();
					} else {
						return minDate.getTime();
					}
				}
		}).keyBy(new KeySelector<GDELTEvent, String>() {
			@Override
			public String getKey(GDELTEvent gdeltEvent) throws Exception {
				if (null == gdeltEvent.actor1Code_countryCode)
					return "DUMMY";
				else
					return gdeltEvent.actor1Code_countryCode;
			}
		}).window(TumblingEventTimeWindows.of(Time.days(1))).fold(new Aggregate(),
			new FoldFunction<GDELTEvent, Aggregate>() {
				@Override
				public Aggregate fold(Aggregate agg, GDELTEvent o) throws Exception {
					agg.calcAggregates(o);
					return agg;
				}
			},
			new WindowFunction<Aggregate, Tuple4<String, Aggregate, Date, Date>, String, TimeWindow>() {
				@Override
				public void apply(String key, TimeWindow window, Iterable<Aggregate> input, Collector<Tuple4<String, Aggregate, Date, Date>> out) throws Exception {
					Iterator<Aggregate> it = input.iterator();
					Aggregate agg = it.next();
					out.collect(new Tuple4<>(key, agg, new Date(window.getStart()), new Date(window.getEnd())));

				}
		})
		.addSink(graphSink);

		try {
			env.execute("Flink Java GDELT Analyzer");
		} catch (Exception e) {
			LOG.error("Failed to execute Flink job {}", e);
		}		
	}	
}
