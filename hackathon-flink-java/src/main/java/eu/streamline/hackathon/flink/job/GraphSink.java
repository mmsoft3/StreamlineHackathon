package eu.streamline.hackathon.flink.job;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import javax.swing.JLabel;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.ui.view.Viewer;

public class GraphSink<T> implements SinkFunction<T>, Serializable {

	private static final long serialVersionUID = -8879997797556342309L;
	private static Graph graph;
	private static Node rootNode;
	private static JLabel dateLabel;
	private static Properties gps;

	private static long maxMentions = 1;

	final String[] colorScale = { "rgb(165,0,38)", "rgb(215,48,39)", "rgb(244,109,67)", "rgb(253,174,97)",
			"rgb(254,224,139)", "rgb(217,239,139)", "rgb(166,217,106)", "rgb(102,189,99)", "rgb(26,152,80)",
			"rgb(0,104,55)" };

	public GraphSink(Graph graph, Node rootNode, Viewer viewer) {
		GraphSink.graph = graph;
		GraphSink.rootNode = rootNode;

		dateLabel = new JLabel("DATUM");
		dateLabel.locate(0, 10);
		viewer.getDefaultView().add(dateLabel);

		gps = new Properties();
		try {
			// load a properties file from class path, inside static method
			gps.load(FlinkJavaJob.class.getClassLoader().getResourceAsStream("gps.properties"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	@Override
	public void invoke(T arg0) throws Exception {

		if (arg0 instanceof Tuple4) {

			@SuppressWarnings("unchecked")
			Tuple4<String, Aggregate, Date, Date> data = (Tuple4<String, Aggregate, Date, Date>) arg0;
			String key = data.f0;
			Aggregate agg = data.f1;
			Date start = data.f2;
			Date end = data.f3;

			synchronized (graph) {

				String labelText = start + " - " + end;
				if (labelText.compareTo(dateLabel.getText()) != 0) {
					dateLabel.setText(start + " - " + end);

					// clear Graph
					Iterator<Edge> ei = rootNode.getEdgeIterator();
					while (ei.hasNext()) {
						Edge e = ei.next();
						if (e != null) {
							graph.removeEdge(e);

							Node node = e.getNode0();
							if (node != rootNode)
								graph.removeNode(node);
							node = e.getNode1();
							if (node != rootNode)
								graph.removeNode(node);
						}
					}
				}
				
				if (StringUtils.equalsIgnoreCase("DUMMY", key))
					return;

				// Node für KEY
				Node keyNode = graph.getNode(key);

				if (keyNode == null) {
					keyNode = graph.addNode(key);
					keyNode.setAttribute("ui.label", key);
					keyNode.setAttribute("layout.fozen");
				}

				double dlat = 0.0f;
				double dlng = 0.0f;

				if (gps.containsKey(key)) {
					String[] gpsLoc = gps.getProperty(key).split(";");
					try {
						dlat = Float.valueOf(gpsLoc[0]);
						dlng = Float.valueOf(gpsLoc[1]);
					} catch (Exception e) {
					}
				}
				keyNode.setAttribute("xy", Math.round(dlng), Math.round(dlat));

				Edge edge = keyNode.getEdgeBetween(rootNode);
				if (edge == null) {
					edge = graph.addEdge(rootNode.getId() + "-" + keyNode.getId(), rootNode, keyNode, false);
				}

				edge.setAttribute("ui.style", calcColorString(agg.avgToneAvg) +
						"size: 3px; ");
				if (key.compareTo("DUMMY") != 0)
					keyNode.setAttribute("ui.style", calcNodeSize(agg.numMentions));

			}

		}
	}

	private String calcNodeSize(long value) {

		if (value <= 0)
			value = 1;
		if (value > maxMentions)
			maxMentions = value;

		long size = Math.round(Math.log(value) * 1.5) + 5;

		return "size: " + size + "; ";
	}

	private String calcColorString(Double value) {

		return "fill-color: " + colorScale[binData(value)] + "; ";
	}

	private static int binData(Double data) {
		int bin = 0;
		if (data >= -10 && data < -7.5) {
			bin = 1;
		} else if (data >= -7.5 && data < -5) {
			bin = 2;
		} else if (data >= -5 && data < -2.5) {
			bin = 3;
		} else if (data >= -2.5 && data < 0) {
			bin = 4;
		} else if (data >= 0 && data < 1.25) {
			bin = 5;
		} else if (data >= 1.25 && data < 2.5) {
			bin = 6;
		} else if (data >= 2.5 && data < 3.75) {
			bin = 7;
		} else if (data >= 3.75 && data < 5) {
			bin = 8;
		} else if (data >= 5) {
			bin = 9;
		}
		return bin;
	};
}
