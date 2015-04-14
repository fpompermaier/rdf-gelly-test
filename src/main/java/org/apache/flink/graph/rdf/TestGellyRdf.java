package org.apache.flink.graph.rdf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;

public class TestGellyRdf {

	private static final String idPers1 = "http://test/people/1";
	private static final String idPeriod1 = "http://test/period/1";
	private static final String idPeriod2 = "http://test/period/2";
	private static final String idLoc1 = "http://test/location/1";//non-existing entity
	private static final String idEvent1 = "http://test/event/1";
	private static final String idEvent2 = "http://test/event/2";
	private static final String idEvent3 = "http://test/event/3";
	private static final String idEvent4 = "http://test/event/4";
	
	private static final String testNs = "http://www.test.com/test#";
	private static final String graphUri = "http://my.graph.com";
	

	private static DateTimeParser[] parsers = {
		DateTimeFormat.forPattern("yyyyMMdd").getParser(),
		DateTimeFormat.forPattern("dd-MM-yyyy").getParser(),
		DateTimeFormat.forPattern("dd/MMyyyy").getParser(),
		DateTimeFormat.forPattern("dd/MM/yyyy").getParser(),
		DateTimeFormat.forPattern("yyyy/MM/dd").getParser(),
		DateTimeFormat.forPattern("yyyy-MM-dd HH").getParser(),
		DateTimeFormat.forPattern("yyyy-MM-dd").getParser() };
	private static DateTimeFormatter dateFormatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();
	
	public static void main(String[] args) throws Exception {
		// initialize a new Collection-based execution environment
	    final ExecutionEnvironment env = new CollectionEnvironment();
	    List<String> serializedGraphs = Arrays.asList(
	    		serializedPeriod1,serializedPeriod2,serializedPerson, serializedMeeting1, serializedMeeting2, serializedTicketEvent1, serializedTicketEvent2);
	    DataSet<String> datasetGraphs = env.fromCollection(serializedGraphs);

		// set up the execution environment
		DataSet<Edge<String, String>> edges = datasetGraphs.flatMap(new EdgeExtractor());
		DataSet<Vertex<String, String>> initialVertices = datasetGraphs.map(new VertexExtractor());
		Graph<String, String, String> rdfGraph = Graph.fromDataSet(initialVertices, edges, env);
		
		//TODO 1 compute subgraph for Person
		//TODO 1.1 foreach Person's ImportantMeetings find those having eventDate in (VacationPeriod.startDate, VacationPeriod,startDate) and emit a new quad 
		// --> <idEvent1, happensDuringVacation, idPeriod1, someGraphName>
		
		//TODO 2 compute subgraph for PayedTicket but not include Person-data in the results, only ImportantMeeting
		//TODO 2.1 output a dataProperty 'totalCost' as the sum of all eventCost
		// --> <idEvent2, totalCost, 50, someGraphName>
		
		DataSet<Vertex<String, String>> result = rdfGraph.getVertices();//TODO change

		// emit result
		result.print();//print newly computed quads
		
		// kick off execution.
		env.execute();
	}
	private static DateTime getDate(String ss, String sp, DatasetGraph dsg) {
		Node s = NodeFactory.createURI(ss);
		Node p = NodeFactory.createURI(sp); 
		DateTime retDate = null;
		Iterator<Quad> it = dsg.find(null, s, p, null);
		while (it.hasNext()) {
			Quad quad = (Quad) it.next();
			String dateStr = quad.getObject().getLiteralValue().toString();
			try {
				retDate = dateFormatter.parseDateTime(dateStr);
			} catch (IllegalArgumentException e) {
				System.err.println(e.getMessage());
			}
		}
		return retDate;
	}
	//---------------------------------------------------------------------------------------------------------------------
	private static String serializedPeriod1 = 
		 "<"+idPeriod1+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+testNs+"VacationPeriod> <"+graphUri+"> .\n"
		+"<"+idPeriod1+"> <"+testNs+"startDate> \"2011-01-01\"^^<http://www.w3.org/2001/XMLSchema#date>  <"+graphUri+"> .\n"
	    +"<"+idPeriod1+"> <"+testNs+"endDate> \"2011-12-28\"^^<http://www.w3.org/2001/XMLSchema#date>  <"+graphUri+"> .\n"
		+"<"+idPeriod1+"> <"+testNs+"hasPerson> <"+idPers1+">  <"+graphUri+"> .\n"
		+"<"+idPeriod1+"> <"+testNs+"vacationType> \"relax\"  <"+graphUri+"> .\n";
	private static String serializedPeriod2 = 	
		 "<"+idPeriod2+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+testNs+"VacationPeriod>  <"+graphUri+"> .\n"
		+"<"+idPeriod2+"> <"+testNs+"startDate> \"2008-02-02\"^^<http://www.w3.org/2001/XMLSchema#date>  <"+graphUri+"> .\n"
		+"<"+idPeriod2+"> <"+testNs+"endDate> \"2010-12-28\"^^<http://www.w3.org/2001/XMLSchema#date>  <"+graphUri+"> .\n"
		+"<"+idPeriod2+"> <"+testNs+"hasPerson> <"+idPers1+">  <"+graphUri+"> .\n"
		+"<"+idPeriod2+"> <"+testNs+"vacationType> \"relax\"  <"+graphUri+"> .\n";
	private static String serializedPerson = 			
		 "<"+idPers1+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+testNs+"Person>  <"+graphUri+"> .\n"
		+"<"+idPers1+"> <http://www.w3.org/2000/01/rdf-schema#label> \"Test person 1\"  <"+graphUri+"> .\n"
		+"<"+idPers1+"> <"+testNs+"name> \"John\"  <"+graphUri+"> .\n"
		+"<"+idPers1+"> <"+testNs+"lastname> \"Smith\"  <"+graphUri+"> .\n"
		+"<"+idPers1+"> <"+testNs+"hasBirthPlace> <"+idLoc1+">  <"+graphUri+"> .\n";
	private static String serializedMeeting1 = 
		 "<"+idEvent1+"> <"+testNs+"year> \"2011\"  <"+graphUri+"> .\n"
		+"<"+idEvent1+"> <"+testNs+"eventDate> \"2011-11-10\"^^<http://www.w3.org/2001/XMLSchema#date>  <"+graphUri+"> .\n"
		+"<"+idEvent1+"> <"+testNs+"involvedPerson> <"+idPers1+">  <"+graphUri+"> .\n"
		+"<"+idEvent1+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+testNs+"ImportantMeeting>  <"+graphUri+"> .\n"
		+"<"+idEvent2+"> <http://www.w3.org/2000/01/rdf-schema#label> \"Important meeting 1\"  <"+graphUri+"> .\n";
	private static String serializedMeeting2 = 
		 "<"+idEvent2+"> <"+testNs+"year> \"2013\"  <"+graphUri+"> .\n"
		+"<"+idEvent2+"> <"+testNs+"eventDate> \"2013-01-10\"^^<http://www.w3.org/2001/XMLSchema#date>  <"+graphUri+"> .\n"
		+"<"+idEvent2+"> <"+testNs+"involvedPerson> <"+idPers1+">  <"+graphUri+"> .\n"
		+"<"+idEvent2+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+testNs+"ImportantMeeting>  <"+graphUri+"> .\n"
		+"<"+idEvent2+"> <http://www.w3.org/2000/01/rdf-schema#label> \"Important meeting 2\"  <"+graphUri+"> .\n";
	private static String serializedTicketEvent1 = 
		 "<"+idEvent3+"> <"+testNs+"eventCost> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer>  <"+graphUri+"> .\n"
		+"<"+idEvent3+"> <"+testNs+"hasMeeting <"+idEvent2+">  <"+graphUri+"> .\n"
		+"<"+idEvent3+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+testNs+"PayedTicket>  <"+graphUri+"> .\n";
	private static String serializedTicketEvent2 = 
		 "<"+idEvent4+"> <"+testNs+"eventCost> \"20\"^^<http://www.w3.org/2001/XMLSchema#integer>  <"+graphUri+"> .\n"
		+"<"+idEvent4+"> <"+testNs+"hasMeeting <"+idEvent2+">  <"+graphUri+"> .\n"
		+"<"+idEvent4+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+testNs+"PayedTicket>  <"+graphUri+"> .\n";

	public static DatasetGraph deserializeDatasetGraph(byte[] serializedGraph, Lang lang) {
		DatasetGraph ret = DatasetGraphFactory.createMem();
		InputStream is = new ByteArrayInputStream(serializedGraph);
		RDFDataMgr.read(ret, is, lang);
		try {
			is.close();
		} catch (IOException e) {
			// do nothing
		}
		return ret;
	}

	private static final class VertexExtractor implements MapFunction<String, Vertex<String, String>> {
		private Vertex<String, String> vertex = new Vertex<>();

		@Override
		public Vertex<String, String> map(
				String serializedGraph) throws Exception {
			DatasetGraph dsg = deserializeDatasetGraph(serializedGraph.getBytes(), Lang.NQUADS);
			Iterator<Quad> it = dsg.find();
			while (it.hasNext()) {
				Quad q = it.next();
				vertex.f0 = q.getSubject().getURI();
				break;
			}
			dsg.close();
			vertex.f1 = serializedGraph;
			return vertex;
		}
	}

	private static final class EdgeExtractor implements FlatMapFunction<String, Edge<String, String>> {
		private final Edge<String, String> edge = new Edge<>();

		@Override
		public void flatMap(String serializedGraph,
				Collector<Edge<String, String>> out) throws Exception {
			DatasetGraph dsg = deserializeDatasetGraph(serializedGraph.getBytes(), Lang.NQUADS);
			Iterator<Quad> it = dsg.find();
			while (it.hasNext()) {
				Quad q = it.next();
				edge.f0 = q.getSubject().getURI();
				if (!(q.getObject() instanceof Node_URI))
					continue;
				edge.f1 = q.getObject().getURI();
				edge.f2 = q.getPredicate().getURI();
				out.collect(edge);

			}
			dsg.close();
		}
	}
	
}
