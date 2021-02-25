package iisg.amsterdam.wp4_links;


/**

The MIT License (MIT)

Copyright (c) 2014, 2015, 2016 Dylon Edwards <dylon.devo+github@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE

 **/


import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.liblevenshtein.collection.dictionary.SortedDawg;
import com.github.liblevenshtein.serialization.PlainTextSerializer;
import com.github.liblevenshtein.serialization.Serializer;
import com.github.liblevenshtein.transducer.Algorithm;
import com.github.liblevenshtein.transducer.Candidate;
import com.github.liblevenshtein.transducer.ITransducer;
import com.github.liblevenshtein.transducer.factory.TransducerBuilder;

import iisg.amsterdam.wp4_links.utilities.LoggingUtilities;



public class MyTransducer {

	public static final Logger lg = LogManager.getLogger(MyTransducer.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	String dictionaryPath = null;
	int maxLevDistance = 1;
	ITransducer<Candidate> transducer = null;

	public MyTransducer(String dictionaryPath, int maxLevDistance) {
		this.dictionaryPath = dictionaryPath;
		this.maxLevDistance = maxLevDistance;
		transducer = constructTransducer();
	}


	public ITransducer<Candidate> constructTransducer()
	{
		long startTime = System.currentTimeMillis();
		LOG.logDebug("constructTransducer", "START: Constructing transducer for dictionary located at: " + dictionaryPath);
		SortedDawg dictionary;
		ITransducer<Candidate> transducer = null;
		Path pathDictionary = Paths.get(dictionaryPath);
		try { 
			final InputStream stream = Files.newInputStream(pathDictionary); 
			final Serializer serializer = new PlainTextSerializer(false);
			dictionary = serializer.deserialize(SortedDawg.class, stream);
			transducer = new TransducerBuilder()
					.dictionary(dictionary)
					.algorithm(Algorithm.STANDARD)
					.defaultMaxDistance(maxLevDistance)
					.includeDistance(true)
					.build();
		} catch (Exception e) {
			LOG.logError("constructTransducer", "Error while constructing transducer");
			e.printStackTrace();
		}
		LOG.logDebug("constructTransducer", LOG.outputTotalRuntime("Constructing transducer for dictionary located at: " + dictionaryPath, startTime, false));
		return transducer;
	}
	
	

}
