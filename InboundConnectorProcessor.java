/*
 * Copyright 2021 Metro Bank. All rights reserved.
 */

package com.metrobank.payments.sepa.inbound.connector.processor;

import com.metrobank.payments.sepa.inbound.connector.topology.AvroConversionTransformer;
import com.metrobank.payments.sepa.inbound.connector.topology.InvalidXmlTransformer;
import com.metrobank.payments.sepa.inbound.connector.util.DataUtil;
import com.metrobank.payments.sepa.internal.RawData;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Base64;
import java.util.function.Function;

@Service
@Log4j2
public class InboundConnectorProcessor {

  /**
   * A bean that has 'Function' functional interface, receives input - 1. Raw Data requests
   * (Containing XML Pacs004 base 64 encoded)
   *
   * @return An array of Streams that contain 1. A stream of messages with appropriate 2. A stream
   *     of Pacs008 Avro to be written into the requests topic
   */
  @Bean
  public Function<KStream<String, SpecificRecord>, KStream<String, SpecificRecord>[]>
      inboundConnector() {

    log.info("constructing the inboundConnector bean");
    return inputStream -> {
      inputStream.peek((key, value) -> MDC.put("uuid", key));
      return validateXML().andThen(convertToAvro()).apply(inputStream);
    };
  }

  /**
   * A function that Validates the XML against the XSD and processes the messages that contain the
   * invalid XML
   *
   * @return A functional interface that returns an array of KStream, where 0th Element contains the
   *     messages that are to be written in to the Monitoring topic and 1st Element contains the
   *     messages that are to be written in to the Request Topic
   */
  public Function<KStream<String, SpecificRecord>, KStream<String, SpecificRecord>[]>
      validateXML() {

    return inputStream -> {
      KStream<String, ? extends SpecificRecord>[] processedStream = new KStream[2];

      KStream<String, SpecificRecord>[] streamsAfterValidation =
          inputStream.branch(
              (key, value) -> {
                RawData rawData = (RawData) value;
                String xmlString = new String(Base64.getDecoder().decode(rawData.getRawXMLAvro()));
                return !DataUtil.isValidXML(xmlString);
              }, // Invalid Messages
              (key, value) -> {
                RawData rawData = (RawData) value;
                String xmlString = new String(Base64.getDecoder().decode(rawData.getRawXMLAvro()));
                return DataUtil.isValidXML(xmlString);
              } // Valid Messages
              );
      processedStream[0] =
          streamsAfterValidation[0].transformValues(InvalidXmlTransformer<SpecificRecord>::new);
      processedStream[1] = streamsAfterValidation[1];
      return (KStream<String, SpecificRecord>[]) processedStream;
    };
  }

  /**
   * A function that is responsible for converting the pacs008 xml into Avro
   *
   * @return A functional interface that returns an array of KStream, where 0th Element contains the
   *     messages that are to be written in to the Monitoring topic and 1st Element contains the
   *     messages that are to be written in to the Request Topic
   */
  public Function<KStream<String, SpecificRecord>[], KStream<String, SpecificRecord>[]>
      convertToAvro() {

    return inputStreams -> {
      inputStreams[1] =
          inputStreams[1].transformValues(
              (ValueTransformerSupplier<SpecificRecord, SpecificRecord>)
                  AvroConversionTransformer::new);

      return (KStream<String, SpecificRecord>[]) inputStreams;
    };
  }

  /**
   * A timestamp extractor bean used for extracting the kafka message timestamps
   *
   * @return TimeStampExtractor bean
   */
  @Bean
  public TimestampExtractor timestampExtractor() {
    return new WallclockTimestampExtractor();
  }
}
