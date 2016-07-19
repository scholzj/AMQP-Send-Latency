package cz.scholz.amqp.sendperformance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;
import java.util.Random;

/**
 * Broadcast Receiver Receives broadcasts from the persistent broadcast queue
 */
public class BroadcastSender {
    private final Logger LOGGER = LoggerFactory
            .getLogger(BroadcastSender.class);
    private final InitialContext context;
    private final int timeoutInMillis;

    public BroadcastSender(Options options) throws NamingException {
        this.timeoutInMillis = options.getTimeoutInMillis();
        try {
            Properties properties = new Properties();
            properties.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                    "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
            properties
                    .setProperty(
                            "connectionfactory.connection",
                            String.format(
                                    "amqp://admin:admin@App1/?brokerlist='tcp://%s:%d'&maxprefetch='10000'&sync_publish='all'",
                                    options.getHostname(), options.getPort()));
            properties
                    .setProperty(
                            "destination.broadcastAddress",
                            String.format(
                                    "broadcast/broadcast.CLEAR_LIVETransaction; { node: { type: topic }, create: never, mode: consume, assert: never }",
                                    options.getAccountName()));
            this.context = new InitialContext(properties);
        } catch (NamingException ex) {
            LOGGER.error("Unable to proceed with broadcast receiver", ex);
            throw ex;
        }
    }

    public void run() throws JMSException, NamingException,
            InterruptedException {
		/*
		 * Step 1: Initializing the context based on the properties file we
		 * prepared
		 */
        Connection connection = null;
        Session session = null;
        MessageProducer broadcastProducer = null;
        try {
			/*
			 * Step 2: Preparing the connection and session
			 */
            LOGGER.info("Creating connection");
            connection = ((ConnectionFactory) context.lookup("connection"))
                    .createConnection();
            session = connection.createSession(false,
                    Session.CLIENT_ACKNOWLEDGE);
			/*
			 * Step 3: Creating a broadcast receiver / consumer
			 */
            broadcastProducer = session.createProducer((Destination) context
                    .lookup("broadcastAddress"));
            broadcastProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
			/*
			 * Step 4: Starting the connection
			 */
            connection.start();
            LOGGER.info("Connected");

			/*
			 * Step 5: Receiving broadcast messages using listener for timeout
			 * seconds
			 */
            BytesMessage msg;
            int targetCount = 100000;
            int messageSize = 1024;
            int messageCount = 0;
            long limit = 10000000; // report all over 10 miliseconds
            long limit2 = 100000000; // report all over 100 miliseconds
            long limit3 = 1000000000; // report all over 1000 miliseconds
            long limit4 = 2000000000; // report all over 2000 miliseconds
            long messageOverLimit = 0;
            long messageOverLimit2 = 0;
            long messageOverLimit3 = 0;
            long messageOverLimit4 = 0;
            Random rand = new Random();
            byte[] body = new byte[messageSize];

            while (messageCount < targetCount)
            {
                messageCount++;
                msg = session.createBytesMessage();
                rand.nextBytes(body);
                msg.writeBytes(body);

                long before = System.nanoTime();
                broadcastProducer.send(msg);
                long after = System.nanoTime();
                long diff = after - before;

                if (diff > limit4) {
                    LOGGER.info(msg.getJMSMessageID() + " took " + diff/1000/1000/1000 + " seconds");
                    messageOverLimit++;
                    messageOverLimit2++;
                    messageOverLimit3++;
                    messageOverLimit4++;
                }
                else if (diff > limit3) {
                    LOGGER.info(msg.getJMSMessageID() + " took " + diff/1000/1000/1000 + " seconds");
                    messageOverLimit++;
                    messageOverLimit2++;
                    messageOverLimit3++;
                }
                else if (diff > limit2) {
                    //LOGGER.info(diff/1000/1000 + " milliseconds");
                    messageOverLimit++;
                    messageOverLimit2++;
                }
                else if (diff > limit) {
                    //LOGGER.info(diff/1000/1000 + " milliseconds");
                    messageOverLimit++;
                }
                else {
                    //LOGGER.info(diff);
                }

                if (messageCount % 1000 == 0)
                {
                    LOGGER.info(messageCount + " messages sent");
                }
            }

            LOGGER.info("Sent " + messageCount + " messages. ");
            LOGGER.info("In total " + messageOverLimit + " were sent in more than " + limit/1000000 + " miliseconds. That is " + ((1.0*messageOverLimit)/messageCount)*100 + "%");
            LOGGER.info("In total " + messageOverLimit2 + " were sent in more than " + limit2/1000000 + " miliseconds. That is " + ((1.0*messageOverLimit2)/messageCount)*100 + "%");
            LOGGER.info("In total " + messageOverLimit3 + " were sent in more than " + limit3/1000000 + " miliseconds. That is " + ((1.0*messageOverLimit3)/messageCount)*100 + "%");
            LOGGER.info("In total " + messageOverLimit4 + " were sent in more than " + limit4/1000000 + " miliseconds. That is " + ((1.0*messageOverLimit4)/messageCount)*100 + "%");

            LOGGER.info("Finished sending broadcast messages for {} seconds",
                    this.timeoutInMillis / 1000);
        } catch (JMSException | NamingException e) {
            LOGGER.error("Unable to proceed with broadcast receiver", e);
            throw e;
        } finally {
			/*
			 * Step 6: Closing the connection
			 */
            if (broadcastProducer != null) {
                LOGGER.info("Closing consumer");
                broadcastProducer.close();
            }
            if (session != null) {
                LOGGER.info("Closing session");
                session.close();
            }
            if (connection != null) {
                // implicitly closes session and producers/consumers
                LOGGER.info("Closing connection");
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws JMSException,
            NamingException, InterruptedException {

        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        System.setProperty("slf4j.logger.org.apache.qpid", "trace");

        System.out.println("Hostname: " + args[0]);
        System.out.println("Port: " + args[1]);

        Options options = new Options.OptionsBuilder()
                .accountName("ABCFR_ABCFRALMMACC1")
                .hostname(args[0]).port(Integer.parseInt(args[1]))
                //.hostname("cbgc01.xeop.de").port(29703)
                .keystoreFilename("ABCFR_ABCFRALMMACC1.keystore")
                .keystorePassword("123456").truststoreFilename("truststore")
                .truststorePassword("123456")
                .certificateAlias("abcfr_abcfralmmacc1").build();
        BroadcastSender broadcastSender = new BroadcastSender(options);
        broadcastSender.run();
    }
}