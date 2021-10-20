#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include "common.h"
#include "json.h"

static void dr_cb (rd_kafka_t *rk,
                   const rd_kafka_message_t *rkmessage, void *opaque) {
        int *delivery_counterp = (int *)rkmessage->_private; /* V_OPAQUE */

        if (rkmessage->err) {
                fprintf(stderr, "Delivery failed for message %.*s: %s\n",
                        (int)rkmessage->len, (const char *)rkmessage->payload,
                        rd_kafka_err2str(rkmessage->err));
        } else {
                fprintf(stderr,
                        "Message delivered to %s [%d] at offset %"PRId64
                        " in %.2fms: %.*s\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        (int)rkmessage->partition,
                        rkmessage->offset,
                        (float)rd_kafka_message_latency(rkmessage) / 1000.0,
                        (int)rkmessage->len, (const char *)rkmessage->payload);
                (*delivery_counterp)++;
        }
}


static int run_producer (char *jsonData) {
        char errstr[512];
        const char *topic;
        rd_kafka_conf_t *conf = rd_kafka_conf_new();

        if (rd_kafka_conf_set(conf, "client.id", "foo", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                exit(1);
        }

        if (rd_kafka_conf_set(conf, "group.id", "foo", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                exit(1);
        }

        if (rd_kafka_conf_set(conf, "bootstrap.servers", "18.232.169.254:9094", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                exit(1);
        }

        /* Create Kafka producer handle */
        rd_kafka_t *rk;
        if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)))) {
                fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
                exit(1);
        }

        topic = "test_c_code";
        int delivery_counter = 0;

        /* Set up a delivery report callback that will be triggered
         * from poll() or flush() for the final delivery status of
         * each message produced. */
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);

        /* Create producer.
         * A successful call assumes ownership of \p conf. */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "Failed to create producer: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        unsigned char run = 1;
        int i = 1;
        /* Produce messages */
        const char *user = "alice";
        
        rd_kafka_resp_err_t err;

        // snprintf(jsonData, sizeof(jsonData), "{ \"count\": %d }", i+1);

        fprintf(stderr, "Producing message #%d to %s: %s=%s\n", i, topic, user, jsonData);

        /* Asynchronous produce */
        err = rd_kafka_producev(
                rk,
                RD_KAFKA_V_TOPIC(topic),
                RD_KAFKA_V_KEY(user, strlen(user)),
                RD_KAFKA_V_VALUE(jsonData, strlen(jsonData)),
                /* producev() will make a copy of the message
                        * value (the key is always copied), so we
                        * can reuse the same json buffer on the
                        * next iteration. */
                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_OPAQUE(&delivery_counter),
                RD_KAFKA_V_END);
        if (err) {
                fprintf(stderr, "Produce failed: %s\n", rd_kafka_err2str(err));
                break;
        }
        rd_kafka_poll(rk, 0);

        if (run) {
                fprintf(stderr, "Waiting for %d more delivery results\n", 10 - delivery_counter);
                rd_kafka_flush(rk, 15*1000);
        }

        /* Destroy the producer instance. */
        rd_kafka_destroy(rk);

        fprintf(stderr, "%d/%d messages delivered\n", delivery_counter, 10);

        return 0;
}

void main(){
        char jsonData[] = "{ \"count\": 45 }";
        run_producer(jsonData); 
}