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

static int run_producer (char *topic, char jsonData[]) {
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();

        if (rd_kafka_conf_set(conf, "client.id", "switch", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                return 0;
        }

        if (rd_kafka_conf_set(conf, "group.id", "denovo", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                return 0;
        }

        if (rd_kafka_conf_set(conf, "bootstrap.servers", "18.232.169.254:9094", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                return 0;
        }

        /* Create Kafka producer handle */
        rd_kafka_t *rk;
        if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)))) {
                fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
                return 0;
        }

        int delivery_counter = 0;

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
        const char *user = "denovo";
        
        rd_kafka_resp_err_t err;

        // snprintf(jsonData, sizeof(jsonData), "{ \"count\": %d }", i+1);
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
                return 0;
        }
        rd_kafka_poll(rk, 0);

        if (run) {
                fprintf(stderr, "Waiting for %d more delivery results\n", i);
                rd_kafka_flush(rk, 15*1000);
        }

        /* Destroy the producer instance. */
        rd_kafka_destroy(rk);

        fprintf(stderr, "%d/%d messages delivered\n", delivery_counter, i);

        return 0;
}

void main() {
// {"id": "00099286-1c6e-11ec-9590-16a97560b275", "txDate": "2021-09-23 12:58:51.000", "txTime": "12:58:51", "maskedPan": "***********1004", "txAmount": 1012, "tipAmount": 0, "localTaxAmount": 0, "cashDiscount": 0, "customFee": 0, "surcharge": 0, "hostResponseCode": "00", "posResponseCode": "00", "emvTags": 0, "invoiceNumber": "000423", "posBatchNumber": "", "processor": "",    "processorInfo": {      "developerID": "003338",      "applicationID": "B001",      "Mid": "886000003338",      "Agent": "000000",      "Chain": "111111",      "Store": "5999",      "TermNo": "1515",      "TermId": "75124668",      "dba": "Hot Chilli 17            ",      "city": "TEMPE        ",      "state": "AZ",      "address": "8320 S HARDY DRIVE",      "zipCode": "85284    ",      "phone": "480-333-3333 ",      "Bin": "999991",      "Mcc": "5999",      "DeviceCode": "X",      "IndustryCode": "R",      "dsGroup": "8GWK"    },    "tpn": 624121933799,    "profileId": "01",    "messageType": "0200",    "processingCode": "000000",    "posEntryMode": 711,    "emvFallBack": 0,    "transactionType": "CREDIT",    "cardHolderName": "",    "commonMode": "DIAL",    "pinpad": 1,    "spin": "",    "signature": "",    "pinBlock": 0,    "requestLoggedTime": "12:58:51",    "responseUpdatedTime": "12:58:52",    "posResponseTime": "08:59:02",    "posRequestTime": "18:29:00",    "transactionId": "00005362412193379920210923182900",    "posTraceNo": "000053",    "rrn": 126612502645,    "approvalCode": "AXS836",    "voidFlag": "N",    "settleFlag": "F",    "reversalFlag": "N",    "createdAt": "2021-09-23 12:58:51.000",    "updatedAt": "2021-10-04 10:16:51.000",    "preauthFlag": "N",    "txnCcy": 840,    "encryptedPan": "68DDBA55B2A8450E80C866E0DF9AA18A848005B59DCD2991",    "sourceType": "TPOS",    "destType": "TSYS",    "cardLabel": "AMEX",    "txName": "SALE",    "totalAmount": 1012,    "tipAdjAmount": 0,    "txnSeqNo": "0418",    "tpnBatchNumber": "001",    "srcRawMessage": "02003220048020418a0000000000000000101220210923182900000053071100370374245001771004d241270215041234500000363234313231393333373939001354584e30313150494430323031084030303030314430333039323630303530013682020d809f36020002df79085465726d696e616c9f100706020103a000009f3303e078c89f350122950500008080009f02060000000010129f1a0208405f2a0208409a032109239c0100df780200019f26089125c6fabe6770e29f2701809f34033f00009f3704987acf749f03060000000000004f08a0000000250104038408a000000025010403",    "hostResponseTime": "08:59:02",    "hostResponseDate": "2021-09-23",    "approvedAmount": 1012,    "tsysRespData": {      "ReqAci": "Y",      "G72": "",      "G14": "",      "HMI": "",      "NIC": "",      "SettleDate": "",      "G77": "",      "CofInd": "",      "CPCode": "",      "SQI": "",      "IntgClass": "",      "VCode": "NV1 ",      "G3CardType": "",      "ResAci": " ",      "CLabel": "AMEX",      "TxCd": "54",      "RF": "D",      "DC": "X",      "IC": "R",      "CIC": "Z",      "ADS": "S",      "G27": "027H1010X25414C",      "TxId": "000000321000837"    },    "posRequestDate": "2021-09-23",    "posConditionCode": "00", "merchantFee": 0, "stateTaxAmount": 0, "commercialTaxAmount": 0, "settleDate": "2021-10-04 10:16:51.000", "serviceFee": 0, "DiscountFee": 0, "rti": 0, "refundFlag": ""}
run_producer("switch_tx_bases", "{\"id\": \"00099286-1c6e-11ec-9590-16a97560b275\", \"txDate\": \"2021-09-23 12:58:51.000\", \"txTime\": \"12:58:51\", \"maskedPan\": \"***********1004\", \"txAmount\": 1012, \"tipAmount\": 0, \"localTaxAmount\": 0, \"cashDiscount\": 0, \"customFee\": 0, \"surcharge\": 0, \"hostResponseCode\": \"00\", \"posResponseCode\": \"00\", \"emvTags\": 0, \"invoiceNumber\": \"000423\", \"posBatchNumber\": \"\", \"processor\": \"\",    \"processorInfo\": {      \"developerID\": \"003338\",      \"applicationID\": \"B001\",      \"Mid\": \"886000003338\",      \"Agent\": \"000000\",      \"Chain\": \"111111\",      \"Store\": \"5999\",      \"TermNo\": \"1515\",      \"TermId\": \"75124668\",      \"dba\": \"Hot Chilli 17            \",      \"city\": \"TEMPE        \",      \"state\": \"AZ\",      \"address\": \"8320 S HARDY DRIVE\",      \"zipCode\": \"85284    \",      \"phone\": \"480-333-3333 \",      \"Bin\": \"999991\",      \"Mcc\": \"5999\",      \"DeviceCode\": \"X\",      \"IndustryCode\": \"R\",      \"dsGroup\": \"8GWK\"    },    \"tpn\": 624121933799,    \"profileId\": \"01\",    \"messageType\": \"0200\",    \"processingCode\": \"000000\",    \"posEntryMode\": 711,    \"emvFallBack\": 0,    \"transactionType\": \"CREDIT\",    \"cardHolderName\": \"\",    \"commonMode\": \"DIAL\",    \"pinpad\": 1,    \"spin\": \"\",    \"signature\": \"\",    \"pinBlock\": 0,    \"requestLoggedTime\": \"12:58:51\",    \"responseUpdatedTime\": \"12:58:52\",    \"posResponseTime\": \"08:59:02\",    \"posRequestTime\": \"18:29:00\",    \"transactionId\": \"00005362412193379920210923182900\",    \"posTraceNo\": \"000053\",    \"rrn\": 126612502645,    \"approvalCode\": \"AXS836\",    \"voidFlag\": \"N\",    \"settleFlag\": \"F\",    \"reversalFlag\": \"N\",    \"createdAt\": \"2021-09-23 12:58:51.000\",    \"updatedAt\": \"2021-10-04 10:16:51.000\",    \"preauthFlag\": \"N\",    \"txnCcy\": 840,    \"encryptedPan\": \"68DDBA55B2A8450E80C866E0DF9AA18A848005B59DCD2991\",    \"sourceType\": \"TPOS\",    \"destType\": \"TSYS\",    \"cardLabel\": \"AMEX\",    \"txName\": \"SALE\",    \"totalAmount\": 1012,    \"tipAdjAmount\": 0,    \"txnSeqNo\": \"0418\",    \"tpnBatchNumber\": \"001\",    \"srcRawMessage\": \"02003220048020418a0000000000000000101220210923182900000053071100370374245001771004d241270215041234500000363234313231393333373939001354584e30313150494430323031084030303030314430333039323630303530013682020d809f36020002df79085465726d696e616c9f100706020103a000009f3303e078c89f350122950500008080009f02060000000010129f1a0208405f2a0208409a032109239c0100df780200019f26089125c6fabe6770e29f2701809f34033f00009f3704987acf749f03060000000000004f08a0000000250104038408a000000025010403\",    \"hostResponseTime\": \"08:59:02\",    \"hostResponseDate\": \"2021-09-23\",    \"approvedAmount\": 1012,    \"tsysRespData\": {      \"ReqAci\": \"Y\",      \"G72\": \"\",      \"G14\": \"\",      \"HMI\": \"\",      \"NIC\": \"\",      \"SettleDate\": \"\",      \"G77\": \"\",      \"CofInd\": \"\",      \"CPCode\": \"\",      \"SQI\": \"\",      \"IntgClass\": \"\",      \"VCode\": \"NV1 \",      \"G3CardType\": \"\",      \"ResAci\": \" \",      \"CLabel\": \"AMEX\",      \"TxCd\": \"54\",      \"RF\": \"D\",      \"DC\": \"X\",      \"IC\": \"R\",      \"CIC\": \"Z\",      \"ADS\": \"S\",      \"G27\": \"027H1010X25414C\",      \"TxId\": \"000000321000837\"    },    \"posRequestDate\": \"2021-09-23\",    \"posConditionCode\": \"00\", \"merchantFee\": 0, \"stateTaxAmount\": 0, \"commercialTaxAmount\": 0, \"settleDate\": \"2021-10-04 10:16:51.000\", \"serviceFee\": 0, \"DiscountFee\": 0, \"rti\": 0, \"refundFlag\": \"\"}"); 
}