package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	sarama "github.com/Shopify/sarama"
	flowmessage "github.com/cloudflare/goflow/v3/pb"
	"github.com/cloudflare/goflow/v3/utils"
	proto "github.com/golang/protobuf/proto"
	utils "github.com/cloudflare/goflow/v3/utils"
)

var (
	KafkaTLS   *bool
	KafkaSASL  *bool
	KafkaTopic *string
	KafkaTopicJson *string
	KafkaSrv   *string
	KafkaBrk   *string

	KafkaLogErrors *bool

	KafkaHashing *bool
	KafkaKeying  *string
	KafkaVersion *string

	kafkaConfigVersion sarama.KafkaVersion = sarama.V0_11_0_0
)

type KafkaState struct {
	FixedLengthProto bool
	producer         sarama.AsyncProducer
	topic            string
	topicjson		 string
	hashing          bool
	keying           []string
}

// SetKafkaVersion sets the KafkaVersion that is used to set the log message format version
func SetKafkaVersion(version sarama.KafkaVersion) {
	kafkaConfigVersion = version
}

// ParseKafkaVersion is a pass through to sarama.ParseKafkaVersion to get a KafkaVersion struct by a string version that can be passed into SetKafkaVersion
// This function is here so that calling code need not import sarama to set KafkaVersion
func ParseKafkaVersion(versionString string) (sarama.KafkaVersion, error) {
	return sarama.ParseKafkaVersion(versionString)
}

func RegisterFlags() {
	KafkaTLS = flag.Bool("kafka.tls", false, "Use TLS to connect to Kafka")
	KafkaSASL = flag.Bool("kafka.sasl", false, "Use SASL/PLAIN data to connect to Kafka (TLS is recommended and the environment variables KAFKA_SASL_USER and KAFKA_SASL_PASS need to be set)")
	KafkaTopic = flag.String("kafka.topic", "flow-messages", "Kafka topic to produce to")
	KafkaTopicJson = flag.String("kafka.topicjson", "flow-messages-json", "Kafka topic to produce to")
	KafkaSrv = flag.String("kafka.srv", "", "SRV record containing a list of Kafka brokers (or use kafka.out.brokers)")
	KafkaBrk = flag.String("kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")

	KafkaLogErrors = flag.Bool("kafka.log.err", false, "Log Kafka errors")

	KafkaHashing = flag.Bool("kafka.hashing", false, "Enable partitioning by hash instead of random")
	KafkaKeying = flag.String("kafka.key", "SamplerAddress,DstAS", "Kafka list of fields to do hashing on (partition) separated by commas")
	KafkaVersion = flag.String("kafka.version", "0.11.0.0", "Log message version (must be a version that parses per sarama.ParseKafkaVersion)")
}

func StartKafkaProducerFromArgs(log utils.Logger) (*KafkaState, error) {
	kVersion, err := ParseKafkaVersion(*KafkaVersion)
	if err != nil {
		return nil, err
	}
	SetKafkaVersion(kVersion)
	addrs := make([]string, 0)
	if *KafkaSrv != "" {
		addrs, _ = utils.GetServiceAddresses(*KafkaSrv)
	} else {
		addrs = strings.Split(*KafkaBrk, ",")
	}
	return StartKafkaProducer(addrs, *KafkaTopic, *KafkaTopicJson,*KafkaHashing, *KafkaKeying, *KafkaTLS, *KafkaSASL, *KafkaLogErrors, log)
}

func StartKafkaProducer(addrs []string, topic string, topicjson string, hashing bool, keying string, useTls bool, useSasl bool, logErrors bool, log utils.Logger) (*KafkaState, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = kafkaConfigVersion
	kafkaConfig.Producer.Return.Successes = false
	kafkaConfig.Producer.Return.Errors = logErrors
	if useTls {
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error initializing TLS: %v", err))
		}
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tls.Config{RootCAs: rootCAs}
	}

	var keyingSplit []string
	if hashing {
		kafkaConfig.Producer.Partitioner = sarama.NewHashPartitioner
		keyingSplit = strings.Split(keying, ",")
	}

	if useSasl {
		if !useTls && log != nil {
			log.Warn("Using SASL without TLS will transmit the authentication in plaintext!")
		}
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = os.Getenv("KAFKA_SASL_USER")
		kafkaConfig.Net.SASL.Password = os.Getenv("KAFKA_SASL_PASS")
		if kafkaConfig.Net.SASL.User == "" && kafkaConfig.Net.SASL.Password == "" {
			return nil, errors.New("Kafka SASL config from environment was unsuccessful. KAFKA_SASL_USER and KAFKA_SASL_PASS need to be set.")
		} else if log != nil {
			log.Infof("Authenticating as user '%s'...", kafkaConfig.Net.SASL.User)
		}
	}

	kafkaProducer, err := sarama.NewAsyncProducer(addrs, kafkaConfig)
	if err != nil {
		return nil, err
	}
	state := KafkaState{
		producer: kafkaProducer,
		topic:    topic,
		hashing:  hashing,
		keying:   keyingSplit,
	}

	if logErrors {
		go func() {
			for {
				select {
				case msg := <-kafkaProducer.Errors():
					if log != nil {
						log.Error(msg)
					}
				}
			}
		}()
	}

	return &state, nil
}

func HashProto(fields []string, flowMessage *flowmessage.FlowMessage) string {
	var keyStr string

	if flowMessage != nil {
		vfm := reflect.ValueOf(flowMessage)
		vfm = reflect.Indirect(vfm)

		for _, kf := range fields {
			fieldValue := vfm.FieldByName(kf)
			if fieldValue.IsValid() {
				keyStr += fmt.Sprintf("%v-", fieldValue)
			}
		}
	}

	return keyStr
}

func (s KafkaState) SendKafkaFlowMessage(flowMessage *flowmessage.FlowMessage) {
	var key sarama.Encoder
	if s.hashing {
		keyStr := HashProto(s.keying, flowMessage)
		key = sarama.StringEncoder(keyStr)
	}
	var b []byte
	if !s.FixedLengthProto {
		b, _ = proto.Marshal(flowMessage)
	} else {
		buf := proto.NewBuffer([]byte{})
		buf.EncodeMessage(flowMessage)
		b = buf.Bytes()
	}
	s.producer.Input() <- &sarama.ProducerMessage{
		Topic: s.topic,
		Key:   key,
		Value: sarama.ByteEncoder(b),
	}
	// var flowJson=FlowMessageToJSON(flowmessage);
	// log.Printf(flowJson);
	
	s.producer.Input() <- &sarama.ProducerMessage{
		Topic: s.topicjson,
		Key:   key,
		Value: utils.FlowMessageToJSON(flowMessage),
	}
}

func (s KafkaState) Publish(msgs []*flowmessage.FlowMessage) {
	for _, msg := range msgs {
		s.SendKafkaFlowMessage(msg)
	}
}

// func FlowMessageToJSON(fmsg *flowmessage.FlowMessage) string {
// 	filteredMessage := flowMessageFiltered(fmsg)
// 	message := make([]string, len(filteredMessage))
// 	for i, m := range filteredMessage {
// 		message[i] = fmt.Sprintf("\"%s\":\"%s\"", m.Name, m.Value)
// 	}
// 	return "{" + strings.Join(message, ",") + "}"
// }

// func flowMessageFiltered(fmsg *flowmessage.FlowMessage) []flowMessageItem {
// 	srcmac := make([]byte, 8)
// 	dstmac := make([]byte, 8)
// 	binary.BigEndian.PutUint64(srcmac, fmsg.SrcMac)
// 	binary.BigEndian.PutUint64(dstmac, fmsg.DstMac)
// 	srcmac = srcmac[2:8]
// 	dstmac = dstmac[2:8]
// 	var message []flowMessageItem

// 	for _, field := range strings.Split(*MessageFields, ",") {
// 		switch field {
// 		case "Type":
// 			message = append(message, flowMessageItem{"Type", fmsg.Type.String()})
// 		case "TimeReceived":
// 			message = append(message, flowMessageItem{"TimeReceived", fmt.Sprintf("%v", fmsg.TimeReceived)})
// 		case "SequenceNum":
// 			message = append(message, flowMessageItem{"SequenceNum", fmt.Sprintf("%v", fmsg.SequenceNum)})
// 		case "SamplingRate":
// 			message = append(message, flowMessageItem{"SamplingRate", fmt.Sprintf("%v", fmsg.SamplingRate)})
// 		case "SamplerAddress":
// 			message = append(message, flowMessageItem{"SamplerAddress", net.IP(fmsg.SamplerAddress).String()})
// 		case "TimeFlowStart":
// 			message = append(message, flowMessageItem{"TimeFlowStart", fmt.Sprintf("%v", fmsg.TimeFlowStart)})
// 		case "TimeFlowEnd":
// 			message = append(message, flowMessageItem{"TimeFlowEnd", fmt.Sprintf("%v", fmsg.TimeFlowEnd)})
// 		case "Bytes":
// 			message = append(message, flowMessageItem{"Bytes", fmt.Sprintf("%v", fmsg.Bytes)})
// 		case "Packets":
// 			message = append(message, flowMessageItem{"Packets", fmt.Sprintf("%v", fmsg.Packets)})
// 		case "SrcAddr":
// 			message = append(message, flowMessageItem{"SrcAddr", net.IP(fmsg.SrcAddr).String()})
// 		case "DstAddr":
// 			message = append(message, flowMessageItem{"DstAddr", net.IP(fmsg.DstAddr).String()})
// 		case "Etype":
// 			message = append(message, flowMessageItem{"Etype", fmt.Sprintf("%v", fmsg.Etype)})
// 		case "Proto":
// 			message = append(message, flowMessageItem{"Proto", fmt.Sprintf("%v", fmsg.Proto)})
// 		case "SrcPort":
// 			message = append(message, flowMessageItem{"SrcPort", fmt.Sprintf("%v", fmsg.SrcPort)})
// 		case "DstPort":
// 			message = append(message, flowMessageItem{"DstPort", fmt.Sprintf("%v", fmsg.DstPort)})
// 		case "InIf":
// 			message = append(message, flowMessageItem{"InIf", fmt.Sprintf("%v", fmsg.InIf)})
// 		case "OutIf":
// 			message = append(message, flowMessageItem{"OutIf", fmt.Sprintf("%v", fmsg.OutIf)})
// 		case "SrcMac":
// 			message = append(message, flowMessageItem{"SrcMac", net.HardwareAddr(srcmac).String()})
// 		case "DstMac":
// 			message = append(message, flowMessageItem{"DstMac", net.HardwareAddr(dstmac).String()})
// 		case "SrcVlan":
// 			message = append(message, flowMessageItem{"SrcVlan", fmt.Sprintf("%v", fmsg.SrcVlan)})
// 		case "DstVlan":
// 			message = append(message, flowMessageItem{"DstVlan", fmt.Sprintf("%v", fmsg.DstVlan)})
// 		case "VlanId":
// 			message = append(message, flowMessageItem{"VlanId", fmt.Sprintf("%v", fmsg.VlanId)})
// 		case "IngressVrfID":
// 			message = append(message, flowMessageItem{"IngressVrfID", fmt.Sprintf("%v", fmsg.IngressVrfID)})
// 		case "EgressVrfID":
// 			message = append(message, flowMessageItem{"EgressVrfID", fmt.Sprintf("%v", fmsg.EgressVrfID)})
// 		case "IPTos":
// 			message = append(message, flowMessageItem{"IPTos", fmt.Sprintf("%v", fmsg.IPTos)})
// 		case "ForwardingStatus":
// 			message = append(message, flowMessageItem{"ForwardingStatus", fmt.Sprintf("%v", fmsg.ForwardingStatus)})
// 		case "IPTTL":
// 			message = append(message, flowMessageItem{"IPTTL", fmt.Sprintf("%v", fmsg.IPTTL)})
// 		case "TCPFlags":
// 			message = append(message, flowMessageItem{"TCPFlags", fmt.Sprintf("%v", fmsg.TCPFlags)})
// 		case "IcmpType":
// 			message = append(message, flowMessageItem{"IcmpType", fmt.Sprintf("%v", fmsg.IcmpType)})
// 		case "IcmpCode":
// 			message = append(message, flowMessageItem{"IcmpCode", fmt.Sprintf("%v", fmsg.IcmpCode)})
// 		case "IPv6FlowLabel":
// 			message = append(message, flowMessageItem{"IPv6FlowLabel", fmt.Sprintf("%v", fmsg.IPv6FlowLabel)})
// 		case "FragmentId":
// 			message = append(message, flowMessageItem{"FragmentId", fmt.Sprintf("%v", fmsg.FragmentId)})
// 		case "FragmentOffset":
// 			message = append(message, flowMessageItem{"FragmentOffset", fmt.Sprintf("%v", fmsg.FragmentOffset)})
// 		case "BiFlowDirection":
// 			message = append(message, flowMessageItem{"BiFlowDirection", fmt.Sprintf("%v", fmsg.BiFlowDirection)})
// 		case "SrcAS":
// 			message = append(message, flowMessageItem{"SrcAS", fmt.Sprintf("%v", fmsg.SrcAS)})
// 		case "DstAS":
// 			message = append(message, flowMessageItem{"DstAS", fmt.Sprintf("%v", fmsg.DstAS)})
// 		case "NextHop":
// 			message = append(message, flowMessageItem{"NextHop", net.IP(fmsg.NextHop).String()})
// 		case "NextHopAS":
// 			message = append(message, flowMessageItem{"NextHopAS", fmt.Sprintf("%v", fmsg.NextHopAS)})
// 		case "SrcNet":
// 			message = append(message, flowMessageItem{"SrcNet", fmt.Sprintf("%v", fmsg.SrcNet)})
// 		case "DstNet":
// 			message = append(message, flowMessageItem{"DstNet", fmt.Sprintf("%v", fmsg.DstNet)})
// 		case "HasEncap":
// 			message = append(message, flowMessageItem{"HasEncap", fmt.Sprintf("%v", fmsg.HasEncap)})
// 		case "SrcAddrEncap":
// 			message = append(message, flowMessageItem{"SrcAddrEncap", net.IP(fmsg.SrcAddrEncap).String()})
// 		case "DstAddrEncap":
// 			message = append(message, flowMessageItem{"DstAddrEncap", net.IP(fmsg.DstAddrEncap).String()})
// 		case "ProtoEncap":
// 			message = append(message, flowMessageItem{"ProtoEncap", fmt.Sprintf("%v", fmsg.ProtoEncap)})
// 		case "EtypeEncap":
// 			message = append(message, flowMessageItem{"EtypeEncap", fmt.Sprintf("%v", fmsg.EtypeEncap)})
// 		case "IPTosEncap":
// 			message = append(message, flowMessageItem{"IPTosEncap", fmt.Sprintf("%v", fmsg.IPTosEncap)})
// 		case "IPTTLEncap":
// 			message = append(message, flowMessageItem{"IPTTLEncap", fmt.Sprintf("%v", fmsg.IPTTLEncap)})
// 		case "IPv6FlowLabelEncap":
// 			message = append(message, flowMessageItem{"IPv6FlowLabelEncap", fmt.Sprintf("%v", fmsg.IPv6FlowLabelEncap)})
// 		case "FragmentIdEncap":
// 			message = append(message, flowMessageItem{"FragmentIdEncap", fmt.Sprintf("%v", fmsg.FragmentIdEncap)})
// 		case "FragmentOffsetEncap":
// 			message = append(message, flowMessageItem{"FragmentOffsetEncap", fmt.Sprintf("%v", fmsg.FragmentOffsetEncap)})
// 		case "HasMPLS":
// 			message = append(message, flowMessageItem{"HasMPLS", fmt.Sprintf("%v", fmsg.HasMPLS)})
// 		case "MPLSCount":
// 			message = append(message, flowMessageItem{"MPLSCount", fmt.Sprintf("%v", fmsg.MPLSCount)})
// 		case "MPLS1TTL":
// 			message = append(message, flowMessageItem{"MPLS1TTL", fmt.Sprintf("%v", fmsg.MPLS1TTL)})
// 		case "MPLS1Label":
// 			message = append(message, flowMessageItem{"MPLS1Label", fmt.Sprintf("%v", fmsg.MPLS1Label)})
// 		case "MPLS2TTL":
// 			message = append(message, flowMessageItem{"MPLS2TTL", fmt.Sprintf("%v", fmsg.MPLS2TTL)})
// 		case "MPLS2Label":
// 			message = append(message, flowMessageItem{"MPLS2Label", fmt.Sprintf("%v", fmsg.MPLS2Label)})
// 		case "MPLS3TTL":
// 			message = append(message, flowMessageItem{"MPLS3TTL", fmt.Sprintf("%v", fmsg.MPLS3TTL)})
// 		case "MPLS3Label":
// 			message = append(message, flowMessageItem{"MPLS3Label", fmt.Sprintf("%v", fmsg.MPLS3Label)})
// 		case "MPLSLastTTL":
// 			message = append(message, flowMessageItem{"MPLSLastTTL", fmt.Sprintf("%v", fmsg.MPLSLastTTL)})
// 		case "MPLSLastLabel":
// 			message = append(message, flowMessageItem{"MPLSLastLabel", fmt.Sprintf("%v", fmsg.MPLSLastLabel)})
// 		case "HasPPP":
// 			message = append(message, flowMessageItem{"HasPPP", fmt.Sprintf("%v", fmsg.HasPPP)})
// 		case "PPPAddressControl":
// 			message = append(message, flowMessageItem{"PPPAddressControl", fmt.Sprintf("%v", fmsg.PPPAddressControl)})
// 		}
// 	}

// 	return message
// }