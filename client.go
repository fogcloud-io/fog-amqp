package amqp

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Logger interface {
	Debug(args ...interface{})
	Debugf(template string, args ...interface{})
	Info(args ...interface{})
	Infof(template string, args ...interface{})
	Warn(args ...interface{})
	Warnf(template string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(template string, args ...interface{})
}

var (
	ErrAmqpShutdown           = errors.New("amqp shutdown")
	ErrAmqpChannelInitTimeout = errors.New("amqp channel init timeout")
	ErrAmqpConnTimeout        = errors.New("amqp connect timeout")
	ErrAmqpConnNil            = errors.New("amqp conn nil")
	ErrAmqpReconn             = errors.New("amqp reconnecting")
)

type (
	ClientOption          func(*RabbitmqClient)
	ConsumerOption        func(*consumerParams)
	ExchangeDeclareOption func(*exchangeParams)
	ExchangeDeleteOption  func(*exchangeDeleteParams)
	QueueDeclareOption    func(*queueParams)
	QueueBindOption       func(*queueBindParams)
	QueueDeleteOption     func(*queueDeleteParams)
	PublishOption         func(*publishParams)

	reconnOption func(*RabbitmqClient) error

	AMQPMsgHandler = func(amqp.Delivery)
	Delivery       = amqp.Delivery
	Table          = amqp.Table
	Publishing     = amqp.Publishing

	consumerParams struct {
		consumerTag string
		autoAck     bool
		exclusive   bool
		noWait      bool
		args        Table
	}

	exchangeParams struct {
		durable    bool
		autoDelete bool
		internal   bool
		noWait     bool
		args       Table
	}

	exchangeDeleteParams struct {
		ifUnused bool
		noWait   bool
	}

	queueBindParams struct {
		noWait bool
		args   Table
	}

	queueParams struct {
		durable    bool
		autoDelete bool
		exclusive  bool
		noWait     bool
		args       Table
	}

	queueDeleteParams struct {
		ifUnused bool
		ifEmpty  bool
		noWait   bool
	}

	publishParams struct {
		mandatory bool
		immediate bool

		headers       amqp.Table
		deliveryMode  uint8
		priority      uint8
		contentType   string
		msgExpiration string
		msgId         string
		msgType       string
		userId        string
		appId         string
	}
)

func WithClientOptionsLogger(logger Logger) ClientOption {
	return func(rc *RabbitmqClient) {
		if logger != nil {
			rc.logger = logger
		}
	}
}

func WithClientOptionsConnTimeout(t time.Duration) ClientOption {
	return func(rc *RabbitmqClient) {
		rc.connTimeout = t
	}
}

func WithClientOptionsChInitTimeout(t time.Duration) ClientOption {
	return func(rc *RabbitmqClient) {
		rc.initMQChTimeout = t
	}
}

/*
When autoAck (also known as noAck) is true, the server will acknowledge
deliveries to this consumer prior to writing the delivery to the network.  When
autoAck is true, the consumer should not call Delivery.Ack. Automatically
acknowledging deliveries means that some deliveries may get lost if the
consumer is unable to process them after the server delivers them.
See http://www.rabbitmq.com/confirms.html for more details.
*/
func WithConsumerOptionsAutoAck(autoAck bool) ConsumerOption {
	return func(co *consumerParams) {
		co.autoAck = autoAck
	}
}

/*
When exclusive is true, the server will ensure that this is the sole consumer
from this queue. When exclusive is false, the server will fairly distribute
deliveries across multiple consumers.
*/
func WithConsumerOptionsExclusive(exclusive bool) ConsumerOption {
	return func(co *consumerParams) {
		co.exclusive = exclusive
	}
}

/*
When noWait is true, do not wait for the server to confirm the request and
immediately begin deliveries.  If it is not possible to consume, a channel
exception will be raised and the channel will be closed.
*/
func WithConsumerOptionsNoWait(noWait bool) ConsumerOption {
	return func(co *consumerParams) {
		co.noWait = noWait
	}
}

func WithConsumerOptionsConsumerTag(consumerTag string) ConsumerOption {
	return func(co *consumerParams) {
		co.consumerTag = consumerTag
	}
}

func WithConsumerOptionsArgs(args Table) ConsumerOption {
	return func(co *consumerParams) {
		co.args = args
	}
}

func WithExchangeDeclareOptionsDurable(durable bool) ExchangeDeclareOption {
	return func(ep *exchangeParams) {
		ep.durable = durable
	}
}

func WithExchangeDeclareOptionsAutoDelete(autoDelete bool) ExchangeDeclareOption {
	return func(ep *exchangeParams) {
		ep.autoDelete = autoDelete
	}
}

func WithExchangeDeclareOptionsInternal(internal bool) ExchangeDeclareOption {
	return func(ep *exchangeParams) {
		ep.internal = internal
	}
}

func WithExchangeDeclareOptionsNoWait(noWait bool) ExchangeDeclareOption {
	return func(ep *exchangeParams) {
		ep.noWait = noWait
	}
}

func WithExchangeDeclareOptionsArgs(args Table) ExchangeDeclareOption {
	return func(ep *exchangeParams) {
		ep.args = args
	}
}

func WithExchangeDeleteOptionsIfUnused(ifUnused bool) ExchangeDeleteOption {
	return func(edp *exchangeDeleteParams) {
		edp.ifUnused = ifUnused
	}
}

func WithExchangeDeleteOptionsNoWait(noWait bool) ExchangeDeleteOption {
	return func(edp *exchangeDeleteParams) {
		edp.noWait = noWait
	}
}

func WithQueueBindOptionsNoWait(noWait bool) QueueBindOption {
	return func(qbp *queueBindParams) {
		qbp.noWait = noWait
	}
}

func WithQueueBindOptionsArgs(args Table) QueueBindOption {
	return func(qbp *queueBindParams) {
		qbp.args = args
	}
}

func WithQueueDeclareOptionsDurable(durable bool) QueueDeclareOption {
	return func(qp *queueParams) {
		qp.durable = durable
	}
}

func WithQueueDeclareOptionsAutoDelete(autoDelete bool) QueueDeclareOption {
	return func(qp *queueParams) {
		qp.autoDelete = autoDelete
	}
}

func WithQueueDeclareOptionsInternal(exclusive bool) QueueDeclareOption {
	return func(qp *queueParams) {
		qp.exclusive = exclusive
	}
}

func WithQueueDeclareOptionsNoWait(noWait bool) QueueDeclareOption {
	return func(qp *queueParams) {
		qp.noWait = noWait
	}
}

func WithQueueDeclareOptionsArgs(args Table) QueueDeclareOption {
	return func(qp *queueParams) {
		qp.args = args
	}
}

func WithQueueDeleteOptionsIfUnused(ifUnused bool) QueueDeleteOption {
	return func(qdp *queueDeleteParams) {
		qdp.ifUnused = ifUnused
	}
}

func WithQueueDeleteOptionsNoWait(noWait bool) QueueDeleteOption {
	return func(qdp *queueDeleteParams) {
		qdp.noWait = noWait
	}
}

func WithPublishOptionsMandatory(mandatory bool) PublishOption {
	return func(pp *publishParams) {
		pp.mandatory = mandatory
	}
}

func WithPublishOptionsImmediate(immediate bool) PublishOption {
	return func(pp *publishParams) {
		pp.immediate = immediate
	}
}

func WithPublishOptionsContentType(contentType string) PublishOption {
	return func(pp *publishParams) {
		pp.contentType = contentType
	}
}

func WithPublishOptionsDeliveryMode(deliveryMode uint8) PublishOption {
	return func(pp *publishParams) {
		pp.deliveryMode = deliveryMode
	}
}

func WithPublishOptionsPriority(priority uint8) PublishOption {
	return func(pp *publishParams) {
		pp.priority = priority
	}
}

func WithPublishOptionsMsgExpiration(expiration string) PublishOption {
	return func(pp *publishParams) {
		pp.msgExpiration = expiration
	}
}

func WithPublishOptionsMsgId(msgId string) PublishOption {
	return func(pp *publishParams) {
		pp.msgId = msgId
	}
}

func WithPublishOptionsMsgType(msgType string) PublishOption {
	return func(pp *publishParams) {
		pp.msgType = msgType
	}
}

func WithPublishOptionsUserId(userId string) PublishOption {
	return func(pp *publishParams) {
		pp.userId = userId
	}
}

func WithPublishOptionsAppId(appId string) PublishOption {
	return func(pp *publishParams) {
		pp.appId = appId
	}
}

func WithPublishOptionsHeader(headers Table) PublishOption {
	return func(pp *publishParams) {
		pp.headers = headers
	}
}

func withReconnOptionsMQInit(client *RabbitmqClient) error {
	if client == nil {
		return nil
	}
	ch, err := client.handleMQChInit()
	if err != nil {
		return err
	}
	client.logger.Info("RabbitmqClient init channel successfully")
	client.chgDefaultCh(ch)
	return nil
}

func parseAMQPURL(host, port, key, secret, clientID string, tls bool) string {
	u, p := getAMQPAccess(key, secret, clientID)
	protocol := "amqp"
	if tls {
		protocol = "amqps"
	}
	return fmt.Sprintf("%s://%s:%s@%s:%s", protocol, u, p, host, port)
}

func getAMQPAccess(key, secret, clientID string) (username, password string) {
	timestamp := strconv.Itoa(int(time.Now().Unix()))
	sign, _ := authAMQPSign(clientID, key, timestamp, secret, "hmacsha1")
	username = fmt.Sprintf("%s&%s&%s", clientID, key, timestamp)
	password = sign
	return
}

func authAMQPSign(clientId, accessKey, timestamp, accessSecret, signMethod string) (string, error) {
	src := ""
	src = fmt.Sprintf("clientId%saccessKey%s", clientId, accessKey)
	if timestamp != "" {
		src = src + "timestamp" + timestamp
	}

	var h hash.Hash
	switch signMethod {
	case "hmacsha1":
		h = hmac.New(sha1.New, []byte(accessSecret))
	case "hmacmd5":
		h = hmac.New(md5.New, []byte(accessSecret))
	default:
		return "", errors.New("no access")
	}

	_, err := h.Write([]byte(src))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func NewFogAMQPClient(host, port, accessKey, accessSecret, clientID string, tls bool, opts ...ClientOption) (*RabbitmqClient, error) {
	uri := parseAMQPURL(host, port, accessKey, accessSecret, clientID, tls)
	return NewAMQPClient(uri, opts...)
}

func NewAMQPClient(endpoint string, opts ...ClientOption) (*RabbitmqClient, error) {
	l, _ := zap.NewProduction()
	cli := &RabbitmqClient{
		url:             endpoint,
		done:            make(chan bool, 1),
		reconnDone:      make(chan struct{}, 1),
		connTimeout:     time.Second * 32,
		initMQChTimeout: time.Second * 32,
		logger:          l.Sugar(),
	}

	for _, opt := range opts {
		opt(cli)
	}

	err := cli.handleReConnSync(withReconnOptionsMQInit)
	if err != nil {
		return nil, err
	}
	go cli.pingpong()
	return cli, nil
}

type RabbitmqClient struct {
	url        string
	conn       *amqp.Connection
	mqCh       *amqp.Channel
	logger     Logger
	reConnFlag uint32

	connTimeout     time.Duration
	initMQChTimeout time.Duration

	mu sync.Mutex

	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation

	done       chan bool
	reconnDone chan struct{}
}

func (rc *RabbitmqClient) Close() error {
	close(rc.done)
	if rc.mqCh != nil {
		rc.mqCh.Close()
	}
	if rc.conn != nil {
		rc.conn.Close()
	}
	return nil
}

func (rc *RabbitmqClient) ConsumeWithHandler(prefetchCnt int, queue string, handler AMQPMsgHandler, opts ...ConsumerOption) error {
	mqCh, err := rc.handleMQChInit()
	if err != nil {
		return err
	}
	ch, err := rc.consume(mqCh, prefetchCnt, queue, opts...)
	if err != nil {
		rc.logger.Debugf("RabbitmqClient consume error: %s", err)
		select {
		case <-rc.done:
			return ErrAmqpShutdown
		default:
			return rc.ConsumeWithHandler(prefetchCnt, queue, handler, opts...)
		}
	}

	rc.logger.Info("RabbitmqClient consuming...")
	for msg := range ch {
		handler(msg)
	}

	select {
	case <-rc.done:
		return ErrAmqpShutdown
	case <-rc.notifyConnClose:
		return rc.ConsumeWithHandler(prefetchCnt, queue, handler, opts...)
	case <-rc.notifyChanClose:
		return rc.ConsumeWithHandler(prefetchCnt, queue, handler, opts...)
	default:
		return nil
	}
}

func (rc *RabbitmqClient) DeclareExchange(exchangeName, kind string, opts ...ExchangeDeclareOption) error {
	ch, err := rc.makeMQCh()
	if err != nil {
		return err
	}
	defer ch.Close()

	params := exchangeParams{}

	for i := range opts {
		opts[i](&params)
	}

	return ch.ExchangeDeclare(exchangeName, kind, params.durable, params.autoDelete, params.internal, params.noWait, params.args)
}

func (rc *RabbitmqClient) DeleteExchange(exchangeName string, opts ...ExchangeDeleteOption) error {
	ch, err := rc.makeMQCh()
	if err != nil {
		return err
	}
	defer ch.Close()

	params := exchangeDeleteParams{}

	for i := range opts {
		opts[i](&params)
	}

	return ch.ExchangeDelete(exchangeName, params.ifUnused, params.noWait)
}

func (rc *RabbitmqClient) BindQueue(queueName, key, exchangeName string, opts ...QueueBindOption) error {
	ch, err := rc.makeMQCh()
	if err != nil {
		return err
	}
	defer ch.Close()

	params := queueBindParams{}

	for i := range opts {
		opts[i](&params)
	}

	return ch.QueueBind(queueName, key, exchangeName, params.noWait, params.args)
}

func (rc *RabbitmqClient) DeclareQueue(queueName string, opts ...QueueDeclareOption) error {
	ch, err := rc.makeMQCh()
	if err != nil {
		return err
	}
	defer ch.Close()

	params := queueParams{}

	for i := range opts {
		opts[i](&params)
	}

	_, err = ch.QueueDeclare(queueName, params.durable, params.autoDelete, params.exclusive, params.noWait, params.args)
	return err
}

func (rc *RabbitmqClient) DeleteQueue(queueName string, opts ...QueueDeleteOption) error {
	ch, err := rc.makeMQCh()
	if err != nil {
		return err
	}
	defer ch.Close()

	params := queueDeleteParams{}

	for i := range opts {
		opts[i](&params)
	}

	_, err = ch.QueueDelete(queueName, params.ifUnused, params.ifEmpty, params.noWait)
	return err
}

func (rc *RabbitmqClient) PublishUnsafeWithContext(ctx context.Context, exchangeName, key string, body []byte, opts ...PublishOption) error {
	params := publishParams{}

	for i := range opts {
		opts[i](&params)
	}

	err := rc.mqCh.PublishWithContext(ctx, exchangeName, key, params.mandatory, params.immediate, amqp.Publishing{
		Body:         body,
		Headers:      params.headers,
		ContentType:  params.contentType,
		DeliveryMode: params.deliveryMode,
		Priority:     params.priority,
		Expiration:   params.msgExpiration,
		MessageId:    params.msgId,
		Type:         params.msgType,
		UserId:       params.userId,
		AppId:        params.appId,
	})
	if err != nil {
		rc.handleReconnAsync()
	}
	return err
}

func (rc *RabbitmqClient) consume(mqCh *amqp.Channel, prefetchCnt int, queue string, opts ...ConsumerOption) (ch <-chan amqp.Delivery, err error) {
	err = mqCh.Qos(prefetchCnt, 0, false)
	if err != nil {
		rc.logger.Debugf("RabbitmqClient.Consume: mqCh.Qos: %s", err)
		return nil, err
	}

	defaultOpts := consumerParams{
		consumerTag: uuid.NewString(),
		args:        amqp.Table{},
	}

	for i := range opts {
		opts[i](&defaultOpts)
	}

	ch, err = mqCh.Consume(queue, defaultOpts.consumerTag, defaultOpts.autoAck, defaultOpts.exclusive, false, defaultOpts.noWait, defaultOpts.args)
	if err != nil {
		rc.logger.Debugf("RabbitmqClient.Consume: mqCh.Consume: %s", err)
	}
	return
}

func (rc *RabbitmqClient) pingpong() {
	tick := time.NewTicker(time.Second * 5)
	defer tick.Stop()
	for {
		select {
		case <-rc.done:
			return
		case <-rc.notifyChanClose:
			rc.logger.Debug("RabbitmqClient chan closed")
			err := rc.handleReConnSync(withReconnOptionsMQInit)
			if err != nil {
				return
			}
			continue
		case <-rc.notifyConnClose:
			rc.logger.Debug("RabbitmqClient conn closed")
			err := rc.handleReConnSync(withReconnOptionsMQInit)
			if err != nil {
				return
			}
		case <-tick.C:
		}
	}
}

// handleReconnAsync
func (rc *RabbitmqClient) handleReconnAsync() error {
	// 已开始重连
	if atomic.LoadUint32(&rc.reConnFlag) == 1 {
		rc.logger.Debug("handleReconnAsync reconnecting")
		return ErrAmqpReconn
	} else {
		go rc.handleReConnSync(withReconnOptionsMQInit)
		return ErrAmqpReconn
	}
}

// handleReConnSync will block until conn successfully or conn timeout
func (rc *RabbitmqClient) handleReConnSync(opts ...reconnOption) error {
	rc.mu.Lock()
	if !atomic.CompareAndSwapUint32(&rc.reConnFlag, 0, 1) {
		rc.logger.Info("RabbitmqClient waiting for reconnecting...")
		rc.mu.Unlock()
		select {
		case <-rc.reconnDone:
			return nil
		case <-rc.done:
			return ErrAmqpShutdown
		}
	}
	rc.reconnDone = make(chan struct{}, 1)
	rc.mu.Unlock()
	err := rc.reconnWithBlock()
	if err == nil {
		for i := range opts {
			err = opts[i](rc)
			if err != nil {
				return err
			}
		}
	}
	close(rc.reconnDone)
	atomic.StoreUint32(&rc.reConnFlag, 0)
	return err
}

func (rc *RabbitmqClient) reconnWithBlock() error {
	var err error
	delay := time.Millisecond * 500
	rc.logger.Info("RabbitmqClient attempt to connect")
loop:
	for {
		conn, err1 := rc.makeConn()
		if err1 != nil {
			if delay > rc.connTimeout {
				err = ErrAmqpConnTimeout
				close(rc.done)
				break
			}
			select {
			case <-rc.done:
				err = ErrAmqpShutdown
				break loop
			case <-time.After(delay):
				rc.logger.Infof("RabbitmqClient connect error, Retrying after %s...", delay)
				delay *= 2
			}
			continue
		}
		rc.chgConn(conn)

		select {
		case <-rc.done:
			err = ErrAmqpShutdown
			break loop
		case <-rc.notifyConnClose:
			rc.logger.Info("RabbitmqClient connection closed. Reconnecting...")
		default:
			err = nil
			break loop
		}
	}

	if err == nil {
		rc.logger.Info("RabbitmqClient connect successfully")
	} else {
		rc.logger.Infof("RabbitmqClient connect error: %s", err)
	}

	return err
}

func (rc *RabbitmqClient) handleMQChInit() (ch *amqp.Channel, err error) {
	delay := time.Second
	for {
		if !rc.validateConn() {
			err = rc.handleReConnSync()
			if err != nil {
				return nil, err
			}
		}
		ch, err := rc.makeMQCh()
		if err != nil {
			if delay > rc.initMQChTimeout {
				return nil, ErrAmqpChannelInitTimeout
			}
			rc.logger.Infof("RabbitmqClient init channel error: %s. Retrying after %s...", err, delay)

			select {
			case <-rc.done:
				return nil, ErrAmqpShutdown
			case <-time.After(delay):
				delay *= 2
			}
			continue
		}

		return ch, nil
	}
}

func (rc *RabbitmqClient) makeConn() (*amqp.Connection, error) {
	if rc.conn != nil && !rc.conn.IsClosed() {
		return rc.conn, nil
	}
	conn, err := amqp.Dial(rc.url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (rc *RabbitmqClient) chgConn(conn *amqp.Connection) {
	rc.conn = conn
	rc.notifyConnClose = make(chan *amqp.Error, 1)
	rc.conn.NotifyClose(rc.notifyConnClose)
}

func (client *RabbitmqClient) validateConn() bool {
	if client.conn != nil && !client.conn.IsClosed() {
		return true
	} else {
		return false
	}
}

func (rc *RabbitmqClient) makeMQCh() (*amqp.Channel, error) {
	if rc.conn == nil {
		return nil, ErrAmqpConnNil
	}
	ch, err := rc.conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.Confirm(false)
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (rc *RabbitmqClient) chgDefaultCh(channel *amqp.Channel) {
	rc.mqCh = channel
	rc.notifyChanClose = make(chan *amqp.Error, 1)
	rc.notifyConfirm = make(chan amqp.Confirmation, 1)
	rc.mqCh.NotifyClose(rc.notifyChanClose)
	rc.mqCh.NotifyPublish(rc.notifyConfirm)
}
