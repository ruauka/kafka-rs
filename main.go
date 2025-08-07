package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "sf-topic"
	groupID       = "sf-group"
)

// createTopic создает топик
func createTopic() {
	// подключение к kafka
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		log.Fatalf("Ошибка подключения к Kafka: %v", err)
	}
	defer conn.Close()

	// конфиг топика
	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	// создаение топика
	err = conn.CreateTopics(topicConfig)
	if err != nil {
		log.Printf("Ошибка создания топика: %v", err)
	}

	log.Println("Topic создан...")
}

// producer отправляет 10 сообщений в топик
func producer(ctx context.Context) {
	log.Println("Producer запущен...")

	// Настраиваем Writer (Producer)
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Простой балансировщик
	}
	defer writer.Close()

	for i := range 1000 {
		select {
		case <-ctx.Done(): // Проверяем, не был ли отменен контекст
			log.Println("Producer: контекст отменен, остановка.")
			return
		default:
			msg := kafka.Message{
				Key:   []byte(strconv.Itoa(i)),
				Value: []byte(fmt.Sprintf("Hello, Kafka! Message #%d", i)),
			}

			// Отправляем сообщение
			err := writer.WriteMessages(ctx, msg)
			if err != nil {
				log.Printf("Producer: не удалось отправить сообщение: %v", err)
				// Небольшая пауза перед повторной попыткой
				time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("Producer: отправлено сообщение: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
			// time.Sleep(1 * time.Second) // Пауза между отправками
		}
	}
	log.Println("Producer: все сообщения отправлены.")
}

// consumer читает сообщения из топика
func consumer(ctx context.Context) {
	log.Println("Consumer запущен...")

	// Настраиваем Reader (Consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10 << 10, // 10KB (10e3)
		MaxBytes: 10 << 20, // 10MB (10e6)
	})

	defer reader.Close()

	for {
		select {
		case <-ctx.Done(): // Проверяем, не был ли отменен контекст
			log.Println("Consumer: контекст отменен, остановка.")
			return
		default:
			// Читаем сообщение
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				// Контекст отменен - это ожидаемая ошибка при завершении
				if err == context.Canceled {
					return
				}
				log.Printf("Consumer: ошибка чтения сообщения: %v", err)
				break
			}
			log.Printf("Consumer: получено сообщение: key=%s, value=%s (partition: %d, offset: %d)\n", string(msg.Key), string(msg.Value), msg.Partition, msg.Offset)
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func main() {
	// Создаем контекст, который будет отменен при получении сигнала ОС (Ctrl+C)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createTopic()

	// Используем WaitGroup для ожидания завершения горутин
	var wg sync.WaitGroup
	wg.Add(2)

	// Запускаем consumer в отдельной горутине
	go func() {
		defer wg.Done()
		consumer(ctx)
	}()

	// Запускаем producer в отдельной горутине
	go func() {
		defer wg.Done()
		producer(ctx)
	}()

	// Ожидаем сигнала для graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("Контекст завершен.")
	case <-sigterm:
		log.Println("Получен сигнал завершения, начинаю остановку...")
	}

	// Отменяем контекст, чтобы сигнализировать горутинам о завершении
	cancel()

	// Ожидаем, пока все горутины завершат свою работу
	wg.Wait()
	log.Println("Все горутины завершены. Выход.")
}


// 1 //
// Параметр MinBytes (минимальное количество байт) — это рекомендация для брокера Kafka, а не строгое правило. Он говорит брокеру: "Пожалуйста, не отвечай на мой запрос, пока у тебя не наберется хотя бы 10 КБ данных для меня".
// Это делается для оптимизации: чтобы consumer не делал множество сетевых запросов за крошечными порциями данных, а получал их более крупными "пачками", снижая нагрузку на сеть.
// Но что, если сообщения приходят редко и 10 КБ никогда не наберутся? Неужели consumer будет ждать вечно? Конечно, нет.
// Здесь в игру вступает второй важный параметр: MaxWait (максимальное время ожидания).
// Когда consumer делает запрос на получение данных (fetch request), брокер Kafka смотрит на MinBytes.
// Он начинает ждать, пока не накопится 10 КБ данных в топике.
// Одновременно с этим он запускает таймер, определенный параметром MaxWait (в библиотеке segmentio/kafka-go по умолчанию он равен 10 секундам).
// Происходит то, что случится раньше:
// Либо набирается 10 КБ данных, и брокер немедленно отправляет их consumer'у.
// Либо истекают 10 секунд ожидания. Брокер видит, что 10 КБ так и не набралось, и отправляет все, что у него есть на данный момент, даже если это всего одно маленькое сообщение.

// 2 //