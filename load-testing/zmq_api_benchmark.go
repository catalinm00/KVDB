package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
)

// Estructuras de la API
type ApiRequest struct {
	Action string `json:"action,omitempty"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
}

type ApiResponse struct {
	Entry   EntryResponse `json:"entry"`
	Success bool          `json:"success,omitempty"`
}

type EntryResponse struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Tombstone bool   `json:"tombstone,omitempty"`
}

// Métricas
type RequestResult struct {
	Duration  time.Duration
	Success   bool
	TimedOut  bool
	Error     error
	Timestamp time.Time
}

type BenchmarkStats struct {
	TotalRequests      int64
	SuccessfulRequests int64
	TimeoutRequests    int64
	ErrorRequests      int64
	ResponseTimes      []time.Duration
	StartTime          time.Time
	EndTime            time.Time
	mu                 sync.Mutex
}

func (b *BenchmarkStats) AddResult(result RequestResult) {
	b.mu.Lock()
	defer b.mu.Unlock()

	atomic.AddInt64(&b.TotalRequests, 1)

	if result.TimedOut {
		atomic.AddInt64(&b.TimeoutRequests, 1)
	} else if result.Success {
		atomic.AddInt64(&b.SuccessfulRequests, 1)
	} else {
		atomic.AddInt64(&b.ErrorRequests, 1)
	}

	b.ResponseTimes = append(b.ResponseTimes, result.Duration)
}

func (b *BenchmarkStats) CalculatePercentiles() map[string]time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.ResponseTimes) == 0 {
		return make(map[string]time.Duration)
	}

	sort.Slice(b.ResponseTimes, func(i, j int) bool {
		return b.ResponseTimes[i] < b.ResponseTimes[j]
	})

	percentiles := map[string]time.Duration{
		"p50":  b.ResponseTimes[int(float64(len(b.ResponseTimes))*0.50)],
		"p90":  b.ResponseTimes[int(float64(len(b.ResponseTimes))*0.90)],
		"p95":  b.ResponseTimes[int(float64(len(b.ResponseTimes))*0.95)],
		"p99":  b.ResponseTimes[int(float64(len(b.ResponseTimes))*0.99)],
		"p999": b.ResponseTimes[int(float64(len(b.ResponseTimes))*0.999)],
	}

	return percentiles
}

func (b *BenchmarkStats) GetRPS() float64 {
	duration := b.EndTime.Sub(b.StartTime).Seconds()
	if duration == 0 {
		return 0
	}
	return float64(b.TotalRequests) / duration
}

func (b *BenchmarkStats) GetSuccessRate() float64 {
	if b.TotalRequests == 0 {
		return 0
	}
	return float64(b.SuccessfulRequests) / float64(b.TotalRequests) * 100
}

// Cliente ZMQ
type ZmqClient struct {
	socket  zmq4.Socket
	timeout time.Duration
}

func NewZmqClient(address string, timeout time.Duration) (*ZmqClient, error) {
	socket := zmq4.NewReq(context.Background())
	err := socket.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	return &ZmqClient{
		socket:  socket,
		timeout: timeout,
	}, nil
}

func (c *ZmqClient) SendRequest(req ApiRequest) (ApiResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return ApiResponse{}, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Enviar request
	err = c.socket.Send(zmq4.NewMsgFrom(payload))
	if err != nil {
		return ApiResponse{}, fmt.Errorf("failed to send request: %w", err)
	}

	// Recibir respuesta con timeout
	msgChan := make(chan zmq4.Msg, 1)
	errChan := make(chan error, 1)

	go func() {
		msg, err := c.socket.Recv()
		if err != nil {
			errChan <- err
			return
		}
		msgChan <- msg
	}()

	select {
	case msg := <-msgChan:
		var resp ApiResponse
		err = json.Unmarshal(msg.Bytes(), &resp)
		if err != nil {
			return ApiResponse{}, fmt.Errorf("failed to unmarshal response: %w", err)
		}
		return resp, nil
	case err := <-errChan:
		return ApiResponse{}, err
	case <-time.After(c.timeout):
		return ApiResponse{}, fmt.Errorf("request timeout")
	}
}

func (c *ZmqClient) Close() error {
	return c.socket.Close()
}

// Worker para realizar requests
func worker(id int, address string, timeout time.Duration, duration time.Duration,
	stats *BenchmarkStats, wg *sync.WaitGroup) {
	defer wg.Done()

	client, err := NewZmqClient(address, timeout)
	if err != nil {
		log.Printf("Worker %d failed to create client: %v", id, err)
		return
	}
	defer client.Close()

	endTime := time.Now().Add(duration)
	requests := []string{"SAVE", "GET", "DELETE"}

	for time.Now().Before(endTime) {
		// Generar request aleatorio
		action := requests[rand.Intn(len(requests))]
		key := fmt.Sprintf("key_%d_%d", id, rand.Intn(1000))
		value := fmt.Sprintf("value_%d_%d", id, rand.Intn(1000))

		req := ApiRequest{
			Action: action,
			Key:    key,
			Value:  value,
		}

		start := time.Now()
		resp, err := client.SendRequest(req)
		duration := time.Since(start)

		result := RequestResult{
			Duration:  duration,
			Success:   err == nil && resp.Success,
			TimedOut:  err != nil && err.Error() == "request timeout",
			Error:     err,
			Timestamp: time.Now(),
		}

		stats.AddResult(result)

		// Pequeña pausa para no saturar
		time.Sleep(1 * time.Millisecond)
	}

	log.Printf("Worker %d completed", id)
}

func printResults(stats *BenchmarkStats) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("BENCHMARK RESULTS")
	fmt.Println(strings.Repeat("=", 60))

	duration := stats.EndTime.Sub(stats.StartTime)

	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Total Requests: %d\n", stats.TotalRequests)
	fmt.Printf("Successful Requests: %d\n", stats.SuccessfulRequests)
	fmt.Printf("Failed Requests: %d\n", stats.ErrorRequests)
	fmt.Printf("Timeout Requests: %d\n", stats.TimeoutRequests)
	fmt.Printf("Success Rate: %.2f%%\n", stats.GetSuccessRate())
	fmt.Printf("RPS (Requests Per Second): %.2f\n", stats.GetRPS())

	fmt.Println("\nRESPONSE TIME PERCENTILES:")
	percentiles := stats.CalculatePercentiles()
	for _, p := range []string{"p50", "p90", "p95", "p99", "p999"} {
		if duration, exists := percentiles[p]; exists {
			fmt.Printf("%s: %v\n", p, duration)
		}
	}

	if len(stats.ResponseTimes) > 0 {
		// Calcular promedio y desviación estándar
		var sum time.Duration
		for _, rt := range stats.ResponseTimes {
			sum += rt
		}
		avg := time.Duration(int64(sum) / int64(len(stats.ResponseTimes)))

		var variance float64
		for _, rt := range stats.ResponseTimes {
			diff := float64(rt - avg)
			variance += diff * diff
		}
		variance /= float64(len(stats.ResponseTimes))
		stdDev := time.Duration(math.Sqrt(variance))

		fmt.Printf("\nSTATISTICS:\n")
		fmt.Printf("Average Response Time: %v\n", avg)
		fmt.Printf("Standard Deviation: %v\n", stdDev)
		fmt.Printf("Min Response Time: %v\n", stats.ResponseTimes[0])
		fmt.Printf("Max Response Time: %v\n", stats.ResponseTimes[len(stats.ResponseTimes)-1])
	}

	fmt.Println(strings.Repeat("=", 60))
}

func main() {
	var (
		address   = flag.String("address", "tcp://localhost:5555", "ZMQ server address")
		workers   = flag.Int("workers", 10, "Number of worker goroutines")
		duration  = flag.Duration("duration", 30*time.Second, "Test duration")
		timeout   = flag.Duration("timeout", 5*time.Second, "Request timeout")
		reportInt = flag.Duration("report", 5*time.Second, "Report interval during test")
	)
	flag.Parse()

	fmt.Printf("Starting benchmark with %d workers for %v\n", *workers, *duration)
	fmt.Printf("Server: %s\n", *address)
	fmt.Printf("Request timeout: %v\n", *timeout)
	fmt.Printf("Report interval: %v\n", *reportInt)
	fmt.Println("Press Ctrl+C to stop early...")

	stats := &BenchmarkStats{
		StartTime: time.Now(),
	}

	var wg sync.WaitGroup

	// Iniciar workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(i, *address, *timeout, *duration, stats, &wg)
	}

	// Reporte en tiempo real
	go func() {
		ticker := time.NewTicker(*reportInt)
		defer ticker.Stop()

		for range ticker.C {
			currentTime := time.Now()
			if currentTime.After(stats.StartTime.Add(*duration)) {
				return
			}

			elapsed := currentTime.Sub(stats.StartTime).Seconds()
			currentRPS := float64(stats.TotalRequests) / elapsed

			fmt.Printf("\n[%.0fs] Requests: %d | RPS: %.2f | Success: %.2f%% | Timeouts: %d\n",
				elapsed,
				stats.TotalRequests,
				currentRPS,
				stats.GetSuccessRate(),
				stats.TimeoutRequests)
		}
	}()

	// Esperar a que terminen todos los workers
	wg.Wait()
	stats.EndTime = time.Now()

	// Mostrar resultados finales
	printResults(stats)
}
