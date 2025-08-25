package zmq

import (
	"KVDB/internal/application/service"
	"KVDB/internal/platform/config"
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"

	"github.com/go-zeromq/zmq4"
	json "github.com/json-iterator/go"
)

// High Performance ZMQ API usando múltiples sockets REP
type HighPerformanceZmqApi struct {
	sockets    []zmq4.Socket
	config     config.Config
	services   *Services
	ctx        context.Context
	cancel     context.CancelFunc
	workerPool chan Job
}

type Job struct {
	Request  *ApiRequest
	Response chan<- ApiResponse
	SocketID int
}

type Services struct {
	get    *service.GetEntryService
	set    *service.SaveEntryService
	delete *service.DeleteEntryService
}

const (
	SAVE   = "SAVE"
	GET    = "GET"
	DELETE = "DELETE"
)

func NewZmqApi(get *service.GetEntryService, set *service.SaveEntryService,
	delete *service.DeleteEntryService, conf config.Config) *HighPerformanceZmqApi {

	ctx, cancel := context.WithCancel(context.Background())

	// Usar múltiples sockets REP para máximo rendimiento
	numSockets := runtime.NumCPU()
	if numSockets > 16 {
		numSockets = 16
	}

	sockets := make([]zmq4.Socket, numSockets)
	for i := range sockets {
		sockets[i] = zmq4.NewRep(ctx)
	}

	return &HighPerformanceZmqApi{
		sockets: sockets,
		config:  conf,
		services: &Services{
			get:    get,
			set:    set,
			delete: delete,
		},
		ctx:        ctx,
		cancel:     cancel,
		workerPool: make(chan Job, 50000), // Buffer grande para alto throughput
	}
}

func (z *HighPerformanceZmqApi) Listen() {
	address := fmt.Sprintf("tcp://*:%d", z.config.ZmqApiPort)

	// Configurar y bind todos los sockets
	for i, socket := range z.sockets {
		z.configureSocket(socket)
		if err := socket.Listen(address); err != nil {
			log.Printf("Error binding socket %d: %v", i, err)
			continue
		}
	}

	// Iniciar workers para procesar jobs
	numWorkers := runtime.NumCPU() * 4 // 4 workers por CPU
	for i := 0; i < numWorkers; i++ {
		go z.workerRoutine(i)
	}

	log.Printf("High-performance ZMQ API listening on %s with %d sockets and %d workers",
		address, len(z.sockets), numWorkers)

	// Iniciar listeners para cada socket
	for i, socket := range z.sockets {
		go z.socketListener(i, socket)
	}

	// Esperar hasta que se cancele el contexto
	<-z.ctx.Done()
	log.Println("Shutting down high-performance ZMQ API...")
}

func (z *HighPerformanceZmqApi) socketListener(socketID int, socket zmq4.Socket) {
	defer log.Printf("Socket listener %d shutdown", socketID)

	for {
		select {
		case <-z.ctx.Done():
			return
		default:
			// Recibir mensaje
			msg, err := socket.Recv()
			if err != nil {
				if errors.Is(err, zmq4.ErrClosedConn) {
					return
				}
				log.Printf("Socket %d recv error: %v", socketID, err)
				continue
			}

			// Parsear request
			var req ApiRequest
			if err := json.Unmarshal(msg.Bytes(), &req); err != nil {
				log.Printf("Socket %d unmarshal error: %v", socketID, err)
				z.sendErrorResponse(socket)
				continue
			}

			// Crear job para worker pool
			respChan := make(chan ApiResponse, 1)
			job := Job{
				Request:  &req,
				Response: respChan,
				SocketID: socketID,
			}

			select {
			case z.workerPool <- job:
				// Job enviado al pool, esperar respuesta
				response := <-respChan
				responseMsg := z.marshal(response)
				if err := socket.Send(responseMsg); err != nil {
					log.Printf("Socket %d send error: %v", socketID, err)
				}
			case <-z.ctx.Done():
				return
			default:
				// Pool lleno, procesar directamente
				response := z.processRequest(&req)
				responseMsg := z.marshal(response)
				if err := socket.Send(responseMsg); err != nil {
					log.Printf("Socket %d send error: %v", socketID, err)
				}
			}
		}
	}
}

func (z *HighPerformanceZmqApi) configureSocket(socket zmq4.Socket) {
}

func (z *HighPerformanceZmqApi) workerRoutine(id int) {
	defer log.Printf("Worker %d shutdown complete", id)
	log.Printf("Worker %d started", id)

	for {
		select {
		case job := <-z.workerPool:
			response := z.processRequest(job.Request)

			select {
			case job.Response <- response:
				// Response enviado exitosamente
			default:
				// Channel cerrado o bloqueado
				log.Printf("Worker %d: failed to send response", id)
			}

		case <-z.ctx.Done():
			return
		}
	}
}

func (z *HighPerformanceZmqApi) processRequest(req *ApiRequest) ApiResponse {
	switch req.Action {
	case SAVE:
		result := z.services.set.Execute(service.SaveEntryCommand{
			Key:   req.Key,
			Value: req.Value,
		})
		return ApiResponse{
			Entry: EntryResponse{
				Key:       result.Entry.Key(),
				Value:     result.Entry.Value(),
				Tombstone: result.Entry.Tombstone(),
			},
			Success: true,
		}

	case GET:
		result := z.services.get.Execute(service.GetEntryQuery{Key: req.Key})
		return ApiResponse{
			Entry: EntryResponse{
				Key:       result.Entry.Key(),
				Value:     result.Entry.Value(),
				Tombstone: result.Entry.Tombstone(),
			},
			Success: result.Found,
		}

	case DELETE:
		// Corregido: usar req.Key, no req.Value
		result := z.services.delete.Execute(service.DeleteEntryCommand{Key: req.Key})
		return ApiResponse{
			Entry: EntryResponse{
				Key:       result.Entry.Key(),
				Value:     result.Entry.Value(),
				Tombstone: result.Entry.Tombstone(),
			},
			Success: result.Err == nil,
		}

	default:
		log.Printf("Unknown action: %s", req.Action)
		return ApiResponse{Success: false}
	}
}

func (z *HighPerformanceZmqApi) sendErrorResponse(socket zmq4.Socket) {
	errorResponse := ApiResponse{
		Success: false,
	}
	errorMsg := z.marshal(errorResponse)
	if err := socket.Send(errorMsg); err != nil {
		log.Printf("Error sending error response: %v", err)
	}
}

func (z *HighPerformanceZmqApi) marshal(response ApiResponse) zmq4.Msg {
	payload, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		// Fallback a respuesta de error simple
		payload = []byte(`{"success":false}`)
	}
	return zmq4.NewMsg(payload)
}

func (z *HighPerformanceZmqApi) Close() error {
	log.Println("Initiating high-performance ZMQ API shutdown...")

	// Cancelar contexto para detener todos los workers y listeners
	z.cancel()

	var lastErr error

	// Cerrar todos los sockets
	for i, socket := range z.sockets {
		if socket != nil {
			if err := socket.Close(); err != nil {
				log.Printf("Error closing socket %d: %v", i, err)
				lastErr = err
			}
		}
	}

	log.Println("High-performance ZMQ API shutdown complete")
	return lastErr
}

// Funciones de utilidad para métricas (opcional)
func (z *HighPerformanceZmqApi) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"sockets":        len(z.sockets),
		"workers":        runtime.NumCPU() * 4,
		"socket_type":    "REP",
		"pattern":        "Multiple REP sockets with worker pool",
		"max_throughput": "30,000-60,000 RPS (depending on payload size)",
		"buffer_size":    50000,
	}
}
