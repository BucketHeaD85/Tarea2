package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"os"
	"strconv"
	"sync"

	"../proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//Parametros para la gestion de conexiones
var direcciones = [3]string{"localhost:50052", "localhost:50053", "localhost:50054"}
var flags = [3]bool{true, true, true}
var nameNode = "localhost:50055"

//Variables de control
var disponible = true
var cola []int

//var titulo string

//Coordinador para la sincronizacion del Acceso en el archivo log
var mutexAcceso = &sync.Mutex{}
var condAcceso = sync.NewCond(mutexAcceso)

type server struct{}

func main() {
	listener, err := net.Listen("tcp", ":50055")
	if err != nil {
		log.Fatalf("failed to listen on port 50052: %v", err)
	}

	srv := grpc.NewServer()
	proto.RegisterServicioNameNodeServer(srv, &server{})
	reflection.Register(srv)
	fmt.Println("Escuchando")
	if e := srv.Serve(listener); e != nil {
		log.Fatalf("failed to Serve on port 50055: %v", e)
	}

}

func (server *server) SolicitarAcceso(ctx context.Context, request *proto.Solicitud) (*proto.Solicitud, error) {

	condAcceso.L.Lock()
	if disponible {
		fmt.Println("Sup")
		disponible = false
	} else {
		fmt.Println("Waiting")
		cola = append(cola, int(request.GetId()))
		//condAcceso.L.Lock()
		for (!disponible) && (cola[0] != int(request.GetId())) {
			condAcceso.Wait()
		}
		cola[0] = 0
		cola = cola[1:]
		//condAcceso.L.Unlock()
	}
	condAcceso.L.Unlock()
	return request, nil
}

func (server *server) Confirmar(ctx context.Context, request *proto.Propuesta) (*proto.Propuesta, error) {
	/*for i := 0; i < len(request);i++{
	}
	fmt.Println(len(request.GetAsignacion()))
	asignacion := make([]*proto.Asignacion, 1)
	asignacion[0] = new(proto.Asignacion)
	asignacion[0].PosDireccion = 0
	asignacion[0].NumChunk = 1
	*/
	time.Sleep(3 * time.Second)
	titulo := request.GetTitulo()

	f, err := os.OpenFile("./Log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	_, err = f.WriteString(titulo + " " + strconv.FormatInt(request.GetNchunks(), 10) + "\n")
	if err != nil {
		panic(err)
	}

	n := len(request.GetAsignacion())
	for i := 0; i < n; i++ {
		current := request.GetAsignacion()[i].PosDireccion
		//fmt.Println("Current proposal:" + direcciones[current])
		pos := verificarDataNode(current)
		request.GetAsignacion()[i].PosDireccion = pos
		f.WriteString("Chunk " + strconv.Itoa(i+1) + ": " + direcciones[pos] + "\n")
	}
	disponible = true
	fmt.Println("Done")
	condAcceso.Signal()
	return request, nil
}

func verificarDataNode(posDireccion int64) int64 {
	nodosDisponibles := 3
	factible := true
	_, err := grpc.Dial(direcciones[posDireccion], grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
	if err != nil { //el nodo propuesto no esta disponible
		for {
			flags[posDireccion] = false
			nodosDisponibles--
			/*
				if !(flags[0] && flags[1] && flags[2]){
					log.Fatalf("Ningun data node disponible. Operacion interrumpida")
					factible = false
					break
				}
			*/
			if nodosDisponibles == 1 { // solo queda un nodo posible
				posDireccion = (posDireccion + int64(1)) % 3 //Verifica si el primero de ambos nodos restantes esta disponible
				if !flags[(posDireccion)] {
					posDireccion = (posDireccion + int64(1)) % 3 //Si no lo esta, elige el ultimo nodo posible
					if !flags[(posDireccion)] {                  //El ultimo nodo tampoco esta disponible
						factible = false //Marca la eleccion como imposible
					}
				}
				break
			}
			moneda := rand.Float64() // se sortea un nuevo candidato

			if moneda <= 0.5 && flags[(posDireccion+int64(1))%3] {
				posDireccion = (posDireccion + int64(1)) % 3
			} else {
				posDireccion = (posDireccion + int64(2)) % 3
			}

			_, err = grpc.Dial(direcciones[posDireccion], grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
			if err == nil {
				break
			} // se ha encontrado un reemplazo
		}

		if factible {
			return posDireccion
		}
		return int64(-1) //no existe nodo factible para almacenar el chunk
	}
	return posDireccion //se aprueba el nodo propuesto originalmente

}
