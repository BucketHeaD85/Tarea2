package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	//"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"../proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct{}

//Variables para gestionar la comunicacion
var contadorChunks = int64(-1)
var totalChunks = int64(0)
var titulo string
var eleccion int
var chunkSize = 250000
var indice = 0

//mutex
var mutexChunks = &sync.Mutex{}
var condChunk = sync.NewCond(mutexChunks)
var clockMutex = &sync.Mutex{}
var criticalMutex = &sync.Mutex{}

//Direcciones ip relevantes
var direcciones = [3]string{"localhost:50052", "localhost:50053", "localhost:50054"}
var direccionesActivas []string

//namenode
var nameNode = "localhost:50055"
var self string
var idDataNode int64

//Reloj lógico
var clock int64
var criticalClock = int64(0)

//algoritmo a utilizar, 0=distribuido, 1=centralizado 
var algoritmo =0

type asignacion struct {
	posDireccion int64
	numChunk     int64
}

/*
message Chunk{
    bytes Contenido = 1;
}*/

type clientGRPC struct {
	conn      *grpc.ClientConn
	client    proto.ServicioSubidaClient
	chunkSize int
}

//Conexiones entre data nodes
var clienteA clientGRPC



func main() {
	//escoger algoritmo
	fmt.Println("Escoger algoritmo, 0 distribuido, 1 centralizado")
	_, err := fmt.Scanf("%d", &algoritmo)
	if err != nil {
		fmt.Println(err)
	}

	//escoger puerto
	fmt.Println("Puerto localhost:50052, localhost:50053, localhost:50054")
	_, er := fmt.Scanf("%d", &eleccion)
	if er != nil {
		fmt.Println(er)
	}

	clock=0

	var puerto string
	direccionesActivas=direcciones[0:2]
	if eleccion == 0 {
		puerto = ":50052"
		self="localhost:50052"
		idDataNode=int64(1)
	} else if eleccion == 1 {
		puerto = ":50053"
		self="localhost:50053"
		idDataNode=int64(2)
	} else /*if eleccion == 2*/{
			puerto = ":50054"
			self="localhost:50054"
			idDataNode=int64(3)
	}

	listener, err := net.Listen("tcp", puerto)
	if err != nil {
		log.Fatalf("failed to listen on port: %v", err)
	}

	srv := grpc.NewServer()
	proto.RegisterServicioSubidaServer(srv, &server{})
	reflection.Register(srv)
	fmt.Println("Escuchando")
	if e := srv.Serve(listener); e != nil {
		log.Fatalf("failed to Serve on port: %v", e)
	}
}

func gestionEnvios(cliente clientGRPC, nodo int, nombre string) {
	cliente = preparar(nodo)
	cliente.distribuirChunks(context.Background(), nombre)
	cliente.conn.Close()
}

func newClient(direccion string) (c clientGRPC, err error) {

	c.chunkSize = 250000

	c.conn, err = grpc.Dial(direccion, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to start grpc connection with address" + direccion)
		return
	}

	c.client = proto.NewServicioSubidaClient(c.conn)

	return c, err
}

func conectarDataNode(direccion int) clientGRPC {
	var tecla string
	fmt.Println("Direccion de nodo destino" + direcciones[direccion])
	c1, err := newClient(direcciones[direccion])
	for err != nil {
		//	disponibles[direccion] = false
		fmt.Println("Error al conectar al Data Node de direccion " + direcciones[direccion] + ". Presione 'r' para reintentar, o cualquier otra para omitir.")
		fmt.Scanf("%s", &tecla)
		if strings.ToLower(tecla) == "r" {
			c1, err = newClient(direcciones[direccion])
			/*
				if(err == nil){
					disponibles[direccion] = true
				}*/
		} else {
			break
		}

	}
	return c1
}

func preparar(nodo int) clientGRPC {
	cliente := conectarDataNode(nodo)
	return cliente
}

func (c *clientGRPC) distribuirChunks(ctx context.Context, f string) (err error) {
	var (
		writing = true
		buf     []byte
		n       int
		file    *os.File
		status  *proto.StatusSubida
	)

	file, err = os.Open(f)
	if err != nil {
		log.Fatalf("Error al abrir el archivo: %v", err)
		return
	}
	defer file.Close()

	stream, err := c.client.SubirArchivo(ctx)
	if err != nil {
		log.Fatalf("Failed to create upstream for file:%v", err)
		return
	}
	defer stream.CloseSend()

	req := &proto.Chunk{
		Data: &proto.Chunk_Nombre{
			Nombre: f,
		},
	}

	err = stream.Send(req)

	if err != nil {
		log.Fatalf("Cannot send file info")
	}

	//stats.StartedAt = time.Now()
	buf = make([]byte, c.chunkSize)
	for writing {
		n, err = file.Read(buf)
		if err != nil {
			if err == io.EOF {
				writing = false
				err = nil
				continue
			}

			log.Fatalf("Error while copying from file to buf:%v", err)
			return
		}

		req := &proto.Chunk{
			Data: &proto.Chunk_Contenido{
				Contenido: buf[:n],
			},
		}

		err = stream.Send(req)
		fmt.Println("Chunk enviado a data node")
	}

	//stats.FinishedAt = time.Now()

	status, err = stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("failed to receive upstream status response:%v", err)
		return
	}

	if status.Codigo != proto.CodigoStatusSubida_Exitoso {
		log.Fatalf("upload failed - msg:%v", err)
		return
	}

	return
}

func generarPropuesta(numChunks int64) {
	asignaciones := make([]*proto.Asignacion, numChunks)
	//fmt.Println("NumeroChunks:", numChunks)
	pos := int64(-1)
	for i := int64(0); i < numChunks; i++ {
		//dado := rand.Float64()
		asignaciones[i] = new(proto.Asignacion)
		asignaciones[i].PosDireccion = (pos + 1) % 3
		pos++
		asignaciones[i].NumChunk = i + int64(1)
		fmt.Println("Generated proposal:" + direcciones[asignaciones[i].PosDireccion])
	}

	conn, err := grpc.Dial(nameNode, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}
	client := proto.NewServicioNameNodeClient(conn)
	propuesta := &proto.Propuesta{
		Asignacion: asignaciones,
		Titulo:     titulo,
		Nchunks:    numChunks,
	}

	solicitud := proto.Request{
		Id: int64(eleccion),
	}
	//fmt.Println(len(propuesta.Asignacion))
	//fmt.Println("Largo de asignaciones:%v", len(asignaciones))
	//fmt.Println("Numero de chunks:", numChunks)
	var respuesta *proto.Propuesta
	if algoritmo !=0{
		client.SolicitarAcceso(context.Background(), &solicitud)
		respuesta,_= client.Confirmar(context.Background(), propuesta)
	}else{
		propuesta:= confirmarDistribucion(propuesta)
		//walala confirmation here <-----------
		criticalMutex.Lock()
		requestCritical()
		clockMutex.Lock()
		criticalClock=int64(0)
		clockMutex.Unlock()
		respuesta, _ = client.EnviarDistribucion(context.Background(), propuesta)
		criticalMutex.Unlock()
	}
	

	for i := int64(0); i < numChunks; i++ {
		fmt.Println("Titulo:" + titulo)
		nodo := int(respuesta.GetAsignacion()[i].PosDireccion)
		nombre := titulo + "_" + strconv.FormatInt(i, 10)
		fmt.Println("Nombre:" + nombre)
		if nodo == eleccion {
			//ioutil.WriteFile(nombre, recepcion.Contenido, os.ModeAppend)
			continue
		}
		go gestionEnvios(clienteA, nodo, nombre)
	}

	fmt.Println(respuesta.GetAsignacion())

}

func (server *server) AcusoEnvio(ctx context.Context, nChunks *proto.Setup) (*proto.Setup, error) {
	condChunk.L.Lock()
	contadorChunks = nChunks.GetNumChunks()
	titulo = nChunks.GetTitulo()
	totalChunks = contadorChunks
	//indice = 0
	return &proto.Setup{NumChunks: contadorChunks, Titulo: titulo}, nil
}

func (server *server) SubirArchivo(stream proto.ServicioSubida_SubirArchivoServer) (err error) {
	req, err := stream.Recv()
	if err != nil {
		log.Fatalf("Error al recibir informacion sobre el archivo: %v", err)
		return nil
	}
	nombre := req.GetNombre()
	for {
		recepcion, e := stream.Recv()
		if e != nil {
			if e == io.EOF {
				goto END
			}

			log.Fatalf("failed unexpectadely while reading chunks from stream")
			return
		}
		newFileName := nombre
		fmt.Println("Filename:" + newFileName)
		indice++
		//contadorChunks++
		_, err = os.Create(newFileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		ioutil.WriteFile(newFileName, recepcion.GetContenido(), os.ModeAppend)

	}

END:
	// once the transmission finished, send the
	// confirmation if nothign went wrong

	err = stream.SendAndClose(&proto.StatusSubida{
		Mensaje: "Archivo recibido",
		Codigo:  proto.CodigoStatusSubida_Exitoso,
	})
	contadorChunks--
	if contadorChunks == 0 {
		indice = 0
		//fmt.Println("Going to the other side")
		generarPropuesta(totalChunks)
		condChunk.L.Unlock()
	}
	return

}

func (server *server) DescargarArchivo(request *proto.Solicitud, stream proto.ServicioSubida_DescargarArchivoServer) error {
	archivo := request.GetNombreChunk()
	final := archivo[len(archivo)-1:]
	numero, _ := strconv.Atoi(final)
	//numero--
	fmt.Println(strconv.Itoa(numero))
	letras := strconv.Itoa(numero)
	archivoReal := archivo[:len(archivo)-2] + ".pdf_" + letras
	file, err := os.Open(archivoReal)
	if err != nil {
		log.Fatalf("Error al abrir el archivo: %v", err)
		return err
	}

	defer file.Close()
	buf := make([]byte, chunkSize)
	n, err := file.Read(buf)
	if err != nil {
		log.Fatalf("Error while copying from file to buf:%v", err)
		return err
	}

	req := &proto.Chunk{
		Data: &proto.Chunk_Contenido{
			Contenido: buf[:n],
		},
	}

	err = stream.Send(req)

	if err != nil {
		log.Fatalf("Failed to send chunk over stream:%v", err)
		return err
	}
	fmt.Println("Chunk enviado a cliente")

	return nil

}

func confirmarDistribucion(propuesta *proto.Propuesta)(distribucion *proto.Propuesta){
	var acuerdo = false
	var slice []string
	var dist=propuesta
	for !acuerdo{
		acuerdo=true
		direccionesActivas=checkDireccionesActivas(direcciones[0:2])
		for _,v := range direccionesActivas {
			if v==self{
				//check propuesta propia
				for _, value := range dist.Asignacion{
					if direcciones[value.PosDireccion]==self{
						continue
					}else{
						_,found:=findInSlice(slice,direcciones[value.PosDireccion])
						if !found{
							slice=append(slice,direcciones[value.PosDireccion])
						}
					}
				}
				for _, value:= range slice{
					acuerdo=acuerdo&&isAlive(value)
				}
				slice=slice[:0]
			}else{
				con, err := grpc.Dial(v, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
				if err !=nil{
					//log.Fatalf("Failed to connect: %s", err)
					continue
				}
				//llamar funcion gRPC
				client := proto.NewServicioSubidaClient(con)
				reply,err:=client.ConfirmarPropuesta(context.Background(), dist)
				if err!=nil{
					continue
				}
				fmt.Println(reply.GetAceptar())
				acuerdo=acuerdo && reply.GetAceptar()
				fmt.Println(acuerdo)
			}
		}
		if !acuerdo{
			//hacer nueva propuesta
			numChunks:=dist.Nchunks
			direccionesActivas=checkDireccionesActivas(direcciones[0:2])
			fmt.Println(len(direccionesActivas))
			asignaciones := make([]*proto.Asignacion, numChunks)
			//fmt.Println("NumeroChunks:", numChunks)
			pos := int64(-1)
			for i := int64(0); i < numChunks; i++ {
				//dado := rand.Float64()
				asignaciones[i] = new(proto.Asignacion)
				k,_ :=findInSlice( direcciones[0:2],direccionesActivas[(pos + 1) % int64(len(direccionesActivas))])
				asignaciones[i].PosDireccion = int64(k)
				pos++
				asignaciones[i].NumChunk = i + int64(1)
				fmt.Println("Generated proposal(2):" + direcciones[asignaciones[i].PosDireccion])
			}
			dist = &proto.Propuesta{
				Asignacion: asignaciones,
				Titulo:     titulo,
				Nchunks:    numChunks,
			}
			fmt.Println("finalized Generating proposal(2):")
			distribucion=dist
		}
		distribucion=dist
	}
	return distribucion
}
//acuerdo=acuerdo&&isAlive(v)

func isAlive(direccion string)(alive bool){
	con, err := grpc.Dial(direccion, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
	if err != nil {
		alive=false
		return alive
	}
	con.Close()
	alive = true
	return alive
}

func checkDireccionesActivas(verificar []string)(activas[]string){
	for _,v := range verificar {
		if v==self{
			activas=append(activas,v)
		}else{
			if isAlive(v){
				activas=append(activas,v)
			}
		}
	}
	return activas
}

func findInSlice(slice []string, val string) (int, bool) {
    for i, item := range slice {
        if item == val {
            return i, true
        }
    }
    return -1, false
}
func (server *server) ConfirmarPropuesta(ctx context.Context, propuesta *proto.Propuesta) (*proto.Confirmacion, error){
	direccionesActivas=checkDireccionesActivas(direcciones[0:2])
	var acuerdo =true
	var slice []string
	for _, value := range propuesta.GetAsignacion(){
		if direcciones[value.PosDireccion]==self{
			continue
		}else{
			_,found:=findInSlice(slice,direcciones[value.PosDireccion])
			if !found{
				slice=append(slice,direcciones[value.PosDireccion])
			}
		}
	}
	for _, value:= range slice{
		acuerdo=acuerdo&&isAlive(value)
	}
	fmt.Println(acuerdo)
	response:= proto.Confirmacion{Aceptar:acuerdo}
	return &response, nil
}

func requestCritical(){
	direccionesActivas = checkDireccionesActivas(direcciones[0:2])
	var wg sync.WaitGroup
	clockMutex.Lock()
	clock++
	timestamp:=clock
	criticalClock=clock
	clockMutex.Unlock()
	for _,v := range direccionesActivas{
		if v==self{
			continue
		}
		wg.Add(1)
		go sendRequest(v,timestamp, &wg)
	}
	wg.Wait()
}

func sendRequest(address string, timestamp int64, wg *sync.WaitGroup){
	con, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
	if err!=nil{
		log.Fatalf("Failed to connect: %s", err)
	}			
	//llamar funcion gRPC
	client := proto.NewServicioSubidaClient(con)
	request:= proto.Ramessage{
		Clock: timestamp,
		IdDataNode: idDataNode,
	}
	response,err:= client.PedirAccesoCritico(context.Background(), &request)
	clockMutex.Lock()
	if response.GetClock()>clock{
		clock=response.GetClock()
	}
	clockMutex.Unlock()
	wg.Done()
}

func (server *server) PedirAccesoCritico(ctx context.Context, request *proto.Ramessage) (*proto.Ramessage,error){

	clockMutex.Lock()
	if request.GetClock()>clock{
		clock=request.GetClock()
	}
	clock++
	timestamp:=clock
	criticalTimestamp:=criticalClock
	clockMutex.Unlock()
	var reply proto.Ramessage
	if criticalTimestamp==0 ||request.GetClock()<criticalTimestamp||(criticalTimestamp==request.GetClock()&&request.GetIdDataNode()<idDataNode){
		reply= proto.Ramessage{
			Clock: timestamp,
			IdDataNode: idDataNode,		
		}
	}else{
		criticalMutex.Lock()
		criticalMutex.Unlock()
		reply= proto.Ramessage{
			Clock: timestamp,
			IdDataNode: idDataNode,		
		}
	}
	return &reply,nil
}